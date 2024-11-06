use anyhow::{bail, Result};
use azure_storage_blobs::prelude::BlobClient;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BackendIds {
    workflow_run_backend_id: String,
    workflow_job_run_backend_id: String,
}

fn decode_backend_ids(token: &str) -> BackendIds {
    use base64::Engine as _;

    let mut token_parts = token.split(".").skip(1);
    let b64_part = token_parts.next().unwrap();
    let decoded = base64::engine::general_purpose::STANDARD_NO_PAD
        .decode(b64_part)
        .unwrap();
    let v = serde_json::from_slice::<serde_json::Value>(&decoded).unwrap();

    let scp = v.get("scp").unwrap().as_str().unwrap();

    let scope_parts = scp
        .split(" ")
        .map(|p| p.split(":").collect::<Vec<_>>())
        .find(|p| p[0] == "Actions.Results")
        .unwrap();

    BackendIds {
        workflow_run_backend_id: scope_parts[1].into(),
        workflow_job_run_backend_id: scope_parts[2].into(),
    }
}

struct TwirpClient {
    client: reqwest::Client,
    token: String,
    base_url: String,
    backend_ids: BackendIds,
}

impl TwirpClient {
    fn new() -> Self {
        let client = reqwest::Client::new();

        let token = std::env::var("ACTIONS_RUNTIME_TOKEN").unwrap();
        let backend_ids = decode_backend_ids(&token);

        let base_url = std::env::var("ACTIONS_RESULTS_URL").unwrap();

        Self {
            client,
            token,
            base_url,
            backend_ids,
        }
    }

    async fn request<BodyT: Serialize, RespT: DeserializeOwned>(
        &self,
        service: &str,
        method: &str,
        body: &BodyT,
    ) -> Result<RespT> {
        let req = self
            .client
            .post(format!(
                "{base_url}twirp/{service}/{method}",
                base_url = &self.base_url
            ))
            .header("Content-Type", "application/json")
            .header("User-Agent", "@actions/artifact-2.1.11")
            .header(
                "Authorization",
                &format!("Bearer {token}", token = &self.token),
            )
            .json(body);

        let resp = req.send().await?;
        if !resp.status().is_success() {
            bail!("{}", resp.text().await.unwrap());
        }

        Ok(resp.json().await?)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateArtifactRequest {
    #[serde(flatten)]
    backend_ids: BackendIds,
    name: String,
    version: u32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FinalizeArtifactRequest {
    #[serde(flatten)]
    backend_ids: BackendIds,
    name: String,
    size: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ListArtifactsRequest {
    #[serde(flatten)]
    backend_ids: BackendIds,
}

#[derive(Debug, Deserialize)]
struct Artifact {
    #[serde(flatten, with = "BackendIdsSnakeCase")]
    backend_ids: BackendIds,
    name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(remote = "BackendIds")]
struct BackendIdsSnakeCase {
    workflow_run_backend_id: String,
    workflow_job_run_backend_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListArtifactsResponse {
    artifacts: Vec<Artifact>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GetSignedArtifactUrlRequest {
    #[serde(flatten)]
    backend_ids: BackendIds,
    name: String,
}

#[derive(Debug, Deserialize)]
struct CreateArtifactResponse {
    signed_upload_url: String,
}

#[derive(Debug, Deserialize)]
struct GetSignedArtifactUrlResponse {
    signed_url: String,
}

struct GhClient {
    client: TwirpClient,
}

impl GhClient {
    fn new() -> Self {
        Self {
            client: TwirpClient::new(),
        }
    }

    async fn start_upload(&self, name: &str) -> Result<BlobClient> {
        let req = CreateArtifactRequest {
            backend_ids: self.client.backend_ids.clone(),
            name: name.into(),
            version: 4,
        };
        let resp: CreateArtifactResponse = self
            .client
            .request(
                "github.actions.results.api.v1.ArtifactService",
                "CreateArtifact",
                &req,
            )
            .await?;

        let upload_url = url::Url::parse(&resp.signed_upload_url)?;
        Ok(BlobClient::from_sas_url(&upload_url)?)
    }

    async fn finish_upload(&self, name: &str, content_length: usize) -> Result<()> {
        let req = FinalizeArtifactRequest {
            backend_ids: self.client.backend_ids.clone(),
            name: name.into(),
            size: content_length,
        };
        self.client
            .request::<_, serde_json::Value>(
                "github.actions.results.api.v1.ArtifactService",
                "FinalizeArtifact",
                &req,
            )
            .await?;
        Ok(())
    }

    async fn upload(&self, name: &str, content: &str) -> Result<()> {
        let blob_client = self.start_upload(name).await?;
        blob_client
            .put_block_blob(content.to_owned())
            .content_type("text/plain")
            .await?;
        self.finish_upload(name, content.len()).await?;
        Ok(())
    }

    async fn list(&self) -> Result<Vec<Artifact>> {
        let req = ListArtifactsRequest {
            backend_ids: self.client.backend_ids.clone(),
        };
        let resp: ListArtifactsResponse = self
            .client
            .request(
                "github.actions.results.api.v1.ArtifactService",
                "ListArtifacts",
                &req,
            )
            .await?;
        Ok(resp.artifacts)
    }

    async fn start_download(&self, backend_ids: BackendIds, name: &str) -> Result<BlobClient> {
        let req = GetSignedArtifactUrlRequest {
            backend_ids,
            name: name.into(),
        };
        let resp: GetSignedArtifactUrlResponse = self
            .client
            .request(
                "github.actions.results.api.v1.ArtifactService",
                "GetSignedArtifactURL",
                &req,
            )
            .await?;
        let url = url::Url::parse(&resp.signed_url)?;
        Ok(BlobClient::from_sas_url(&url)?)
    }

    #[allow(dead_code)]
    async fn download(&self, backend_ids: BackendIds, name: &str) -> Result<String> {
        let blob_client = self.start_download(backend_ids, name).await?;
        let content = blob_client.get_content().await?;
        Ok(String::from_utf8_lossy(&content[..]).into())
    }
}

struct GhReadSocket {
    client: Arc<GhClient>,
    remote_backend_ids: BackendIds,
    key: String,
    sequence_id: u64,
}

impl GhReadSocket {
    fn new(client: Arc<GhClient>, remote_backend_ids: BackendIds, key: String) -> Self {
        Self {
            client,
            remote_backend_ids: remote_backend_ids.clone(),
            key,
            sequence_id: 1,
        }
    }

    async fn read_msg(&mut self) -> Result<String> {
        let next = format!("{}-{}", &self.key, &self.sequence_id);
        self.sequence_id += 1;
        loop {
            if let Ok(content) = self
                .client
                .download(self.remote_backend_ids.clone(), &next)
                .await
            {
                break Ok(content);
            }
        }
    }
}

struct GhWriteSocket {
    client: Arc<GhClient>,
    key: String,
    sequence_id: u64,
}

impl GhWriteSocket {
    fn new(client: Arc<GhClient>, key: String) -> Self {
        Self {
            client,
            key,
            sequence_id: 1,
        }
    }

    async fn send_msg(&mut self, content: &str) -> Result<()> {
        let next = format!("{}-{}", &self.key, &self.sequence_id);
        self.client.upload(&next, content).await?;
        self.sequence_id += 1;
        Ok(())
    }
}

async fn wait_for_artifact(client: &GhClient, name: &str) -> Result<BackendIds> {
    loop {
        let artifacts = client.list().await?;
        if let Some(artifact) = artifacts.iter().find(|a| a.name == name) {
            return Ok(artifact.backend_ids.clone());
        } else {
            println!("waiting for {name}");
        }
    }
}

struct GhSocket {
    read: GhReadSocket,
    write: GhWriteSocket,
}

impl GhSocket {
    async fn connect(key: &str) -> Result<Self> {
        let client = Arc::new(GhClient::new());
        client.upload(&format!("{key}-connect"), " ").await?;
        let backend_ids = wait_for_artifact(&client, &format!("{key}-listen")).await?;
        Ok(Self {
            read: GhReadSocket::new(client.clone(), backend_ids, key.into()),
            write: GhWriteSocket::new(client, key.into()),
        })
    }

    async fn listen(key: &str) -> Result<Self> {
        let client = Arc::new(GhClient::new());
        client.upload(&format!("{key}-listen"), " ").await?;
        let backend_ids = wait_for_artifact(&client, &format!("{key}-connect")).await?;
        Ok(Self {
            read: GhReadSocket::new(client.clone(), backend_ids, key.into()),
            write: GhWriteSocket::new(client, key.into()),
        })
    }

    async fn read_msg(&mut self) -> Result<String> {
        self.read.read_msg().await
    }

    async fn send_msg(&mut self, content: &str) -> Result<()> {
        self.write.send_msg(content).await
    }
}

#[tokio::main]
async fn main() {
    if std::env::var("ACTION").unwrap() == "1" {
        let mut socket = GhSocket::listen("foo").await.unwrap();

        println!("sending ping");
        socket.send_msg("ping").await.unwrap();
        println!("sent ping");

        println!("waiting for response");
        let content = socket.read_msg().await.unwrap();
        println!("received message {content:?}");
    } else {
        let mut socket = GhSocket::connect("foo").await.unwrap();

        println!("waiting for message");
        let content = socket.read_msg().await.unwrap();
        println!("received message {content:?}");

        println!("sending pong");
        socket.send_msg("ping").await.unwrap();
        println!("sent pong");
    }
}
