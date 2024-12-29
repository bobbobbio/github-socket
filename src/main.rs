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

    async fn upload(&self, name: &str, content: &[u8]) -> Result<()> {
        let blob_client = self.start_upload(name).await?;
        blob_client
            .put_block_blob(content.to_owned())
            .content_type("application/octet-stream")
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

    async fn download(&self, backend_ids: BackendIds, name: &str) -> Result<Vec<u8>> {
        let blob_client = self.start_download(backend_ids, name).await?;
        let content = blob_client.get_content().await?;
        Ok(content.into())
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

    async fn read_msg(&mut self) -> Result<Vec<u8>> {
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

    async fn send_msg(&mut self, content: &[u8]) -> Result<()> {
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
        client.upload(&format!("{key}-connect"), b" ").await?;
        let backend_ids = wait_for_artifact(&client, &format!("{key}-listen")).await?;
        Ok(Self {
            read: GhReadSocket::new(client.clone(), backend_ids, format!("{key}-down")),
            write: GhWriteSocket::new(client, format!("{key}-up")),
        })
    }

    async fn listen(key: &str) -> Result<Self> {
        let client = Arc::new(GhClient::new());
        client.upload(&format!("{key}-listen"), b" ").await?;
        let backend_ids = wait_for_artifact(&client, &format!("{key}-connect")).await?;
        Ok(Self {
            read: GhReadSocket::new(client.clone(), backend_ids, format!("{key}-up")),
            write: GhWriteSocket::new(client, format!("{key}-down")),
        })
    }

    async fn read_msg(&mut self) -> Result<Vec<u8>> {
        self.read.read_msg().await
    }

    async fn send_msg(&mut self, content: &[u8]) -> Result<()> {
        self.write.send_msg(content).await
    }
}

#[expect(dead_code)]
async fn job_one() {
    let mut socket = GhSocket::listen("foo").await.unwrap();

    println!("sending ping");
    socket.send_msg(&[1; 1024 * 1024]).await.unwrap();
    println!("sent ping");

    println!("waiting for response");
    let content = socket.read_msg().await.unwrap();
    assert!(content.iter().all(|b| *b == 2));
    println!("received response of length {}", content.len());
}

#[expect(dead_code)]
async fn job_two() {
    let mut socket = GhSocket::connect("foo").await.unwrap();

    println!("waiting for message");
    let content = socket.read_msg().await.unwrap();
    assert!(content.iter().all(|b| *b == 1));
    println!("received message of length {}", content.len());

    println!("sending pong");
    socket.send_msg(&[2; 1024 * 1024]).await.unwrap();
    println!("sent pong");
}

const CHUNK_A: [u8; 512] = [b'a'; 512];
const CHUNK_B: [u8; 512] = [b'b'; 512];
const CHUNK_C: [u8; 512] = [b'c'; 512];

async fn job_one_experiment() {
    let client = GhClient::new();
    let b_client = client.start_upload("foo").await.unwrap();
    b_client.put_page_blob(1024).await.unwrap();
    b_client
        .put_page((0, 511).try_into().unwrap(), &CHUNK_A[..])
        .await
        .unwrap();
    client.finish_upload("foo", 1024).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    b_client
        .put_page((512, 1023).try_into().unwrap(), &CHUNK_B[..])
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    b_client
        .put_page((0, 511).try_into().unwrap(), &CHUNK_C[..])
        .await
        .unwrap();
}

async fn job_two_experiment() {
    let client = GhClient::new();
    let backend_ids = wait_for_artifact(&client, "foo").await.unwrap();
    loop {
        let msg = client.download(backend_ids.clone(), "foo").await.unwrap();
        let s = String::from_utf8_lossy(&msg);
        println!("got {}", s);
    }
}

#[tokio::main]
async fn main() {
    if std::env::var("ACTION").unwrap() == "1" {
        job_one_experiment().await;
    } else {
        job_two_experiment().await;
    }
}
