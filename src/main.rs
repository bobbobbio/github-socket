use anyhow::{bail, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

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
    size: u32,
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

struct GhClient {
    client: TwirpClient,
}

impl GhClient {
    fn new() -> Self {
        Self {
            client: TwirpClient::new(),
        }
    }

    async fn upload(&self, name: &str, content: &str) {
        let req = CreateArtifactRequest {
            backend_ids: self.client.backend_ids.clone(),
            name: name.into(),
            version: 4,
        };
        let resp: serde_json::Value = self
            .client
            .request(
                "github.actions.results.api.v1.ArtifactService",
                "CreateArtifact",
                &req,
            )
            .await
            .unwrap();

        let upload_url =
            url::Url::parse(resp.get("signed_upload_url").unwrap().as_str().unwrap()).unwrap();
        let blob_client =
            azure_storage_blobs::prelude::BlobClient::from_sas_url(&upload_url).unwrap();
        blob_client
            .put_block_blob(content.to_owned())
            .content_type("text/plain")
            .await
            .unwrap();

        let req = FinalizeArtifactRequest {
            backend_ids: self.client.backend_ids.clone(),
            name: name.into(),
            size: content.len() as u32,
        };
        self.client
            .request::<_, serde_json::Value>(
                "github.actions.results.api.v1.ArtifactService",
                "FinalizeArtifact",
                &req,
            )
            .await
            .unwrap();
    }

    async fn list(&self) -> Vec<Artifact> {
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
            .await
            .unwrap();
        resp.artifacts
    }

    async fn download(&self, backend_ids: BackendIds, name: &str) -> String {
        let req = GetSignedArtifactUrlRequest {
            backend_ids,
            name: name.into(),
        };
        let resp = self
            .client
            .request::<_, serde_json::Value>(
                "github.actions.results.api.v1.ArtifactService",
                "GetSignedArtifactURL",
                &req,
            )
            .await
            .unwrap();
        let url = url::Url::parse(resp.get("signed_url").unwrap().as_str().unwrap()).unwrap();
        let blob_client = azure_storage_blobs::prelude::BlobClient::from_sas_url(&url).unwrap();
        let content = blob_client.get_content().await.unwrap();
        String::from_utf8_lossy(&content[..]).into()
    }
}

async fn wait_for_artifact(client: &GhClient, name: &str) -> BackendIds {
    loop {
        let artifacts = client.list().await;
        if let Some(artifact) = artifacts.iter().find(|a| a.name == name) {
            return artifact.backend_ids.clone();
        } else {
            println!("waiting for {name}");
        }
    }
}

#[tokio::main]
async fn main() {
    let client = GhClient::new();

    if std::env::var("ACTION").unwrap() == "1" {
        println!("sending ping");
        client.upload("foo1", "ping").await;
        println!("sent ping");

        let backend_ids = wait_for_artifact(&client, "foo2").await;

        let content = client.download(backend_ids, "foo2").await;
        println!("received message {content:?}");
    } else {
        let backend_ids = wait_for_artifact(&client, "foo1").await;

        let content = client.download(backend_ids, "foo1").await;
        println!("received message {content:?}");

        println!("sending pong");
        client.upload("foo2", "pong").await;
        println!("sent pong");
    }
}
