use anyhow::{bail, Result};
use serde::{de::DeserializeOwned, Serialize};

#[derive(Debug)]
struct BackendIds {
    workflow_run_backend_id: String,
    workflow_job_run_backend_id: String,
}

fn decode_backend_ids(token: &str) -> BackendIds {
    use base64::Engine as _;

    let mut token_parts = token.split(".").skip(1);
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(token_parts.next().unwrap())
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

        // let github_repository = std::env::var("GITHUB_REPOSITORY").unwrap();
        // let mut parts = github_repository.split('/');
        // let owner = parts.next().unwrap().into();
        // let repo = parts.next().unwrap().into();
        // let run_id = std::env::var("GITHUB_RUN_ID").unwrap();
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

struct PublicClient {
    client: reqwest::Client,
    owner: String,
    repo: String,
    token: String,
    base_url: String,
}

impl PublicClient {
    fn new() -> Self {
        let client = reqwest::Client::new();

        let github_repository = std::env::var("GITHUB_REPOSITORY").unwrap();
        let mut parts = github_repository.split('/');
        let owner = parts.next().unwrap().into();
        let repo = parts.next().unwrap().into();
        let token = std::env::var("GH_TOKEN").unwrap();

        let base_url = "https://api.github.com".into();

        Self {
            client,
            token,
            owner,
            repo,
            base_url,
        }
    }

    async fn list_workflow_runs(&self) {
        let resp = self
            .client
            .get(format!(
                "{base_url}/repos/{owner}/{repo}/actions/runs",
                base_url = &self.base_url,
                owner = &self.owner,
                repo = &self.repo,
            ))
            .header("Accept", "application/vnd.github.v3+json")
            .header("User-Agent", "@actions/artifact-2.1.11")
            .header(
                "Authorization",
                &format!("Bearer {token}", token = &self.token),
            )
            .send()
            .await
            .unwrap();
        println!("{resp:?}");
        println!("{}", resp.text().await.unwrap());
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateArtifactRequest {
    workflow_run_backend_id: String,
    workflow_job_run_backend_id: String,
    name: String,
    version: u32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FinalizeArtifactRequest {
    workflow_run_backend_id: String,
    workflow_job_run_backend_id: String,
    name: String,
    size: u32,
}

async fn upload() {
    let client = TwirpClient::new();
    let req = CreateArtifactRequest {
        workflow_run_backend_id: client.backend_ids.workflow_run_backend_id.clone(),
        workflow_job_run_backend_id: client.backend_ids.workflow_job_run_backend_id.clone(),
        name: "foo".into(),
        version: 4,
    };
    let resp: serde_json::Value = client
        .request(
            "github.actions.results.api.v1.ArtifactService",
            "CreateArtifact",
            &req,
        )
        .await
        .unwrap();

    let upload_url =
        url::Url::parse(resp.get("signed_upload_url").unwrap().as_str().unwrap()).unwrap();
    let blob_client = azure_storage_blobs::prelude::BlobClient::from_sas_url(&upload_url).unwrap();
    blob_client
        .put_block_blob("hello world")
        .content_type("text/plain")
        .await
        .unwrap();

    let req = FinalizeArtifactRequest {
        workflow_run_backend_id: client.backend_ids.workflow_run_backend_id.clone(),
        workflow_job_run_backend_id: client.backend_ids.workflow_job_run_backend_id.clone(),
        name: "foo".into(),
        size: 11,
    };
    let resp: serde_json::Value = client
        .request(
            "github.actions.results.api.v1.ArtifactService",
            "FinalizeArtifact",
            &req,
        )
        .await
        .unwrap();
    println!("{resp:#?}");
}

async fn download() {
    let client = PublicClient::new();
    client.list_workflow_runs().await;
}

#[tokio::main]
async fn main() {
    if std::env::var("GITHUB_WORKFLOW").unwrap() == "Test A" {
        upload().await;
    } else {
        download().await;
    }
}
