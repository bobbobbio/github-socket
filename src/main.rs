use serde::Serialize;

#[allow(dead_code)]
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

struct Client {
    client: reqwest::Client,
    token: String,
    base_url: String,
    #[allow(dead_code)]
    backend_ids: BackendIds,
}

impl Client {
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

    async fn request<T: Serialize>(&self, service: &str, method: &str, body: &T) {
        let resp = self.client
            .post(format!("{base_url}/twirp/{service}/{method}", base_url=&self.base_url))
            .header(
                "Content-Type", "application/json",
            )
            .header("User-Agent", "@actions/artifact-2.1.11")
            .header("Authorization", &format!("Bearer {token}", token=&self.token))
            .json(body)
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

#[tokio::main]
async fn main() {
    let client = Client::new();
    let req = CreateArtifactRequest {
        workflow_run_backend_id: client.backend_ids.workflow_run_backend_id.clone(),
        workflow_job_run_backend_id: client.backend_ids.workflow_job_run_backend_id.clone(),
        name: "foo".into(),
        version: 4
    };
    client.request("github.actions.results.api.v1.ArtifactService", "CreateArtifact", &req).await;
}
