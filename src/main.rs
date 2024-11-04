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
    owner: String,
    repo: String,
    run_id: String,
    token: String,
    base_url: String,
    #[allow(dead_code)]
    backend_ids: BackendIds,
}

impl Client {
    fn new() -> Self {
        let client = reqwest::Client::new();

        let github_repository = std::env::var("GITHUB_REPOSITORY").unwrap();
        let mut parts = github_repository.split('/');
        let owner = parts.next().unwrap().into();
        let repo = parts.next().unwrap().into();
        let run_id = std::env::var("GITHUB_RUN_ID").unwrap();
        let token = std::env::var("ACTIONS_RUNTIME_TOKEN").unwrap();
        let backend_ids = decode_backend_ids(&token);

        let base_url = std::env::var("ACTIONS_RESULTS_URL").unwrap();

        Self {
            client,
            owner,
            repo,
            run_id,
            token,
            base_url,
            backend_ids,
        }
    }

    async fn get(&self, path: &str) {
        let resp = self
            .client
            .get(format!(
                "{base_url}/repos/{owner}/{repo}/actions/runs/{run_id}/artifacts{path}",
                base_url = &self.base_url,
                owner = &self.owner,
                repo = &self.repo,
                run_id = &self.run_id,
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

    /*
    async fn post<T: Serialize>(&self, path: &str, body: &T) {
        let resp = self.client
            .post(format!(
                "{base_url}/repos/{owner}/{repo}/actions/runs/{run_id}/artifacts{path}",
                base_url=&self.base_url,
                owner=&self.owner,
                repo=&self.repo,
                run_id=&self.run_id,
            ))
            .header(
                "Accept",
                "application/vnd.github.v3+json",
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
    */
}

#[tokio::main]
async fn main() {
    let client = Client::new();
    client.get("").await;
}
