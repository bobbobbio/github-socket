struct Client {
    client: reqwest::Client,
    owner: String,
    repo: String,
    run_id: String,
    token: String,
    base_url: String,
}

impl Client {
    fn new() -> Self {
        let client = reqwest::Client::new();

        let github_repository = std::env::var("GITHUB_REPOSITORY").unwrap();
        let mut parts = github_repository.split('/');
        let owner = parts.next().unwrap().into();
        let repo = parts.next().unwrap().into();
        let run_id = std::env::var("GITHUB_RUN_ID").unwrap();
        let token = std::env::var("GH_TOKEN").unwrap();

        use base64::Engine as _;
        let b64 = base64::engine::general_purpose::STANDARD.encode(&token);
        println!("token: {b64}");

        let base_url = "https://api.github.com".into();

        Self {
            client, owner, repo, run_id, token, base_url
        }
    }

    async fn get(&self, path: &str) {
        let resp = self.client
            .get(format!(
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
