#[tokio::main]
async fn main() {
    let client = reqwest::Client::new();

    let github_repository = std::env::var("GITHUB_REPOSITORY").unwrap();
    let mut parts = github_repository.split('/');
    let owner = parts.next().unwrap();
    let repo = parts.next().unwrap();
    let run_id = std::env::var("GITHUB_RUN_ID").unwrap();
    let name = "foo";

    let token = std::env::var("ACTIONS_RUNTIME_TOKEN").unwrap();
    let base_url = "https://api.github.com";
    let req = client
        .get(format!(
            "{base_url}/repos/{owner}/{repo}/actions/runs/{run_id}/artifacts/{name}"
        ))
        .header(
            "Accept",
            "application/vnd.github.v3+json",
        )
        .header("User-Agent", "@actions/artifact-2.1.11")
        .header("Authorization", &format!("Bearer {token}"));
    println!("{req:?}");

    let resp = req
        .send()
        .await
        .unwrap();
    println!("{resp:?}");
    println!("{}", resp.text().await.unwrap());
}
