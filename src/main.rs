#[tokio::main]
async fn main() {

    for (key, value) in std::env::vars() {
        println!("{key}: {value}");
    }

    let client = reqwest::Client::new();

    let github_repository = std::env::var("GITHUB_REPOSITORY").unwrap();
    let mut parts = github_repository.split('/');
    let owner = parts.next().unwrap();
    let repo = parts.next().unwrap();
    let run_id = std::env::var("GITHUB_RUN_ID").unwrap();
    let name = "foo";

    let token = "unknown";
    let base_url = "https://api.github.com";
    let resp = client
        .get(format!(
            "{base_url}/repos/{owner}/{repo}/actions/runs/{run_id}/artifacts?{name}"
        ))
        .header(
            "Accept",
            "application/vnd.github.v3+json",
        )
        .header("Authorization", &format!("token {token}"))
        .send()
        .await
        .unwrap();
    println!("{resp:?}");
}
