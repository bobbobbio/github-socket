use anyhow::{anyhow, bail, Result};
use azure_storage_blobs::prelude::BlobClient;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashSet;

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

    #[expect(dead_code)]
    async fn download(&self, backend_ids: BackendIds, name: &str) -> Result<Vec<u8>> {
        let blob_client = self.start_download(backend_ids, name).await?;
        let content = blob_client.get_content().await?;
        Ok(content.into())
    }
}

struct GhReadSocket {
    blob: BlobClient,
    index: usize,
    etag: Option<azure_core::Etag>,
}

impl GhReadSocket {
    async fn new(client: &GhClient, backend_ids: BackendIds, key: &str) -> Result<Self> {
        let blob = client.start_download(backend_ids, key).await?;
        Ok(Self {
            blob,
            index: 0,
            etag: None,
        })
    }

    async fn maybe_read_msg(&mut self) -> Result<Option<Vec<u8>>> {
        use futures_util::StreamExt as _;

        let mut builder = self.blob.get().range(self.index..);

        if let Some(etag) = &self.etag {
            builder = builder.if_match(azure_core::request_options::IfMatchCondition::NotMatch(
                etag.to_string(),
            ));
        }

        let mut stream = builder.into_stream();
        let resp = stream
            .next()
            .await
            .ok_or_else(|| anyhow!("missing read response"))?;
        match resp {
            Ok(resp) => {
                self.etag = Some(resp.blob.properties.etag);

                let msg = resp.data.collect().await?;
                self.index += msg.len();
                Ok(Some(msg.to_vec()))
            }
            Err(err) => {
                use azure_core::{error::ErrorKind, StatusCode};

                match err.kind() {
                    ErrorKind::HttpResponse {
                        status: StatusCode::NotModified,
                        error_code: Some(error_code),
                    } if error_code == "ConditionNotMet" => {
                        return Ok(None);
                    }
                    ErrorKind::HttpResponse {
                        status: StatusCode::RequestedRangeNotSatisfiable,
                        error_code: Some(error_code),
                    } if error_code == "InvalidRange" => {
                        return Ok(None);
                    }
                    _ => {}
                }
                Err(err.into())
            }
        }
    }

    async fn read_msg(&mut self) -> Result<Vec<u8>> {
        loop {
            if let Some(res) = self.maybe_read_msg().await? {
                return Ok(res);
            }
        }
    }
}

struct GhWriteSocket {
    blob: BlobClient,
}

impl GhWriteSocket {
    async fn new(client: &GhClient, key: &str) -> Result<Self> {
        let blob = client.start_upload(key).await?;
        blob.put_append_blob().await?;
        client.finish_upload(key, 0).await?;
        Ok(Self { blob })
    }

    async fn write_msg(&mut self, data: &[u8]) -> Result<()> {
        self.blob.append_block(data.to_owned()).await?;
        Ok(())
    }
}

async fn wait_for_artifact(client: &GhClient, key: &str) -> Result<()> {
    while !client.list().await?.iter().any(|a| &a.name == key) {}
    Ok(())
}

struct GhSocket {
    read: GhReadSocket,
    write: GhWriteSocket,
}

impl GhSocket {
    async fn new(
        client: &GhClient,
        read_backend_ids: BackendIds,
        read_key: &str,
        write_key: &str,
    ) -> Result<Self> {
        Ok(Self {
            write: GhWriteSocket::new(client, write_key).await?,
            read: GhReadSocket::new(client, read_backend_ids, read_key).await?,
        })
    }

    async fn maybe_connect(client: &GhClient, id: &str) -> Result<Option<Self>> {
        let artifacts = client.list().await?;
        if let Some(listener) = artifacts
            .iter()
            .find(|a| &a.name == &format!("{id}-listen"))
        {
            let Artifact { name, backend_ids } = listener;
            let key = name.strip_suffix("-listen").unwrap();
            let self_id = "random_id";

            let write_key = format!("{self_id}-{key}-up");
            let write = GhWriteSocket::new(client, &write_key).await?;

            let read_key = format!("{self_id}-{key}-down");
            wait_for_artifact(client, &read_key).await?;
            let read = GhReadSocket::new(client, backend_ids.clone(), &read_key).await?;

            Ok(Some(Self { write, read }))
        } else {
            Ok(None)
        }
    }

    async fn connect(client: &GhClient, id: &str) -> Result<Self> {
        loop {
            if let Some(socket) = Self::maybe_connect(client, id).await? {
                return Ok(socket);
            }
        }
    }

    async fn read_msg(&mut self) -> Result<Vec<u8>> {
        self.read.read_msg().await
    }

    async fn write_msg(&mut self, data: &[u8]) -> Result<()> {
        self.write.write_msg(data).await
    }
}

struct GhListener {
    id: String,
    accepted: HashSet<String>,
}

impl GhListener {
    async fn new(client: &GhClient, id: &str) -> Result<Self> {
        let key = format!("{id}-listen");
        client.upload(&key, &[]).await?;
        Ok(Self {
            id: id.into(),
            accepted: HashSet::new(),
        })
    }

    async fn maybe_accept_one(&mut self, client: &GhClient) -> Result<Option<GhSocket>> {
        let artifacts = client.list().await?;
        if let Some(connected) = artifacts.iter().find(|a| {
            a.name.ends_with(&format!("{}-up", self.id)) && !self.accepted.contains(&a.name)
        }) {
            let Artifact { name, backend_ids } = connected;
            let key = name.strip_suffix("-up").unwrap();
            let socket = GhSocket::new(
                client,
                backend_ids.clone(),
                &format!("{key}-up"),
                &format!("{key}-down"),
            )
            .await?;
            self.accepted.insert(name.into());
            Ok(Some(socket))
        } else {
            Ok(None)
        }
    }

    async fn accept_one(&mut self, client: &GhClient) -> Result<GhSocket> {
        loop {
            if let Some(socket) = self.maybe_accept_one(client).await? {
                return Ok(socket);
            }
        }
    }
}

async fn job_one_experiment() {
    let client = GhClient::new();
    let mut listener = GhListener::new(&client, "foo").await.unwrap();

    let mut sock = listener.accept_one(&client).await.unwrap();

    for _ in 0..3 {
        println!("sending ping");
        sock.write_msg(&b"ping"[..]).await.unwrap();

        println!("waiting for response");
        let msg = sock.read_msg().await.unwrap();
        let msg_str = String::from_utf8_lossy(&msg);
        println!("got {msg_str:?}");

        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    }

    sock.write_msg(&b"done"[..]).await.unwrap();
}

async fn job_two_experiment() {
    let client = GhClient::new();
    let mut sock = GhSocket::connect(&client, "foo").await.unwrap();
    loop {
        let msg = sock.read_msg().await.unwrap();
        let msg_str = String::from_utf8_lossy(&msg);
        println!("got message = {msg_str:?}");

        if msg_str == "ping" {
            sock.write_msg(&b"pong"[..]).await.unwrap();
        } else if msg_str == "done" {
            break;
        }
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
