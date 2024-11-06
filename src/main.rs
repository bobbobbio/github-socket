use anyhow::{bail, Result};
use azure_storage_blobs::{blob::operations::GetBlobResponse, prelude::BlobClient};
use bytes::Bytes;
use futures_core::Stream;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::future::Future;
use std::io;
use std::pin::{pin, Pin};
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt as _, ReadBuf};
use tokio_util::io::StreamReader;

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
    signed_upload_url: String
}

#[derive(Debug, Deserialize)]
struct GetSignedArtifactUrlResponse {
    signed_url: String
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
            .await
            .unwrap();
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

    async fn start_download_retry(&self, backend_ids: BackendIds, name: &str) -> Result<BlobClient> {
        loop {
            if let Ok(client) = self.start_download(backend_ids.clone(), name).await {
                return Ok(client)
            }
        }
    }

    async fn start_download(&self, backend_ids: BackendIds, name: &str) -> Result<BlobClient> {
        let req = GetSignedArtifactUrlRequest {
            backend_ids,
            name: name.into(),
        };
        let resp: GetSignedArtifactUrlResponse  = self
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

    async fn download(&self, backend_ids: BackendIds, name: &str) -> Result<String> {
        let blob_client = self.start_download(backend_ids, name).await?;
        let content = blob_client.get_content().await?;
        Ok(String::from_utf8_lossy(&content[..]).into())
    }
}

struct BlobBytesStream {
    response_body_stream: azure_core::Pageable<GetBlobResponse, azure_core::Error>,
    response_body: Option<azure_core::ResponseBody>,
}

impl BlobBytesStream {
    fn new(client: BlobClient) -> Self {
        Self {
            response_body_stream: client.get().into_stream(),
            response_body: None,
        }
    }
}

impl Stream for BlobBytesStream {
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(body) = &mut self.response_body {
            if let Some(value) = ready!(futures_util::Stream::poll_next(pin!(body), cx)) {
                return Poll::Ready(Some(
                    value.map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string())),
                ));
            } else {
                self.response_body = None;
            }
        }
        if let Some(resp) = ready!(futures_util::Stream::poll_next(
            pin!(&mut self.response_body_stream),
            cx
        )) {
            let resp = resp.map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            self.response_body = Some(resp.data);
            self.poll_next(cx)
        } else {
            Poll::Ready(None)
        }
    }
}

enum GhReadSocketState {
    Reading(StreamReader<BlobBytesStream, Bytes>),
    Getting(Pin<Box<dyn Future<Output = Result<BlobClient>> + Send + 'static>>),
}

struct GhReadSocket {
    client: Arc<GhClient>,
    remote_backend_ids: BackendIds,
    unique_id: String,
    sequence_id: u64,
    state: GhReadSocketState,
}

impl GhReadSocket {
    fn new(remote_backend_ids: BackendIds, unique_id: String) -> Self {
        let client = Arc::new(GhClient::new());
        Self {
            client: client.clone(),
            remote_backend_ids: remote_backend_ids.clone(),
            unique_id: unique_id.clone(),
            sequence_id: 2,
            state: GhReadSocketState::Getting(Box::pin(async move {
                Ok(client
                    .start_download_retry(remote_backend_ids, &format!("{unique_id}-1"))
                    .await?)
            })),
        }
    }
}

impl AsyncRead for GhReadSocket {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut self.state {
            GhReadSocketState::Reading(r) => {
                ready!(pin!(r).poll_read(cx, buf))?;
                if buf.filled().is_empty() {
                    let client = self.client.clone();
                    let remote_backend_ids = self.remote_backend_ids.clone();
                    let next = format!("{}-{}", self.unique_id, self.sequence_id);
                    self.state = GhReadSocketState::Getting(Box::pin(async move {
                        Ok(client.start_download_retry(remote_backend_ids, &next).await?)
                    }));
                    self.sequence_id += 1;
                    self.poll_read(cx, buf)
                } else {
                    Poll::Ready(Ok(()))
                }
            }
            GhReadSocketState::Getting(f) => {
                let blob_client = ready!(pin!(f).poll(cx))
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                self.state = GhReadSocketState::Reading(StreamReader::new(BlobBytesStream::new(
                    blob_client,
                )));
                self.poll_read(cx, buf)
            }
        }
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

#[tokio::main]
async fn main() {
    let client = GhClient::new();

    if std::env::var("ACTION").unwrap() == "1" {
        println!("sending ping");
        client.upload("foo-1", "ping").await.unwrap();
        println!("sent ping");

        let backend_ids = wait_for_artifact(&client, "foo-2").await.unwrap();

        let content = client.download(backend_ids, "foo-2").await.unwrap();
        println!("received message {content:?}");
    } else {
        let backend_ids = wait_for_artifact(&client, "foo-1").await.unwrap();

        //let content = client.download(backend_ids, "foo-1").await;

        let socket = GhReadSocket::new(backend_ids, "foo".into());
        let mut content = String::new();
        socket.take(4).read_to_string(&mut content).await.unwrap();

        println!("received message {content:?}");

        println!("sending pong");
        client.upload("foo-2", "pong").await.unwrap();
        println!("sent pong");
    }
}
