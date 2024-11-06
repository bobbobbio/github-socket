use anyhow::{bail, Result};
use azure_storage_blobs::{
    blob::operations::{AppendBlock, GetBlobResponse},
    prelude::BlobClient,
};
use bytes::Bytes;
use futures_core::Stream;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::future::Future;
use std::io;
use std::pin::{pin, Pin};
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, ReadBuf};
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

    async fn start_download_retry(
        &self,
        backend_ids: BackendIds,
        name: &str,
    ) -> Result<BlobClient> {
        loop {
            if let Ok(client) = self.start_download(backend_ids.clone(), name).await {
                return Ok(client);
            }
        }
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

    #[allow(dead_code)]
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
                        Ok(client
                            .start_download_retry(remote_backend_ids, &next)
                            .await?)
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

struct PendingWrite {
    f: AppendBlock,
    size: usize,
}

enum GhWriteSocketState {
    Writing {
        client: BlobClient,
        pending: Option<PendingWrite>,
        bytes_written: usize,
    },
    Getting(Pin<Box<dyn Future<Output = Result<BlobClient>> + Send + 'static>>),
    Flushing(Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>),
}

struct GhWriteSocket {
    client: Arc<GhClient>,
    unique_id: String,
    sequence_id: u64,
    state: GhWriteSocketState,
}

impl GhWriteSocket {
    fn new(unique_id: String) -> Self {
        let client = Arc::new(GhClient::new());
        let next = format!("{unique_id}-1");
        Self {
            client: client.clone(),
            unique_id,
            sequence_id: 1,
            state: GhWriteSocketState::Getting(Box::pin(async move {
                let client = client.start_upload(&next).await?;
                client.put_append_blob().await?;
                Ok(client)
            })),
        }
    }
}

impl AsyncWrite for GhWriteSocket {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        src: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut self.state {
            GhWriteSocketState::Writing {
                client,
                pending,
                bytes_written,
            } => {
                if let Some(PendingWrite { f, size }) = pending {
                    println!("write socket: pending write poll");
                    ready!(pin!(f).poll(cx))
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                    let write_size = *size;
                    *bytes_written += write_size;
                    *pending = None;
                    Poll::Ready(Ok(write_size))
                } else {
                    println!("write socket: pending write start");
                    *pending = Some(PendingWrite {
                        f: client.append_block(src.to_owned()).into_future(),
                        size: src.len(),
                    });
                    self.poll_write(cx, src)
                }
            }
            GhWriteSocketState::Getting(f) => {
                println!("write socket: getting write poll");
                let client = ready!(pin!(f).poll(cx))
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                println!("write socket: write mode start");
                self.state = GhWriteSocketState::Writing {
                    client,
                    pending: None,
                    bytes_written: 0,
                };
                self.poll_write(cx, src)
            }
            GhWriteSocketState::Flushing(_) => panic!("pending flush"),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.state {
            GhWriteSocketState::Writing {
                client: _,
                pending,
                bytes_written,
            } => {
                println!("write socket: starting flush");
                assert!(pending.is_none(), "pending write when flushing");
                let size = *bytes_written;
                let client = self.client.clone();
                let next = format!("{}-{}", self.unique_id, self.sequence_id);
                self.state = GhWriteSocketState::Flushing(Box::pin(async move {
                    client.finish_upload(&next, size).await
                }));
                self.poll_flush(cx)
            }
            GhWriteSocketState::Flushing(f) => {
                println!("write socket: flush poll");
                ready!(pin!(f).poll(cx))
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                self.sequence_id += 1;
                let client = self.client.clone();
                let next = format!("{}-{}", self.unique_id, self.sequence_id);
                println!("write socket: getting start");
                self.state = GhWriteSocketState::Getting(Box::pin(async move {
                    let client = client.start_upload(&next).await?;
                    client.put_append_blob().await?;
                    Ok(client)
                }));
                Poll::Ready(Ok(()))
            }
            _ => panic!("pending write when flushing"),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        unimplemented!()
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
        client.upload("exchange_a", "a").await.unwrap();
        let backend_ids = wait_for_artifact(&client, "exchange_b").await.unwrap();

        let mut write_socket = GhWriteSocket::new("foo_b".into());
        let read_socket = GhReadSocket::new(backend_ids, "foo_a".into());

        println!("sending ping");
        write_socket.write_all(b"ping").await.unwrap();
        write_socket.flush().await.unwrap();
        println!("sent ping");

        println!("waiting for response");
        let mut content = String::new();
        read_socket
            .take(4)
            .read_to_string(&mut content)
            .await
            .unwrap();
        println!("received message {content:?}");
    } else {
        client.upload("exchange_b", "b").await.unwrap();
        let backend_ids = wait_for_artifact(&client, "exchange_a").await.unwrap();

        let mut write_socket = GhWriteSocket::new("foo_b".into());
        let read_socket = GhReadSocket::new(backend_ids, "foo_a".into());

        println!("waiting for message");
        let mut content = String::new();
        read_socket
            .take(4)
            .read_to_string(&mut content)
            .await
            .unwrap();
        println!("received message {content:?}");

        println!("sending pong");
        write_socket.write_all(b"ping").await.unwrap();
        write_socket.flush().await.unwrap();
        println!("sent pong");
    }
}
