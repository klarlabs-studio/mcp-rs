//! Configured gRPC admission, discovery and message limits on real HTTP/2 streams.
#![cfg(all(feature = "grpc", feature = "client"))]
#![allow(clippy::unwrap_used)]
use mcp_rs::{
    Client, Server,
    transport::grpc::{GrpcConfigError, GrpcServerConfig, GrpcService, proto, tonic},
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;
use std::{num::NonZeroUsize, time::Duration};
use tokio::{sync::mpsc, time::timeout};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_util::sync::CancellationToken;

fn count(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}
struct Host {
    adapter: GrpcService<Server>,
    channel: tonic::transport::Channel,
    task: tokio::task::JoinHandle<()>,
}
impl Host {
    async fn start(server: Server, config: GrpcServerConfig) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let stop = CancellationToken::new();
        let adapter = GrpcService::with_config(server, stop.clone(), config).unwrap();
        let service = adapter.clone().service();
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(listener),
                    stop.cancelled_owned(),
                )
                .await
                .unwrap();
        });
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect_timeout(Duration::from_secs(3))
            .connect()
            .await
            .unwrap();
        Self {
            adapter,
            channel,
            task,
        }
    }
    async fn stream(
        &self,
    ) -> (
        mpsc::Sender<proto::Message>,
        tonic::Streaming<proto::Message>,
    ) {
        // Permit release may still be finishing after the previous stream's EOF.
        timeout(Duration::from_secs(3), async {
            loop {
                let (sender, receiver) = mpsc::channel(1);
                match proto::mcp_client::McpClient::new(self.channel.clone())
                    .connect(ReceiverStream::new(receiver))
                    .await
                {
                    Ok(response) => return (sender, response.into_inner()),
                    Err(error) => assert_eq!(error.code(), tonic::Code::ResourceExhausted),
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap()
    }
    async fn close(self) {
        timeout(Duration::from_secs(3), self.adapter.shutdown())
            .await
            .unwrap();
        timeout(Duration::from_secs(3), self.task)
            .await
            .unwrap()
            .unwrap();
        self.adapter.shutdown().await;
    }
}
fn discovery() -> proto::Message {
    proto::Message {
        r#type: proto::MessageType::Request as i32,
        request_id: "discover".into(),
        method: "server/discover".into(),
        params: serde_json::to_vec(&json!({"_meta": {
            "io.modelcontextprotocol/protocolVersion":"2026-07-28",
            "io.modelcontextprotocol/clientInfo":{"name":"limits","version":"1"},
            "io.modelcontextprotocol/clientCapabilities":{}
        }}))
        .unwrap(),
        ..Default::default()
    }
}
async fn discover(
    sender: &mpsc::Sender<proto::Message>,
    replies: &mut tonic::Streaming<proto::Message>,
) {
    sender.send(discovery()).await.unwrap();
    let reply = timeout(Duration::from_secs(3), replies.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(reply.request_id, "discover");
    assert!(reply.error.is_none());
}
async fn list_tools(
    sender: &mpsc::Sender<proto::Message>,
    replies: &mut tonic::Streaming<proto::Message>,
) {
    let mut request = discovery();
    request.method = "tools/list".into();
    request.request_id = "tools".into();
    sender.send(request).await.unwrap();
    let reply = timeout(Duration::from_secs(3), replies.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(reply.request_id, "tools");
    assert!(reply.error.is_none());
}
#[test]
fn invalid_limits_return_typed_errors_without_runtime_panics() {
    let mut config = GrpcServerConfig::default();
    assert_eq!(config.max_streams.get(), 1024);
    assert_eq!(config.queue_capacity.get(), 32);
    assert_eq!(config.max_message_bytes.get(), 4 << 20);
    assert_eq!(config.handshake_timeout, Duration::from_secs(10));
    config.max_streams = count(tokio::sync::Semaphore::MAX_PERMITS + 1);
    assert_eq!(
        GrpcService::with_config(Server::new("test", "1"), CancellationToken::new(), config)
            .unwrap_err(),
        GrpcConfigError::StreamLimit
    );
    let mut config = GrpcServerConfig::default();
    config.queue_capacity = count(tokio::sync::Semaphore::MAX_PERMITS + 1);
    assert_eq!(
        GrpcService::with_config(Server::new("test", "1"), CancellationToken::new(), config)
            .unwrap_err(),
        GrpcConfigError::QueueCapacity
    );
    let mut config = GrpcServerConfig::default();
    config.handshake_timeout = Duration::ZERO;
    assert_eq!(
        GrpcService::with_config(Server::new("test", "1"), CancellationToken::new(), config)
            .unwrap_err(),
        GrpcConfigError::HandshakeTimeout
    );
}
#[tokio::test]
async fn stream_limit_counts_silent_peers_and_releases_permits_on_disconnect() {
    let mut config = GrpcServerConfig::default();
    config.max_streams = count(1);
    config.queue_capacity = count(1);
    let host = Host::start(Server::new("limits", "1"), config).await;
    let (first, replies) = host.stream().await;
    let (_second, receiver) = mpsc::channel::<proto::Message>(1);
    let error = proto::mcp_client::McpClient::new(host.channel.clone())
        .connect(ReceiverStream::new(receiver))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    drop((first, replies));
    let (sender, mut replies) = host.stream().await;
    discover(&sender, &mut replies).await;
    list_tools(&sender, &mut replies).await;
    // Shutdown also stops a stream after successful discovery.
    timeout(Duration::from_secs(3), host.adapter.clone().shutdown())
        .await
        .unwrap();
    assert!(
        timeout(Duration::from_secs(3), replies.message())
            .await
            .unwrap()
            .unwrap()
            .is_none()
    );
    let (_sender, receiver) = mpsc::channel::<proto::Message>(1);
    let result = proto::mcp_client::McpClient::new(host.channel.clone())
        .connect(ReceiverStream::new(receiver))
        .await;
    assert!(result.is_err());
    drop((sender, replies));
    host.close().await;
}
#[tokio::test]
async fn configured_handshake_timeout_reclaims_a_silent_stream() {
    let mut config = GrpcServerConfig::default();
    config.max_streams = count(1);
    config.handshake_timeout = Duration::from_millis(500);
    let host = Host::start(Server::new("deadline", "1"), config).await;
    let (sender, mut replies) = host.stream().await;
    assert!(
        timeout(Duration::from_secs(3), replies.message())
            .await
            .unwrap()
            .unwrap()
            .is_none()
    );
    drop(sender);
    let (sender, mut replies) = host.stream().await;
    discover(&sender, &mut replies).await;
    list_tools(&sender, &mut replies).await;
    // The handshake deadline is not an established-session lifetime.
    tokio::time::sleep(Duration::from_millis(600)).await;
    list_tools(&sender, &mut replies).await;
    drop((sender, replies));
    host.close().await;
}
#[derive(Deserialize, JsonSchema)]
struct Input {
    text: String,
}
#[tokio::test]
async fn configured_message_limit_rejects_large_requests_and_responses() {
    let mut server = Server::new("messages", "1");
    server
        .tool("echo")
        .handler(|_, input: Input| async move { Ok(input.text) })
        .unwrap();
    server
        .tool("large")
        .handler(|_, _: Input| async { Ok("x".repeat(8192)) })
        .unwrap();
    let mut config = GrpcServerConfig::default();
    config.max_message_bytes = count(2048);
    config.queue_capacity = count(1);
    let host = Host::start(server, config).await;
    for (tool, text) in [("echo", "x".repeat(8192)), ("large", "small".into())] {
        let client = Client::grpc(
            proto::mcp_client::McpClient::new(host.channel.clone()),
            Default::default(),
        )
        .await
        .unwrap();
        assert!(
            timeout(
                Duration::from_secs(3),
                client.call::<_, String>(tool, &json!({"text":text}))
            )
            .await
            .unwrap()
            .is_err()
        );
        client.close().await.unwrap();
    }
    let client = Client::grpc(
        proto::mcp_client::McpClient::new(host.channel.clone()),
        Default::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        client
            .call::<_, String>("echo", &json!({"text":"healthy"}))
            .await
            .unwrap(),
        "healthy"
    );
    client.close().await.unwrap();
    host.close().await;
}
