//! Network transport ownership, peer callbacks, and rejection policies.
#![cfg(all(
    feature = "websocket",
    feature = "grpc",
    feature = "client",
    feature = "elicitation"
))]
#![allow(clippy::unwrap_used, clippy::expect_used, deprecated)]
use futures_util::{SinkExt, StreamExt};
use mcp_rs::{
    Client, Server, Session, ToolError,
    model::*,
    protocol::{ClientHandler, ClientServiceExt, RoleClient, service::RequestContext},
    transport::{grpc::*, websocket::*},
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;
use tokio::{
    net::TcpListener,
    time::{Duration, timeout},
};
use tokio_util::sync::CancellationToken;
#[derive(Deserialize, JsonSchema)]
struct Input {}
fn application() -> Server {
    let mut server = Server::new("network", "1");
    server
        .tool("hello")
        .handler(|_, _: Input| async { Ok("hello") })
        .unwrap();
    server
        .tool("callback")
        .handler(|ctx, _: Input| async move {
            Session::from_context(&ctx)
                .progress(1.0, None, None)
                .await
                .map_err(|e| ToolError::new(e.to_string()))?;
            let answer = Session::from_context(&ctx)
                .elicit::<Answer>("Your name?")
                .await
                .map_err(|e| ToolError::new(e.to_string()))?;
            Ok(answer.map(|answer| answer.name).unwrap_or_default())
        })
        .unwrap();
    server
}
#[derive(Deserialize, JsonSchema)]
struct Answer {
    name: String,
}
impl mcp_rs::protocol::service::ElicitationSafe for Answer {}
struct Interactive;
impl ClientHandler for Interactive {
    fn get_info(&self) -> ClientConfig {
        ClientConfig::new(
            ClientCapabilities::builder()
                .enable_elicitation_with(
                    ElicitationCapability::new().with_form(FormElicitationCapability::default()),
                )
                .build(),
            Implementation::new("test", "1"),
        )
    }
    async fn create_elicitation(
        &self,
        _: ElicitRequestParams,
        _: RequestContext<RoleClient>,
    ) -> Result<ElicitResult, ErrorData> {
        Ok(ElicitResult::new(ElicitationAction::Accept).with_content(json!({"name":"Felix"})))
    }
}
async fn closed(task: tokio::task::JoinHandle<()>, stop: CancellationToken) {
    stop.cancel();
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn websocket_typed_roundtrip_and_active_session_shutdown() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let stop = CancellationToken::new();
    let token = stop.clone();
    let task = tokio::spawn(async move {
        serve_websocket(application(), listener, token, Default::default())
            .await
            .unwrap();
    });
    let client = Client::websocket(format!("ws://{addr}/")).await.unwrap();
    assert_eq!(
        client.call::<_, String>("hello", &json!({})).await.unwrap(),
        "hello"
    );
    closed(task, stop).await;
    timeout(Duration::from_secs(3), client.close())
        .await
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn websocket_bidirectional_requests_and_origin_policy() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let stop = CancellationToken::new();
    let token = stop.clone();
    let config = WebSocketServerConfig {
        allowed_origins: vec!["https://trusted.test".into()],
        ..Default::default()
    };
    let task = tokio::spawn(async move {
        serve_websocket(application(), listener, token, config)
            .await
            .unwrap();
    });
    let mut request = format!("ws://{addr}/").into_client_request().unwrap();
    request
        .headers_mut()
        .insert("origin", "https://evil.test".parse().unwrap());
    let error = tokio_tungstenite::connect_async(request).await.unwrap_err();
    assert!(
        matches!(error,tokio_tungstenite::tungstenite::Error::Http(response) if response.status()==403)
    );
    let mut duplicate = format!("ws://{addr}/").into_client_request().unwrap();
    duplicate
        .headers_mut()
        .append("origin", "https://trusted.test".parse().unwrap());
    duplicate
        .headers_mut()
        .append("origin", "https://evil.test".parse().unwrap());
    let error = tokio_tungstenite::connect_async(duplicate)
        .await
        .unwrap_err();
    assert!(
        matches!(error, tokio_tungstenite::tungstenite::Error::Http(response) if response.status() == 403)
    );
    let mut request = format!("ws://{addr}/").into_client_request().unwrap();
    request
        .headers_mut()
        .insert("origin", "https://trusted.test".parse().unwrap());
    let (socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    let client = Interactive
        .serve_with_lifecycle(
            WebSocketTransport::<RoleClient, _>::new(socket),
            mcp_rs::protocol::ClientLifecycleMode::Discover {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
            },
        )
        .await
        .unwrap();
    let result = timeout(
        Duration::from_secs(3),
        client.call_tool(CallToolRequestParams::new("callback").with_arguments(Default::default())),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result.content[0].as_text().unwrap().text, "Felix");
    client.cancel().await.unwrap();
    closed(task, stop).await;
}
#[tokio::test]
async fn websocket_ping_limits_and_stalled_handshake_shutdown() {
    use tokio_tungstenite::tungstenite::Message;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let stop = CancellationToken::new();
    let token = stop.clone();
    let config = WebSocketServerConfig {
        max_message_bytes: std::num::NonZeroUsize::new(1024).unwrap(),
        ..Default::default()
    };
    let task = tokio::spawn(async move {
        serve_websocket(application(), listener, token, config)
            .await
            .unwrap();
    });
    let _stalled = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
        .await
        .unwrap();
    socket.send(Message::Ping(vec![1, 2].into())).await.unwrap();
    assert!(matches!(
        timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        Message::Pong(_)
    ));
    socket
        .send(Message::Text("x".repeat(2048).into()))
        .await
        .unwrap();
    // Oversized frames must terminate this connection without affecting the listener.
    let response = timeout(Duration::from_secs(2), socket.next())
        .await
        .unwrap();
    assert!(
        response.is_none()
            || response.is_some_and(|r| r.is_err() || matches!(r, Ok(Message::Close(_))))
    );
    closed(task, stop).await;
}
#[tokio::test]
async fn grpc_typed_roundtrip_and_bidirectional_callbacks() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let stop = CancellationToken::new();
    let token = stop.clone();
    let task = tokio::spawn(async move {
        serve_grpc(application(), listener, token).await.unwrap();
    });
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect_timeout(Duration::from_secs(2))
        .connect()
        .await
        .unwrap();
    let client = Client::grpc(
        proto::mcp_client::McpClient::new(channel.clone()),
        Default::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        client.call::<_, String>("hello", &json!({})).await.unwrap(),
        "hello"
    );
    client.close().await.unwrap();
    let transport = mcp_rs::transport::grpc::connect(
        proto::mcp_client::McpClient::new(channel),
        Default::default(),
    );
    let client = Interactive
        .serve_with_lifecycle(
            transport,
            mcp_rs::protocol::ClientLifecycleMode::Discover {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
            },
        )
        .await
        .unwrap();
    let result = timeout(
        Duration::from_secs(3),
        client.call_tool(CallToolRequestParams::new("callback").with_arguments(Default::default())),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result.content[0].as_text().unwrap().text, "Felix");
    closed(task, stop).await;
    timeout(Duration::from_secs(3), client.cancel())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn grpc_cancellation_reaches_handler_and_idle_stream_shutdown_finishes() {
    use std::sync::Arc;
    use tokio::sync::Notify;
    struct Dropped(Arc<Notify>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.notify_one();
        }
    }
    let started = Arc::new(Notify::new());
    let dropped = Arc::new(Notify::new());
    let mut server = application();
    let observed = started.clone();
    let cancelled = dropped.clone();
    server
        .tool("wait")
        .handler(move |_, _: Input| {
            let started = observed.clone();
            let dropped = cancelled.clone();
            async move {
                let _guard = Dropped(dropped);
                started.notify_one();
                std::future::pending::<Result<String, ToolError>>().await
            }
        })
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let stop = CancellationToken::new();
    let token = stop.clone();
    let task = tokio::spawn(async move {
        serve_grpc(server, listener, token).await.unwrap();
    });
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let (_idle_sender, idle_receiver) = tokio::sync::mpsc::channel(1);
    let _idle = proto::mcp_client::McpClient::new(channel.clone())
        .connect(tokio_stream::wrappers::ReceiverStream::new(idle_receiver))
        .await
        .unwrap();
    let client = Client::grpc(
        proto::mcp_client::McpClient::new(channel),
        Default::default(),
    )
    .await
    .unwrap();
    let handle = client
        .peer()
        .send_cancellable_request(
            ClientRequest::CallToolRequest(Request::new(
                CallToolRequestParams::new("wait").with_arguments(Default::default()),
            )),
            Default::default(),
        )
        .await
        .unwrap();
    timeout(Duration::from_secs(2), started.notified())
        .await
        .unwrap();
    handle.cancel(None).await.unwrap();
    timeout(Duration::from_secs(2), dropped.notified())
        .await
        .unwrap();
    closed(task, stop).await;
    client.close().await.unwrap();
}
