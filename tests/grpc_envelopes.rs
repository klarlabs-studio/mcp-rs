//! Invalid protobuf envelopes terminate only their stream before tool dispatch.
#![cfg(all(feature = "grpc", feature = "client"))]
#![allow(clippy::unwrap_used)]

use mcp_rs::{
    Client, Server,
    transport::grpc::{proto, serve_grpc, tonic},
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{sync::mpsc, time::timeout};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

#[derive(Deserialize, JsonSchema)]
struct Input {}
fn request(method: &str, mut params: Value) -> proto::Message {
    params["_meta"] = json!({
        "io.modelcontextprotocol/protocolVersion":"2026-07-28",
        "io.modelcontextprotocol/clientInfo":{"name":"envelope-test","version":"1"},
        "io.modelcontextprotocol/clientCapabilities":{}
    });
    proto::Message {
        r#type: proto::MessageType::Request as i32,
        request_id: method.into(),
        method: method.into(),
        params: serde_json::to_vec(&params).unwrap(),
        ..Default::default()
    }
}

#[tokio::test]
async fn malformed_peers_cannot_dispatch_or_interrupt_a_healthy_client() {
    let calls = Arc::new(AtomicUsize::new(0));
    let invoked = calls.clone();
    let mut server = Server::new("grpc-envelope", "1");
    server
        .tool("hello")
        .handler(move |_, _: Input| {
            invoked.fetch_add(1, Ordering::SeqCst);
            async { Ok("hello") }
        })
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let stop = CancellationToken::new();
    let shutdown = stop.clone();
    let task = tokio::spawn(async move {
        serve_grpc(server, listener, shutdown).await.unwrap();
    });
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect_timeout(Duration::from_secs(3))
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
    let valid = request("tools/call", json!({"name":"hello", "arguments":{}}));
    for invalid in [
        proto::Message {
            result: b"{}".to_vec(),
            ..valid.clone()
        },
        proto::Message {
            error: Some(proto::Error::default()),
            ..valid.clone()
        },
        proto::Message {
            method: String::new(),
            ..valid.clone()
        },
        proto::Message {
            r#type: proto::MessageType::Notification as i32,
            ..valid.clone()
        },
        proto::Message {
            r#type: i32::MAX,
            ..valid.clone()
        },
    ] {
        let (sender, receiver) = mpsc::channel(4);
        let mut raw = proto::mcp_client::McpClient::new(channel.clone());
        let mut replies = raw
            .connect(ReceiverStream::new(receiver))
            .await
            .unwrap()
            .into_inner();
        sender
            .send(request("server/discover", json!({})))
            .await
            .unwrap();
        let discovery = timeout(Duration::from_secs(3), replies.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(discovery.request_id, "server/discover");
        assert!(discovery.error.is_none());
        sender.send(invalid).await.unwrap();
        let closed = timeout(Duration::from_secs(3), replies.message())
            .await
            .unwrap();
        assert!(
            matches!(closed, Ok(None) | Err(_)),
            "malformed stream returned {closed:?}"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "malformed envelope dispatched a tool"
        );
    }
    assert_eq!(
        client.call::<_, String>("hello", &json!({})).await.unwrap(),
        "hello"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    client.close().await.unwrap();
    stop.cancel();
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
}
