//! Incoming stdio limits apply to real protocol dispatch and child transports.
#![cfg(all(feature = "stdio", feature = "client"))]
#![allow(clippy::unwrap_used)]

#[cfg(all(unix, feature = "client-stdio"))]
use mcp_rs::protocol::transport::Transport;
use mcp_rs::{
    Client, Server,
    protocol::{RoleClient, RoleServer, ServiceExt, transport::async_rw::AsyncRwTransport},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[derive(Deserialize, Serialize, JsonSchema)]
struct Input {
    value: String,
}

fn server(calls: Arc<AtomicUsize>) -> Server {
    let mut server = Server::new("stdio-limits", "1");
    server
        .tool("echo")
        .handler(move |_, input: Input| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok(input.value) }
        })
        .unwrap();
    server
}

#[tokio::test]
async fn oversized_request_closes_before_handler_dispatch() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let calls = Arc::new(AtomicUsize::new(0));
        let server = server(calls.clone());
        let (client_io, server_io) = tokio::io::duplex(65536);
        let (reader, writer) = tokio::io::split(server_io);
        let transport =
            AsyncRwTransport::<RoleServer, _, _>::new_with_max_message_size(reader, writer, 2048);
        let task = tokio::spawn(async move {
            server
                .serve(transport)
                .await
                .unwrap()
                .waiting()
                .await
                .unwrap()
        });
        let client = Client::connect(client_io).await.unwrap();
        assert_eq!(
            client
                .call::<_, String>("echo", &Input { value: "ok".into() })
                .await
                .unwrap(),
            "ok"
        );
        assert!(
            client
                .call::<_, String>(
                    "echo",
                    &Input {
                        value: "x".repeat(8192)
                    }
                )
                .await
                .is_err()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        client.close().await.unwrap();
        task.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn oversized_response_ends_client_reception_after_successful_discovery() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let calls = Arc::new(AtomicUsize::new(0));
        let server = server(calls.clone());
        let (client_io, server_io) = tokio::io::duplex(65536);
        let task = tokio::spawn(async move {
            server
                .serve(server_io)
                .await
                .unwrap()
                .waiting()
                .await
                .unwrap()
        });
        let (reader, writer) = tokio::io::split(client_io);
        let transport =
            AsyncRwTransport::<RoleClient, _, _>::new_with_max_message_size(reader, writer, 2048);
        let client = Client::connect(transport).await.unwrap();
        assert_eq!(
            client
                .call::<_, String>("echo", &Input { value: "ok".into() })
                .await
                .unwrap(),
            "ok"
        );
        assert!(
            client
                .call::<_, String>(
                    "echo",
                    &Input {
                        value: "x".repeat(8192)
                    }
                )
                .await
                .is_err()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        client.close().await.unwrap();
        task.await.unwrap();
    })
    .await
    .unwrap();
}

#[cfg(all(unix, feature = "client-stdio"))]
#[tokio::test]
async fn child_stdout_limit_is_applied_at_exact_boundary() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let response = r#"{"jsonrpc":"2.0","id":1,"result":{"tools":[]}}"#;
        for limit in [response.len(), response.len()-1] {
            let mut command = tokio::process::Command::new("sh");
            // Fixed fixture data; no user content or shell interpolation.
            command.arg("-c").arg("printf '%s\n' '{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"tools\":[]}}'; read unused");
            let (mut transport, _) = mcp_rs::protocol::transport::TokioChildProcess::builder(command).max_message_size(limit).spawn().unwrap();
            assert_eq!(transport.receive().await.is_some(), limit == response.len());
            transport.close().await.unwrap();
        }
    }).await.unwrap();
}

#[cfg(all(unix, feature = "client-stdio"))]
#[tokio::test]
async fn typed_child_client_applies_limit_during_discovery() {
    let mut command = tokio::process::Command::new("sh");
    command
        .arg("-c")
        .arg("read request; printf 'oversized-without-newline'; read unused");
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        Client::stdio_with_max_message_size(command, 1),
    )
    .await
    .unwrap();
    assert!(result.is_err());
}
