//! Regression checks for the independently maintained discovery lifecycle.
#![forbid(unsafe_code)]
use mcp_protocol::{
    ClientLifecycleMode, ClientServiceExt, ServerHandler, ServiceExt,
    model::{ClientConfig, ProtocolVersion},
    service::ClientInitializeError,
};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

struct Handler;
impl ServerHandler for Handler {}

#[tokio::test]
async fn default_client_and_custom_server_use_discovery() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let (client_io, server_io) = tokio::io::duplex(65536);
        let server = tokio::spawn(async move {
            Handler
                .serve(server_io)
                .await
                .unwrap()
                .waiting()
                .await
                .unwrap();
        });
        let client = ClientConfig::default().serve(client_io).await.unwrap();
        assert_eq!(
            client.peer_info().unwrap().protocol_version,
            ProtocolVersion::V_2026_07_28
        );
        assert!(client.list_tools(None).await.unwrap().tools.is_empty());
        client.cancel().await.unwrap();
        server.await.unwrap();
    })
    .await
    .expect("protocol lifecycle did not complete within five seconds");
}

#[tokio::test]
async fn discovery_errors_never_trigger_initialize() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
    let (client_io, server_io) = tokio::io::duplex(65536);
    let server = tokio::spawn(async move {
        let (reader, mut writer) = tokio::io::split(server_io);
        let mut lines = BufReader::new(reader).lines();
        let request: serde_json::Value =
            serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        assert_eq!(request["method"], "server/discover");
        let response = json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32601,"message":"unsupported"}});
        writer
            .write_all(format!("{response}\n").as_bytes())
            .await
            .unwrap();
        assert!(lines.next_line().await.unwrap().is_none());
    });
    assert!(ClientConfig::default().serve(client_io).await.is_err());
    server.await.unwrap();
    }).await.expect("protocol lifecycle did not complete within five seconds");
}

#[tokio::test]
async fn older_preferences_are_rejected_without_a_wire_request() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let (client_io, server_io) = tokio::io::duplex(65536);
        let error = ClientConfig::default()
            .serve_with_lifecycle(
                client_io,
                ClientLifecycleMode::Discover {
                    preferred_versions: vec![ProtocolVersion::V_2025_11_25],
                },
            )
            .await
            .err()
            .unwrap();
        assert!(matches!(
            error,
            ClientInitializeError::NoPreferredProtocolVersion
        ));
        assert!(
            BufReader::new(server_io)
                .lines()
                .next_line()
                .await
                .unwrap()
                .is_none()
        );
    })
    .await
    .expect("protocol lifecycle did not complete within five seconds");
}
