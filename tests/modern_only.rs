//! Modern-only negotiation and retired-protocol rejection regressions.
#![cfg(feature = "client")]
#![allow(clippy::unwrap_used)]
use mcp_rs::{
    Client, Server,
    model::*,
    protocol::{ServerHandler, ServiceExt},
};
use serde_json::json;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

async fn rejects_initialize<H: ServerHandler>(server: H, version: ProtocolVersion) {
    let (client_io, server_io) = tokio::io::duplex(65536);
    let job = tokio::spawn(async move {
        assert!(server.serve(server_io).await.is_err());
    });
    let (reader, mut writer) = tokio::io::split(client_io);
    let request = json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":version,"clientInfo":{"name":"old","version":"1"},"capabilities":{}}});
    writer
        .write_all(format!("{request}\n").as_bytes())
        .await
        .unwrap();
    let line = BufReader::new(reader)
        .lines()
        .next_line()
        .await
        .unwrap()
        .unwrap();
    let response: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert!(response.get("error").is_some());
    tokio::time::timeout(Duration::from_secs(3), job)
        .await
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn static_and_live_servers_reject_initialize_and_advertise_only_modern_discovery() {
    for version in [
        ProtocolVersion::V_2025_06_18,
        ProtocolVersion::V_2025_11_25,
        ProtocolVersion::V_2026_07_28,
    ] {
        rejects_initialize(Server::new("modern", "1"), version.clone()).await;
        rejects_initialize(Server::new("live", "1").into_live(), version).await;
    }
    for live in [false, true] {
        let (client_io, server_io) = tokio::io::duplex(65536);
        let job = tokio::spawn(async move {
            let server = Server::new("modern", "1");
            if live {
                server
                    .into_live()
                    .serve(server_io)
                    .await
                    .unwrap()
                    .waiting()
                    .await
                    .unwrap();
            } else {
                server
                    .serve(server_io)
                    .await
                    .unwrap()
                    .waiting()
                    .await
                    .unwrap();
            }
        });
        let client = Client::connect(client_io).await.unwrap();
        let discovery = client
            .peer()
            .discover(RequestMetaObject::new())
            .await
            .unwrap();
        assert_eq!(
            discovery.supported_versions,
            vec![ProtocolVersion::V_2026_07_28]
        );
        client.peer().list_tools(None).await.unwrap();
        for method in [
            "tasks/list",
            "tasks/result",
            "resources/subscribe",
            "resources/unsubscribe",
            "ping",
        ] {
            assert!(
                client
                    .peer()
                    .send_request(ClientRequest::CustomRequest(CustomRequest::new(
                        method,
                        Some(json!({"taskId":"missing","uri":"data://a"}))
                    )))
                    .await
                    .is_err(),
                "accepted retired method {method}"
            );
        }
        let error = client
            .peer()
            .read_resource(ReadResourceRequestParams::new("missing://uri"))
            .await
            .err()
            .unwrap();
        assert!(
            matches!(error, mcp_rs::protocol::service::ServiceError::McpError(error) if error.code == ErrorCode::INVALID_PARAMS)
        );
        client.close().await.unwrap();
        job.await.unwrap();
    }
}
#[tokio::test]
async fn client_does_not_fall_back_to_initialize_when_discovery_is_missing() {
    let (client_io, server_io) = tokio::io::duplex(65536);
    let job = tokio::spawn(async move {
        let (reader, mut writer) = tokio::io::split(server_io);
        let mut lines = BufReader::new(reader).lines();
        let request: serde_json::Value =
            serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        assert_eq!(request["method"], "server/discover");
        let response = json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32601,"message":"No discovery"}});
        writer
            .write_all(format!("{response}\n").as_bytes())
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(2), lines.next_line())
                .await
                .unwrap()
                .unwrap()
                .is_none(),
            "client attempted another lifecycle"
        );
    });
    assert!(Client::connect(client_io).await.is_err());
    job.await.unwrap();
}
#[tokio::test]
async fn owned_protocol_handlers_also_reject_initialize() {
    struct NativeHandler;
    impl ServerHandler for NativeHandler {}
    rejects_initialize(NativeHandler, ProtocolVersion::V_2025_11_25).await;
    rejects_initialize(NativeHandler, ProtocolVersion::V_2026_07_28).await;
}
