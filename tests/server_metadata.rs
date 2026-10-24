//! Metadata identity/instructions across real SDK lifecycles and live snapshots.
#![cfg(feature = "client")]
#![allow(clippy::unwrap_used)]
use mcp_rs::{
    Client, Server,
    model::*,
    protocol::{ServerHandler, ServiceExt},
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;
use std::time::Duration;
fn icon() -> Icon {
    Icon::new("https://example.test/icon.svg")
        .with_mime_type("image/svg+xml")
        .with_sizes(vec!["any".into()])
        .with_theme(IconTheme::Dark)
}
fn configured() -> Server {
    Server::new("metadata-fixture", "1")
        .with_title("Research 🦀")
        .with_description("Track papers, code, and videos")
        .with_website_url("https://example.test/research")
        .with_icons([icon()])
        .with_instructions("Use search before reading details.\nCite the original source.")
}
async fn connect<H: ServerHandler>(server: H) -> (Client, tokio::task::JoinHandle<()>) {
    let (client_io, server_io) = tokio::io::duplex(65536);
    let task = tokio::spawn(async move {
        let running = server.serve(server_io).await.unwrap();
        running.waiting().await.unwrap();
    });
    let client = Client::connect(client_io).await.unwrap();
    // Complete the modern server's first post-discovery request before teardown.
    client.peer().list_tools(None).await.unwrap();
    (client, task)
}
async fn close(client: Client, task: tokio::task::JoinHandle<()>) {
    client.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn modern_discovery_uses_sdk_identity_metadata_and_preserves_instructions() {
    let server = configured();
    let expected = ServerHandler::get_info(&server);
    let (client, task) = connect(server).await;
    let discovery = client
        .peer()
        .discover(RequestMetaObject::new())
        .await
        .unwrap();
    assert_eq!(
        discovery.server_info().as_ref(),
        Some(&expected.server_info)
    );
    assert_eq!(discovery.instructions, expected.instructions);
    let wire = serde_json::to_value(discovery).unwrap();
    assert!(wire.get("serverInfo").is_none());
    assert_eq!(
        wire["_meta"]["io.modelcontextprotocol/serverInfo"]["title"],
        "Research 🦀"
    );
    assert_eq!(wire["ttlMs"], 0);
    assert_eq!(wire["cacheScope"], "private");
    close(client, task).await;
}
#[tokio::test]
async fn defaults_and_convenience_clearing_do_not_emit_optional_metadata() {
    for (server, name) in [
        (Server::new("minimal", "1"), "minimal"),
        (
            configured()
                .with_title("")
                .with_description("")
                .with_website_url("")
                .with_icons([])
                .with_instructions(""),
            "metadata-fixture",
        ),
    ] {
        let (client, task) = connect(server).await;
        let info = client.peer().peer_info().unwrap();
        assert_eq!(
            serde_json::to_value(&info.server_info).unwrap(),
            json!({"name":name,"version":"1"})
        );
        assert!(info.instructions.is_none());
        close(client, task).await;
    }
}
#[derive(Deserialize, JsonSchema)]
struct Input {}
#[tokio::test]
async fn native_identity_replacement_preserves_methods_instructions_and_clone_isolation() {
    let mut original = configured();
    original
        .tool("hello")
        .handler(|_, _: Input| async { Ok("hello") })
        .unwrap();
    let replacement = Implementation::new("renamed", "2")
        .with_title("Replacement")
        .with_description("Native metadata")
        .with_icons(vec![icon()]);
    let changed = original.clone().with_server_info(replacement.clone());
    let (client, task) = connect(changed).await;
    assert_eq!(
        client.peer().peer_info().unwrap().server_info.as_ref(),
        Some(&replacement)
    );
    assert_eq!(
        client.peer().peer_info().unwrap().instructions.as_deref(),
        Some("Use search before reading details.\nCite the original source.")
    );
    assert_eq!(
        client.call_raw("hello", &json!({})).await.unwrap().content[0]
            .as_text()
            .unwrap()
            .text,
        "hello"
    );
    close(client, task).await;
    let (client, task) = connect(original).await;
    assert_eq!(
        client
            .peer()
            .peer_info()
            .unwrap()
            .server_info
            .as_ref()
            .unwrap()
            .name,
        "metadata-fixture"
    );
    close(client, task).await;
}
#[tokio::test]
async fn live_metadata_replacement_reaches_fresh_discovery_without_changing_capabilities() {
    let live = configured().into_live();
    let untouched = live.snapshot();
    let (client, task) = connect(live.clone()).await;
    let before = client
        .peer()
        .discover(RequestMetaObject::new())
        .await
        .unwrap();
    live.replace(
        live.snapshot()
            .with_title("Updated")
            .with_instructions("New instructions"),
    );
    let after = client
        .peer()
        .discover(RequestMetaObject::new())
        .await
        .unwrap();
    assert_eq!(
        after.server_info().unwrap().title.as_deref(),
        Some("Updated")
    );
    assert_eq!(after.instructions.as_deref(), Some("New instructions"));
    assert_eq!(before.capabilities, after.capabilities);
    assert_eq!(
        ServerHandler::get_info(&untouched)
            .server_info
            .title
            .as_deref(),
        Some("Research 🦀")
    );
    close(client, task).await;
}

#[tokio::test]
async fn build_information_round_trips_without_changing_native_sdk_identity() {
    let expected = mcp_rs::BuildInfo::new("abc123", "2026-10-17T10:00:00Z");
    let server = configured().with_build_info(expected.clone());
    let (client, task) = connect(server).await;
    let metadata = client.server_metadata().unwrap().unwrap();
    assert_eq!(metadata.build_info, Some(expected.clone()));
    assert_eq!(metadata.identity.name, "metadata-fixture");
    let discovery = client
        .peer()
        .discover(RequestMetaObject::new())
        .await
        .unwrap();
    assert_eq!(
        mcp_rs::ServerMetadata::from_discovery(&discovery)
            .unwrap()
            .unwrap(),
        metadata
    );
    assert_eq!(discovery.server_info().unwrap(), metadata.identity);
    assert_eq!(
        discovery.meta.as_ref().unwrap()["io.modelcontextprotocol/serverInfo"]["buildInfo"],
        serde_json::to_value(expected).unwrap()
    );
    close(client, task).await;
}
#[tokio::test]
async fn build_information_replacement_clearing_and_clones_are_isolated() {
    let original = configured().with_build_info(mcp_rs::BuildInfo::new("first", ""));
    for (server, expected) in [
        (
            original
                .clone()
                .with_server_info(Implementation::new("replacement", "2")),
            Some(mcp_rs::BuildInfo::new("first", "")),
        ),
        (
            original
                .clone()
                .with_build_info(mcp_rs::BuildInfo::new("second", "")),
            Some(mcp_rs::BuildInfo::new("second", "")),
        ),
        (original.clone().with_build_info(None), None),
        (original, Some(mcp_rs::BuildInfo::new("first", ""))),
    ] {
        let (client, task) = connect(server).await;
        assert_eq!(
            client.server_metadata().unwrap().unwrap().build_info,
            expected
        );
        close(client, task).await;
    }
}
#[tokio::test]
async fn live_build_information_updates_keep_handshake_snapshot_and_capabilities() {
    let live = configured()
        .with_build_info(mcp_rs::BuildInfo::new("first", ""))
        .into_live();
    let (client, task) = connect(live.clone()).await;
    let before = client
        .peer()
        .discover(RequestMetaObject::new())
        .await
        .unwrap();
    live.replace(
        live.snapshot()
            .with_build_info(mcp_rs::BuildInfo::new("second", "")),
    );
    let after = client
        .peer()
        .discover(RequestMetaObject::new())
        .await
        .unwrap();
    assert_eq!(
        mcp_rs::ServerMetadata::from_discovery(&after)
            .unwrap()
            .unwrap()
            .build_info
            .unwrap()
            .commit,
        "second"
    );
    assert_eq!(
        client
            .server_metadata()
            .unwrap()
            .unwrap()
            .build_info
            .unwrap()
            .commit,
        "first"
    );
    assert_eq!(before.capabilities, after.capabilities);
    assert_eq!(after.capabilities.tools.unwrap().list_changed, Some(true));
    live.replace(live.snapshot().with_build_info(None));
    let cleared = client
        .peer()
        .discover(RequestMetaObject::new())
        .await
        .unwrap();
    assert!(
        mcp_rs::ServerMetadata::from_discovery(&cleared)
            .unwrap()
            .unwrap()
            .build_info
            .is_none()
    );
    close(client, task).await;
}
