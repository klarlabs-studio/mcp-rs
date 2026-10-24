//! Real loopback coverage for the static Server Card and AI Catalog routes.
#![cfg(feature = "http")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use axum::http::StatusCode;
use mcp_rs::{Server, model::Implementation, server_card::ServerCard};
use serde_json::{Value, json};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const SERVER_CARD_TYPE: &str = "application/mcp-server-card+json";
const CATALOG_TYPE: &str = "application/ai-catalog+json";

struct Fixture {
    client: reqwest::Client,
    base: String,
    shutdown: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

impl Fixture {
    async fn stop(self) {
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(3), self.task)
            .await
            .expect("server shutdown should be bounded")
            .unwrap();
    }
}

async fn fixture(card: ServerCard) -> Fixture {
    let shutdown = CancellationToken::new();
    let server = Server::new("acme/weather", "2.3.0");
    let mcp = mcp_rs::transport::http_router(server, shutdown.child_token());
    let cards = mcp_rs::transport::server_card::router(
        &card,
        "https://public.example/mcp/server-card",
        "urn:example:acme-weather",
    )
    .unwrap();
    let router = mcp.merge(cards);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let stop = shutdown.clone();
    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(stop.cancelled_owned())
            .await
            .unwrap();
    });
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    Fixture {
        client,
        base,
        shutdown,
        task,
    }
}

fn card(version: &str) -> ServerCard {
    let mut identity = Implementation::new("acme/weather", version);
    identity.title = Some("Weather Service".into());
    identity.description = Some("Forecasts worldwide".into());
    ServerCard::from_identity(&identity)
}

fn assert_static_headers(response: &reqwest::Response, media_type: &str) {
    assert_eq!(
        response.headers()[reqwest::header::CONTENT_TYPE],
        media_type
    );
    assert_eq!(
        response.headers()[reqwest::header::CACHE_CONTROL],
        "public, max-age=3600"
    );
    assert_eq!(
        response.headers()[reqwest::header::ACCESS_CONTROL_ALLOW_ORIGIN],
        "*"
    );
    assert_eq!(
        response.headers()[reqwest::header::ACCESS_CONTROL_ALLOW_METHODS],
        "GET, HEAD"
    );
    assert_eq!(
        response.headers()[reqwest::header::ACCESS_CONTROL_ALLOW_HEADERS],
        "Content-Type, If-None-Match"
    );
    assert_eq!(
        response.headers()[reqwest::header::ACCESS_CONTROL_EXPOSE_HEADERS],
        "ETag"
    );
    assert!(response.headers().contains_key(reqwest::header::ETAG));
}

#[tokio::test]
async fn card_get_head_and_etag_revalidation_are_consistent() {
    let server = fixture(card("2.3.0")).await;
    let url = format!("{}/mcp/server-card", server.base);

    let response = server.client.get(&url).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_static_headers(&response, SERVER_CARD_TYPE);
    let etag = response.headers()[reqwest::header::ETAG]
        .to_str()
        .unwrap()
        .to_owned();
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["name"], "acme/weather");
    assert_eq!(body["version"], "2.3.0");
    assert_eq!(body["title"], "Weather Service");
    assert_eq!(body["description"], "Forecasts worldwide");

    let head = server.client.head(&url).send().await.unwrap();
    assert_eq!(head.status(), StatusCode::OK);
    assert_static_headers(&head, SERVER_CARD_TYPE);
    assert_eq!(head.headers()[reqwest::header::ETAG], etag);
    assert!(head.bytes().await.unwrap().is_empty());

    for candidate in [
        etag.clone(),
        format!("W/{etag}"),
        format!("\"stale\", {etag}"),
        "*".to_owned(),
    ] {
        let response = server
            .client
            .get(&url)
            .header(reqwest::header::IF_NONE_MATCH, candidate)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_static_headers(&response, SERVER_CARD_TYPE);
        assert_eq!(response.headers()[reqwest::header::ETAG], etag);
        assert!(response.bytes().await.unwrap().is_empty());
    }
    let changed = server
        .client
        .get(&url)
        .header(reqwest::header::IF_NONE_MATCH, "\"different\"")
        .send()
        .await
        .unwrap();
    assert_eq!(changed.status(), StatusCode::OK);
    assert_eq!(changed.headers()[reqwest::header::ETAG], etag);

    let changed_snapshot = fixture(card("2.3.1")).await;
    let changed_response = changed_snapshot
        .client
        .get(format!("{}/mcp/server-card", changed_snapshot.base))
        .send()
        .await
        .unwrap();
    assert_ne!(changed_response.headers()[reqwest::header::ETAG], etag);
    changed_snapshot.stop().await;

    assert_eq!(
        server.client.post(url).send().await.unwrap().status(),
        StatusCode::METHOD_NOT_ALLOWED
    );
    server.stop().await;
}

#[tokio::test]
async fn catalog_aliases_are_static_and_modern_discovery_survives_merge() {
    let server = fixture(card("2.3.0")).await;
    let canonical = format!("{}/.well-known/ai-catalog.json", server.base);
    let alias = format!("{}/.well-known/mcp/catalog.json", server.base);
    let canonical_response = server
        .client
        .get(&canonical)
        .header(reqwest::header::HOST, "attacker.invalid")
        .header("x-forwarded-proto", "http")
        .send()
        .await
        .unwrap();
    assert_eq!(canonical_response.status(), StatusCode::OK);
    assert_static_headers(&canonical_response, CATALOG_TYPE);
    let canonical_etag = canonical_response.headers()[reqwest::header::ETAG]
        .to_str()
        .unwrap()
        .to_owned();
    let catalog: Value = canonical_response.json().await.unwrap();
    assert_eq!(catalog["specVersion"], "1.0");
    assert_eq!(
        catalog["entries"][0]["identifier"],
        "urn:example:acme-weather"
    );
    assert_eq!(catalog["entries"][0]["type"], SERVER_CARD_TYPE);
    assert_eq!(
        catalog["entries"][0]["url"],
        "https://public.example/mcp/server-card"
    );

    let alias_response = server.client.get(&alias).send().await.unwrap();
    assert_eq!(alias_response.status(), StatusCode::OK);
    assert_static_headers(&alias_response, CATALOG_TYPE);
    assert_eq!(
        alias_response.headers()[reqwest::header::ETAG],
        canonical_etag
    );
    assert_eq!(alias_response.json::<Value>().await.unwrap(), catalog);
    let head = server.client.head(&canonical).send().await.unwrap();
    assert_eq!(head.status(), StatusCode::OK);
    assert_eq!(head.headers()[reqwest::header::ETAG], canonical_etag);
    assert!(head.bytes().await.unwrap().is_empty());
    let conditional = server
        .client
        .get(&alias)
        .header(
            reqwest::header::IF_NONE_MATCH,
            format!("W/{canonical_etag}"),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(conditional.status(), StatusCode::NOT_MODIFIED);
    assert!(conditional.bytes().await.unwrap().is_empty());
    assert_eq!(
        server
            .client
            .post(&canonical)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::METHOD_NOT_ALLOWED
    );

    let response = server
        .client
        .post(format!("{}/mcp", server.base))
        .header("mcp-method", "server/discover")
        .header("mcp-protocol-version", "2026-07-28")
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .header(
            reqwest::header::ACCEPT,
            "application/json, text/event-stream",
        )
        .json(&json!({
            "jsonrpc":"2.0", "id":1, "method":"server/discover",
            "params":{"_meta":{
                "io.modelcontextprotocol/protocolVersion":"2026-07-28",
                "io.modelcontextprotocol/clientCapabilities":{},
                "io.modelcontextprotocol/clientInfo":{"name":"card-test", "version":"1"}
            }}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "MCP discovery should remain routed after merge"
    );
    let text = response.text().await.unwrap();
    let payload = text
        .lines()
        .find_map(|line| line.strip_prefix("data: "))
        .unwrap_or(&text);
    let discovery: Value = serde_json::from_str(payload).unwrap();
    assert!(
        discovery.get("error").is_none(),
        "discovery rejected: {discovery}"
    );
    assert_eq!(discovery["id"], 1);
    assert_eq!(
        discovery["result"]["supportedVersions"],
        json!(["2026-07-28"])
    );
    server.stop().await;
}
