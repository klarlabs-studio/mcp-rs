//! Configurable HTTP admission and modern-only transport behavior.
#![cfg(feature = "http")]
#![allow(clippy::unwrap_used)]

use mcp_rs::{
    Server,
    transport::{HttpServerConfig, serve_http_with_config},
};
use reqwest::{Client, RequestBuilder, StatusCode};
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
use tokio_util::sync::CancellationToken;

#[derive(Deserialize, JsonSchema)]
struct Input {
    text: String,
}

struct Fixture {
    client: Client,
    url: String,
    calls: Arc<AtomicUsize>,
    stop: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}
impl Fixture {
    async fn new(config: HttpServerConfig, slow: bool) -> Self {
        let calls = Arc::new(AtomicUsize::new(0));
        let invoked = calls.clone();
        let mut server = Server::new("http-policy", "1");
        server
            .tool("echo")
            .handler(move |ctx, input: Input| {
                invoked.fetch_add(1, Ordering::SeqCst);
                async move {
                    mcp_rs::Session::from_context(&ctx)
                        .progress(1.0, Some(1.0), Some("ready".into()))
                        .await
                        .map_err(|error| mcp_rs::ToolError::new(error.to_string()))?;
                    if slow {
                        tokio::time::sleep(Duration::from_secs(600)).await;
                    }
                    Ok(input.text)
                }
            })
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/mcp", listener.local_addr().unwrap());
        let stop = CancellationToken::new();
        let shutdown = stop.clone();
        let task = tokio::spawn(async move {
            serve_http_with_config(server, listener, shutdown, config)
                .await
                .unwrap();
        });
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            url,
            calls,
            stop,
            task,
        }
    }
    fn post(&self) -> RequestBuilder {
        self.client
            .post(&self.url)
            .header("host", "mcp.example:8443")
            .header("mcp-protocol-version", "2026-07-28")
            .header("mcp-method", "tools/call")
            .header("mcp-name", "echo")
            .header("accept", "application/json, text/event-stream")
    }
    async fn close(self) {
        self.stop.cancel();
        tokio::time::timeout(Duration::from_secs(3), self.task)
            .await
            .unwrap()
            .unwrap();
    }
}
fn policy() -> HttpServerConfig {
    let mut config = HttpServerConfig::default();
    config.allowed_hosts = vec!["mcp.example:8443".into()];
    config.allowed_origins = Some(vec!["https://app.example:443".into()]);
    config.max_request_body_bytes = 1024;
    config.json_response = true;
    config
}
fn call(text: &str) -> Value {
    json!({"jsonrpc":"2.0", "id":1, "method":"tools/call", "params":{
        "name":"echo", "arguments":{"text":text}, "_meta":{
            "io.modelcontextprotocol/protocolVersion":"2026-07-28",
            "io.modelcontextprotocol/clientInfo":{"name":"policy-test","version":"1"},
            "io.modelcontextprotocol/clientCapabilities":{}
        }
    }})
}

#[tokio::test]
async fn configured_hosts_and_origins_reject_before_dispatch_and_allow_json() {
    let fixture = Fixture::new(policy(), false).await;
    for (header, value) in [
        ("host", "other.example:8443"),
        ("host", "mcp.example:8444"),
        ("origin", "https://other.example"),
        ("origin", "https://app.example:444"),
        ("origin", "http://app.example:443"),
        ("origin", "null"),
    ] {
        let mut overrides = reqwest::header::HeaderMap::new();
        overrides.insert(
            reqwest::header::HeaderName::from_bytes(header.as_bytes()).unwrap(),
            reqwest::header::HeaderValue::from_str(value).unwrap(),
        );
        let response = fixture
            .post()
            .headers(overrides)
            .json(&call("ok"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "{header}={value}");
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    }
    for origin in [
        Some("https://app.example"),
        Some("https://app.example:443"),
        None,
    ] {
        let request = fixture.post().json(&call("ok"));
        let request = if let Some(origin) = origin {
            request.header("origin", origin)
        } else {
            request
        };
        let response = request.send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], "application/json");
        assert!(!response.headers().contains_key("mcp-session-id"));
        let body: Value = response.json().await.unwrap();
        assert_eq!(body["result"]["content"][0]["text"], "ok");
    }
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 3);
    for method in [reqwest::Method::GET, reqwest::Method::DELETE] {
        let response = fixture
            .client
            .request(method, &fixture.url)
            .header("host", "mcp.example:8443")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    }
    let mut initialize_headers = reqwest::header::HeaderMap::new();
    initialize_headers.insert(
        "mcp-method",
        reqwest::header::HeaderValue::from_static("initialize"),
    );
    let response = fixture
        .post()
        .headers(initialize_headers)
        .json(&json!({"jsonrpc":"2.0", "id":2,
        "method":"initialize", "params":{"protocolVersion":"2025-03-26",
        "capabilities":{}, "clientInfo":{"name":"old","version":"1"}}}))
        .send()
        .await
        .unwrap();
    assert!(!response.headers().contains_key("mcp-session-id"));
    let body = response.text().await.unwrap();
    assert!(body.contains("-32600"), "{body}");
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 3);
    fixture.close().await;
}

#[tokio::test]
async fn empty_origin_allowlist_is_distinct_from_disabled_validation() {
    for origins in [Some(vec![]), None] {
        let mut config = policy();
        config.allowed_origins = origins.clone();
        let fixture = Fixture::new(config, false).await;
        let response = fixture
            .post()
            .header("origin", "https://unlisted.example")
            .json(&call("ok"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            if origins.is_some() {
                StatusCode::FORBIDDEN
            } else {
                StatusCode::OK
            }
        );
        let response = fixture.post().json(&call("ok")).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        fixture.close().await;
    }
}

#[tokio::test]
async fn body_limit_rejects_fixed_and_streamed_bodies_without_dispatch() {
    let fixture = Fixture::new(policy(), false).await;
    let large = call(&"x".repeat(2048)).to_string();
    let response = fixture
        .post()
        .header("content-type", "application/json")
        .body(large.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let chunks = large
        .as_bytes()
        .chunks(128)
        .map(|chunk| Ok::<_, std::io::Error>(chunk.to_vec()))
        .collect::<Vec<_>>();
    let response = fixture
        .post()
        .header("content-type", "application/json")
        .body(reqwest::Body::wrap_stream(futures_util::stream::iter(
            chunks,
        )))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    let response = fixture.post().json(&call("small")).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    fixture.close().await;
    let mut config = policy();
    config.max_request_body_bytes = 0;
    let fixture = Fixture::new(config, false).await;
    assert_eq!(
        fixture
            .post()
            .json(&call("small"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    fixture.close().await;
}

#[tokio::test]
async fn configured_server_shutdown_cancels_active_requests() {
    let fixture = Fixture::new(policy(), true).await;
    let request = fixture.post().json(&call("wait"));
    let pending = tokio::spawn(async move { request.send().await });
    tokio::time::timeout(Duration::from_secs(3), async {
        while fixture.calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    fixture.close().await;
    // Cancellation may close the connection or return a protocol error; either
    // way the handler's ten-minute sleep must not delay server/request teardown.
    tokio::time::timeout(Duration::from_secs(3), pending)
        .await
        .unwrap()
        .unwrap()
        .ok();
}

#[tokio::test]
async fn json_preference_falls_back_to_sse_without_losing_progress() {
    let fixture = Fixture::new(policy(), false).await;
    let mut request = call("done");
    request["params"]["_meta"]["progressToken"] = json!("policy-progress");
    let response = fixture.post().json(&request).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("text/event-stream")
    );
    assert!(!response.headers().contains_key("mcp-session-id"));
    let body = response.text().await.unwrap();
    let events = body
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|data| serde_json::from_str::<Value>(data).unwrap())
        .collect::<Vec<_>>();
    let progress = events
        .iter()
        .position(|event| event["method"] == "notifications/progress")
        .unwrap();
    let result = events.iter().position(|event| event["id"] == 1).unwrap();
    assert!(progress < result);
    assert_eq!(
        events[progress]["params"]["progressToken"],
        "policy-progress"
    );
    assert_eq!(events[result]["result"]["content"][0]["text"], "done");
    fixture.close().await;
}
