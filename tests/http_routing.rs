//! Integration coverage for the 2026-07-28 HTTP routing headers.
#![cfg(feature = "http")]
#![allow(clippy::unwrap_used)]

use axum::http::{HeaderMap, HeaderName, HeaderValue};
use mcp_rs::Server;
use schemars::JsonSchema;
use serde::Deserialize;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio_util::sync::CancellationToken;

const VERSION: &str = "2026-07-28";

#[derive(Deserialize, JsonSchema)]
struct Deploy {
    #[schemars(extend("x-mcp-header" = "Region"))]
    region: String,
}

async fn fixture() -> (
    reqwest::Client,
    String,
    Arc<AtomicUsize>,
    CancellationToken,
    tokio::task::JoinHandle<()>,
) {
    let calls = Arc::new(AtomicUsize::new(0));
    let stop = CancellationToken::new();
    let mut server = Server::new("http-routing", "1")
        .with_result_cache(60_000, mcp_rs::model::CacheScope::Public);
    let invoked = calls.clone();
    server
        .tool("deploy")
        .defer_schema()
        .handler(move |_, input: Deploy| {
            invoked.fetch_add(1, Ordering::SeqCst);
            async move { Ok(input.region) }
        })
        .unwrap();
    let router = mcp_rs::transport::http_router(server, stop.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/mcp", listener.local_addr().unwrap());
    let shutdown = stop.clone();
    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
            .unwrap();
    });
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()
        .unwrap();
    (client, url, calls, stop, task)
}

async fn post(client: &reqwest::Client, url: &str, headers: &[(&str, &str)]) -> reqwest::Response {
    let mut map = HeaderMap::new();
    map.insert("content-type", HeaderValue::from_static("application/json"));
    map.insert(
        "accept",
        HeaderValue::from_static("application/json, text/event-stream"),
    );
    map.insert("mcp-protocol-version", HeaderValue::from_static(VERSION));
    for (name, value) in headers {
        map.append(
            HeaderName::from_bytes(name.as_bytes()).unwrap(),
            HeaderValue::from_str(value).unwrap(),
        );
    }
    client
        .post(url)
        .headers(map)
        .json(&serde_json::json!({
            "jsonrpc":"2.0", "id":1, "method":"tools/call",
            "params":{
                "name":"deploy", "arguments":{"region":"eu-central1"},
                "_meta":{
                    "io.modelcontextprotocol/protocolVersion":"2026-07-28",
                    "io.modelcontextprotocol/clientInfo":{"name":"routing-test","version":"1"},
                    "io.modelcontextprotocol/clientCapabilities":{}
                }
            }
        }))
        .send()
        .await
        .unwrap()
}

async fn rejected(response: reqwest::Response) {
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["error"]["code"], -32020, "unexpected error: {body}");
}

#[tokio::test]
async fn matching_routing_headers_reach_handler_and_invalid_headers_do_not() {
    let (client, url, calls, stop, task) = fixture().await;

    let response = post(
        &client,
        &url,
        &[
            ("Mcp-Method", "tools/call"),
            ("Mcp-Name", "deploy"),
            ("Mcp-Param-Region", "eu-central1"),
        ],
    )
    .await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "matching headers should dispatch"
    );
    assert!(response.status().is_success());
    let body = response.text().await.unwrap();
    let data = body
        .lines()
        .find_map(|line| line.strip_prefix("data: "))
        .unwrap();
    let reply: serde_json::Value = serde_json::from_str(data).unwrap();
    assert_eq!(reply["id"], 1);
    assert_eq!(reply["result"]["content"][0]["text"], "eu-central1");
    assert!(reply.get("error").is_none(), "unexpected reply: {reply}");

    for headers in [
        vec![
            ("Mcp-Method", "tools/list"),
            ("Mcp-Name", "deploy"),
            ("Mcp-Param-Region", "eu-central1"),
        ],
        vec![("Mcp-Name", "deploy"), ("Mcp-Param-Region", "eu-central1")],
        vec![
            ("Mcp-Method", "tools/call"),
            ("Mcp-Name", "other"),
            ("Mcp-Param-Region", "eu-central1"),
        ],
        vec![
            ("Mcp-Method", "tools/call"),
            ("Mcp-Param-Region", "eu-central1"),
        ],
        vec![
            ("Mcp-Method", "tools/call"),
            ("Mcp-Name", "deploy"),
            ("Mcp-Param-Region", "us-west1"),
        ],
        vec![("Mcp-Method", "tools/call"), ("Mcp-Name", "deploy")],
    ] {
        rejected(post(&client, &url, &headers).await).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "invalid headers must be rejected before dispatch"
        );
    }

    stop.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn duplicate_routing_headers_are_rejected_before_dispatch() {
    let (client, url, calls, stop, task) = fixture().await;
    for headers in [
        vec![
            ("Mcp-Method", "tools/call"),
            ("Mcp-Method", "tools/list"),
            ("Mcp-Name", "deploy"),
            ("Mcp-Param-Region", "eu-central1"),
        ],
        vec![
            ("Mcp-Method", "tools/list"),
            ("Mcp-Method", "tools/call"),
            ("Mcp-Name", "deploy"),
            ("Mcp-Param-Region", "eu-central1"),
        ],
        vec![
            ("Mcp-Method", "tools/call"),
            ("Mcp-Method", "tools/call"),
            ("Mcp-Name", "deploy"),
            ("Mcp-Param-Region", "eu-central1"),
        ],
        vec![
            ("Mcp-Method", "tools/call"),
            ("Mcp-Name", "deploy"),
            ("Mcp-Name", "deploy"),
            ("Mcp-Param-Region", "eu-central1"),
        ],
        vec![
            ("Mcp-Method", "tools/call"),
            ("Mcp-Name", "deploy"),
            ("Mcp-Param-Region", "eu-central1"),
            ("Mcp-Param-Region", "eu-central1"),
        ],
    ] {
        rejected(post(&client, &url, &headers).await).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "duplicate headers must not dispatch"
        );
    }
    stop.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn modern_requests_cross_independent_instances_without_transport_sessions() {
    let (client, first_url, first_calls, first_stop, first_task) = fixture().await;
    let (_, second_url, second_calls, second_stop, second_task) = fixture().await;
    // Each instance owns a separate Server and router. No shared session manager,
    // initialize handshake or sticky routing is involved.
    for url in [&first_url, &second_url, &first_url, &second_url] {
        let response = post(
            &client,
            url,
            &[
                ("Mcp-Method", "tools/call"),
                ("Mcp-Name", "deploy"),
                ("Mcp-Param-Region", "eu-central1"),
            ],
        )
        .await;
        assert!(response.status().is_success());
        assert!(!response.headers().contains_key("mcp-session-id"));
        let body = response.text().await.unwrap();
        let data = body
            .lines()
            .find_map(|line| line.strip_prefix("data: "))
            .unwrap();
        let reply: serde_json::Value = serde_json::from_str(data).unwrap();
        assert_eq!(reply["result"]["content"][0]["text"], "eu-central1");
        assert!(reply.get("error").is_none());
    }
    assert_eq!(first_calls.load(Ordering::SeqCst), 2);
    assert_eq!(second_calls.load(Ordering::SeqCst), 2);

    for url in [&first_url, &second_url] {
        for method in [reqwest::Method::GET, reqwest::Method::DELETE] {
            for version in [VERSION, "2025-03-26"] {
                let response = client
                    .request(method.clone(), url)
                    .header("accept", "text/event-stream")
                    .header("mcp-protocol-version", version)
                    .header("mcp-session-id", "unrecognized-session")
                    .send()
                    .await
                    .unwrap();
                assert_eq!(response.status(), reqwest::StatusCode::METHOD_NOT_ALLOWED);
                assert_eq!(response.headers().get("allow").unwrap(), "POST");
                assert!(!response.headers().contains_key("mcp-session-id"));
            }
        }
    }
    first_stop.cancel();
    second_stop.cancel();
    for task in [first_task, second_task] {
        tokio::time::timeout(std::time::Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap();
    }
}

#[cfg(feature = "client-http")]
#[tokio::test]
async fn deferred_http_discovery_preserves_names_and_full_schema_restores_routing_headers() {
    use mcp_rs::protocol::transport::streamable_http_client::StreamableHttpClientTransportConfig;
    use mcp_rs::{Client, model::ListToolsRequestParams};
    let (_, url, calls, stop, task) = fixture().await;
    let client = Client::http(
        reqwest::Client::new(),
        StreamableHttpClientTransportConfig::with_uri(url),
    )
    .await
    .unwrap();
    let page = client.peer().list_tools(None).await.unwrap();
    assert_eq!(page.tools.len(), 1);
    assert_eq!(page.tools[0].name, "deploy");
    assert!(page.tools[0].input_schema.is_none());
    let full: ListToolsRequestParams =
        serde_json::from_value(serde_json::json!({"detail":"full"})).unwrap();
    let page = client.peer().list_tools(Some(full.clone())).await.unwrap();
    assert!(page.tools[0].input_schema.is_some());
    let result: String = client
        .call("deploy", &serde_json::json!({"region":"eu-central1"}))
        .await
        .unwrap();
    assert_eq!(result, "eu-central1");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let names = serde_json::from_value(serde_json::json!({"detail":"names"})).unwrap();
    assert!(
        client.peer().list_tools(Some(names)).await.unwrap().tools[0]
            .input_schema
            .is_none()
    );
    // A cached full listing still works after a lean listing.
    client.peer().list_tools(Some(full)).await.unwrap();
    let result: String = client
        .call("deploy", &serde_json::json!({"region":"us-west1"}))
        .await
        .unwrap();
    assert_eq!(result, "us-west1");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    client.close().await.unwrap();
    stop.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
}
