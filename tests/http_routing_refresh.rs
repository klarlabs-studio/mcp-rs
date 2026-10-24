//! HTTP routing stays current across live schema changes and preserves encoded values.
#![cfg(feature = "http")]
#![allow(clippy::unwrap_used)]

use axum::http::{HeaderMap, HeaderName, HeaderValue};
use mcp_rs::Server;
use schemars::JsonSchema;
use serde::Deserialize;
#[cfg(feature = "client-http")]
use serde::Serialize;
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
struct Region {
    #[serde(default)]
    #[schemars(extend("x-mcp-header" = "Region"))]
    region: String,
}
#[derive(Deserialize, JsonSchema)]
struct Zone {
    #[schemars(extend("x-mcp-header" = "Zone"))]
    region: String,
}
#[derive(Deserialize, JsonSchema)]
struct Plain {
    region: String,
}

fn register_region(server: &mut Server, calls: Arc<AtomicUsize>) {
    server
        .tool("deploy")
        .handler(move |_, input: Region| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok(format!("region:{}", input.region)) }
        })
        .unwrap();
}
fn register_zone(server: &mut Server, calls: Arc<AtomicUsize>) {
    server
        .tool("deploy")
        .handler(move |_, input: Zone| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok(format!("zone:{}", input.region)) }
        })
        .unwrap();
}

struct Fixture {
    http: reqwest::Client,
    url: String,
    stop: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}
impl Fixture {
    async fn router(router: axum::Router, stop: CancellationToken) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/mcp", listener.local_addr().unwrap());
        let shutdown = stop.clone();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
                .unwrap();
        });
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(3))
                .build()
                .unwrap(),
            url,
            stop,
            task,
        }
    }
    async fn new(server: impl mcp_rs::protocol::ServerHandler + Clone) -> Self {
        let stop = CancellationToken::new();
        let mut config = mcp_rs::transport::HttpServerConfig::default();
        config.json_response = true;
        Self::router(
            mcp_rs::transport::http_router_with_config(server, stop.clone(), config),
            stop,
        )
        .await
    }
    async fn post(&self, method: &str, mut params: Value, headers: HeaderMap) -> reqwest::Response {
        params["_meta"] = json!({
            "io.modelcontextprotocol/protocolVersion":"2026-07-28",
            "io.modelcontextprotocol/clientInfo":{"name":"routing-refresh", "version":"1"},
            "io.modelcontextprotocol/clientCapabilities":{}
        });
        self.http
            .post(&self.url)
            .headers(headers)
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", "2026-07-28")
            .json(&json!({"jsonrpc":"2.0", "id":1, "method":method, "params":params}))
            .send()
            .await
            .unwrap()
    }
    async fn deploy(&self, headers: HeaderMap) -> reqwest::Response {
        self.post(
            "tools/call",
            json!({"name":"deploy", "arguments":{"region":"eu"}}),
            headers,
        )
        .await
    }
    async fn close(self) {
        self.stop.cancel();
        tokio::time::timeout(Duration::from_secs(3), self.task)
            .await
            .unwrap()
            .unwrap();
    }
}

fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in pairs {
        headers.append(
            HeaderName::from_bytes(name.as_bytes()).unwrap(),
            HeaderValue::from_str(value).unwrap(),
        );
    }
    headers
}
fn deploy_headers(param: Option<&str>) -> HeaderMap {
    let mut result = headers(&[("Mcp-Method", "tools/call"), ("Mcp-Name", "deploy")]);
    if let Some(param) = param {
        result.insert(
            HeaderName::from_bytes(param.as_bytes()).unwrap(),
            HeaderValue::from_static("eu"),
        );
    }
    result
}
async fn mismatch(response: reqwest::Response) {
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["error"]["code"], -32020, "{body}");
}
async fn success(response: reqwest::Response, expected: &str) {
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["result"]["content"][0]["text"], expected, "{body}");
}

#[tokio::test]
async fn live_tool_schema_replacement_changes_routing_admission_immediately() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut server = Server::new("live-routing", "1");
    register_region(&mut server, calls.clone());
    let live = server.into_live();
    let fixture = Fixture::new(live.clone()).await;
    success(
        fixture
            .deploy(deploy_headers(Some("mcp-param-region")))
            .await,
        "region:eu",
    )
    .await;
    let mut replacement = live.snapshot();
    assert!(replacement.remove_tool("deploy"));
    register_zone(&mut replacement, calls.clone());
    live.replace(replacement);
    mismatch(
        fixture
            .deploy(deploy_headers(Some("mcp-param-region")))
            .await,
    )
    .await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    success(
        fixture.deploy(deploy_headers(Some("mcp-param-zone"))).await,
        "zone:eu",
    )
    .await;

    let mut replacement = live.snapshot();
    assert!(replacement.remove_tool("deploy"));
    let invoked = calls.clone();
    replacement
        .tool("deploy")
        .handler(move |_, input: Plain| {
            invoked.fetch_add(1, Ordering::SeqCst);
            async move { Ok(format!("plain:{}", input.region)) }
        })
        .unwrap();
    live.replace(replacement);
    success(fixture.deploy(deploy_headers(None)).await, "plain:eu").await;
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    fixture.close().await;
}

#[tokio::test]
async fn missing_tool_lookup_does_not_disable_routing_checks_after_registration() {
    let calls = Arc::new(AtomicUsize::new(0));
    let live = Server::new("added-routing", "1").into_live();
    let fixture = Fixture::new(live.clone()).await;
    let missing: Value = fixture
        .deploy(deploy_headers(None))
        .await
        .json()
        .await
        .unwrap();
    assert!(missing.get("error").is_some(), "{missing}");
    let mut replacement = live.snapshot();
    register_region(&mut replacement, calls.clone());
    live.replace(replacement);
    mismatch(fixture.deploy(deploy_headers(None)).await).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    success(
        fixture
            .deploy(deploy_headers(Some("mcp-param-region")))
            .await,
        "region:eu",
    )
    .await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    fixture.close().await;
}

#[tokio::test]
async fn validation_and_dispatch_use_one_service_factory_result_per_request() {
    use mcp_rs::protocol::transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::never::NeverSessionManager,
    };
    let builds = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let factories = builds.clone();
    let invoked = calls.clone();
    let stop = CancellationToken::new();
    let mut config = StreamableHttpServerConfig::default().with_cancellation_token(stop.clone());
    config.json_response = true;
    let service = StreamableHttpService::new(
        move || {
            let index = factories.fetch_add(1, Ordering::SeqCst);
            let mut server = Server::new("factory-routing", "1");
            if index.is_multiple_of(2) {
                register_region(&mut server, invoked.clone());
            } else {
                register_zone(&mut server, invoked.clone());
            }
            Ok(server)
        },
        Arc::new(NeverSessionManager::default()),
        config,
    );
    let fixture = Fixture::router(axum::Router::new().nest_service("/mcp", service), stop).await;
    success(
        fixture
            .deploy(deploy_headers(Some("mcp-param-region")))
            .await,
        "region:eu",
    )
    .await;
    assert_eq!(builds.load(Ordering::SeqCst), 1);
    success(
        fixture.deploy(deploy_headers(Some("mcp-param-zone"))).await,
        "zone:eu",
    )
    .await;
    assert_eq!(builds.load(Ordering::SeqCst), 2);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    fixture.close().await;
}

#[tokio::test]
async fn malformed_optional_parameter_headers_are_rejected_before_default_arguments_dispatch() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut server = Server::new("optional-routing", "1");
    register_region(&mut server, calls.clone());
    let fixture = Fixture::new(server).await;
    for bad in [
        HeaderValue::from_bytes(&[0x80]).unwrap(),
        HeaderValue::from_static("=?base64?!!!!?="),
        HeaderValue::from_static("=?base64?/w==?="),
    ] {
        let mut map = deploy_headers(None);
        map.insert("mcp-param-region", bad);
        mismatch(
            fixture
                .post("tools/call", json!({"name":"deploy", "arguments":{}}), map)
                .await,
        )
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
    success(
        fixture
            .post(
                "tools/call",
                json!({"name":"deploy", "arguments":{}}),
                deploy_headers(None),
            )
            .await,
        "region:",
    )
    .await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    fixture.close().await;
}

#[cfg(feature = "client-http")]
#[derive(Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
struct Routed {
    #[schemars(extend("x-mcp-header" = "Label"))]
    label: String,
    #[schemars(extend("x-mcp-header" = "Count"))]
    count: i64,
    #[schemars(extend("x-mcp-header" = "Enabled"))]
    enabled: bool,
}

#[cfg(feature = "client-http")]
#[tokio::test]
async fn typed_http_client_round_trips_encoded_names_strings_integers_and_booleans() {
    use mcp_rs::{
        Client, protocol::transport::streamable_http_client::StreamableHttpClientTransportConfig,
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let invoked = calls.clone();
    let mut server = Server::new("encoded-routing", "1");
    let name = "route café";
    server
        .tool(name)
        .output_schema::<Routed>()
        .unwrap()
        .handler(move |_, input: Routed| {
            invoked.fetch_add(1, Ordering::SeqCst);
            async move { Ok(input) }
        })
        .unwrap();
    let fixture = Fixture::new(server).await;
    let client = Client::http(
        fixture.http.clone(),
        StreamableHttpClientTransportConfig::with_uri(fixture.url.clone()),
    )
    .await
    .unwrap();
    client
        .peer()
        .list_tools(Some(
            serde_json::from_value(json!({"detail":"full"})).unwrap(),
        ))
        .await
        .unwrap();
    for (index, label) in [
        "plain",
        " leading",
        "trailing ",
        "café ☕",
        "line\nnext\r\nx: 1",
        "=?base64?eA==?=",
        "",
        "\t",
    ]
    .into_iter()
    .enumerate()
    {
        let input = Routed {
            label: label.into(),
            count: if index.is_multiple_of(2) {
                i64::MIN
            } else {
                i64::MAX
            },
            enabled: index.is_multiple_of(2),
        };
        let result: Routed = client.call(name, &input).await.unwrap();
        assert_eq!(result, input);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 8);
    client.close().await.unwrap();
    fixture.close().await;
}

// Replace the shared registry synchronously after schema lookup, before dispatch.
// This orders the race without sleeps or scheduler-dependent timing.
#[derive(Clone)]
struct ReplaceDuringLookup {
    served: mcp_rs::LiveServer,
    current: mcp_rs::LiveServer,
    replacement: Server,
    replaced: Arc<std::sync::atomic::AtomicBool>,
}
impl mcp_rs::protocol::ServerHandler for ReplaceDuringLookup {
    fn request_snapshot(&self) -> Option<Self> {
        Some(Self {
            served: self
                .served
                .request_snapshot()
                .unwrap_or_else(|| self.served.clone()),
            ..self.clone()
        })
    }
    fn get_info(&self) -> mcp_rs::model::ServerConfig {
        self.served.get_info()
    }
    fn get_tool(&self, name: &str) -> Option<mcp_rs::model::Tool> {
        let tool = self.served.get_tool(name);
        if !self.replaced.swap(true, Ordering::SeqCst) {
            self.current.replace(self.replacement.clone());
        }
        tool
    }
    async fn call_tool(
        &self,
        request: mcp_rs::model::CallToolRequestParams,
        context: mcp_rs::Context,
    ) -> Result<mcp_rs::model::CallToolResponse, mcp_rs::protocol::ErrorData> {
        self.served.call_tool(request, context).await
    }
}

async fn replacement_between_admission_and_dispatch<H>(wrap: impl FnOnce(ReplaceDuringLookup) -> H)
where
    H: mcp_rs::protocol::ServerHandler + Clone,
{
    let calls = Arc::new(AtomicUsize::new(0));
    let mut server = Server::new("snapshot-routing", "1");
    register_region(&mut server, calls.clone());
    let live = server.into_live();
    let mut replacement = live.snapshot();
    assert!(replacement.remove_tool("deploy"));
    register_zone(&mut replacement, calls.clone());
    let fixture = Fixture::new(wrap(ReplaceDuringLookup {
        served: live.clone(),
        current: live,
        replacement,
        replaced: Arc::new(std::sync::atomic::AtomicBool::new(false)),
    }))
    .await;

    // Region headers were admitted before replacement, so execute Region too.
    success(
        fixture
            .deploy(deploy_headers(Some("mcp-param-region")))
            .await,
        "region:eu",
    )
    .await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    // The next request must see Zone, rather than retaining the first snapshot.
    mismatch(
        fixture
            .deploy(deploy_headers(Some("mcp-param-region")))
            .await,
    )
    .await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    success(
        fixture.deploy(deploy_headers(Some("mcp-param-zone"))).await,
        "zone:eu",
    )
    .await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    fixture.close().await;
}

#[tokio::test]
async fn live_http_snapshot_survives_replacement_between_admission_and_dispatch() {
    replacement_between_admission_and_dispatch(|handler| handler).await;
}

#[tokio::test]
async fn arc_handler_preserves_live_http_request_snapshot() {
    replacement_between_admission_and_dispatch(Arc::new).await;
}

#[tokio::test]
async fn boxed_handler_preserves_live_http_request_snapshot() {
    replacement_between_admission_and_dispatch(Box::new).await;
}

#[derive(Clone)]
struct RawRoutingSchema {
    schema: Arc<mcp_rs::model::JsonObject>,
    calls: Arc<AtomicUsize>,
}
impl mcp_rs::protocol::ServerHandler for RawRoutingSchema {
    fn get_tool(&self, name: &str) -> Option<mcp_rs::model::Tool> {
        (name == "deploy").then(|| mcp_rs::model::Tool::new("deploy", "raw", self.schema.clone()))
    }
    async fn call_tool(
        &self,
        _: mcp_rs::model::CallToolRequestParams,
        _: mcp_rs::Context,
    ) -> Result<mcp_rs::model::CallToolResponse, mcp_rs::protocol::ErrorData> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(
            mcp_rs::model::CallToolResult::success(vec![mcp_rs::model::ContentBlock::text("raw")])
                .into(),
        )
    }
}

#[tokio::test]
async fn custom_handler_invalid_routing_schemas_are_rejected_before_execution() {
    for (schema, parameter) in [
        (
            json!({"type":"object", "properties":{"region":{"type":"string", "x-mcp-header":""}}}),
            None,
        ),
        (
            json!({"type":"object", "properties":{"items":{"type":"array", "items":{"type":"string", "x-mcp-header":"Nested"}}}}),
            None,
        ),
        (
            json!({"type":"object", "properties":{"region":{"type":"number", "x-mcp-header":"Region"}}}),
            Some("mcp-param-region"),
        ),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let fixture = Fixture::new(RawRoutingSchema {
            schema: Arc::new(schema.as_object().unwrap().clone()),
            calls: calls.clone(),
        })
        .await;
        mismatch(fixture.deploy(deploy_headers(parameter)).await).await;
        assert_eq!(calls.load(Ordering::SeqCst), 0, "{schema}");
        fixture.close().await;
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let fixture = Fixture::new(RawRoutingSchema {
        schema: Arc::new(json!({"type":"object", "properties":{"region":{"type":"string", "x-mcp-header":"Region"}}}).as_object().unwrap().clone()),
        calls: calls.clone(),
    }).await;
    success(
        fixture
            .deploy(deploy_headers(Some("mcp-param-region")))
            .await,
        "raw",
    )
    .await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    fixture.close().await;
}

#[cfg(feature = "client-http")]
#[tokio::test]
async fn custom_routing_headers_cannot_contaminate_optional_arguments() {
    use mcp_rs::{
        Client, protocol::transport::streamable_http_client::StreamableHttpClientTransportConfig,
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let mut server = Server::new("custom-routing", "1");
    register_region(&mut server, calls.clone());
    let fixture = Fixture::new(server).await;
    let custom = [
        ("mcp-method", "stale/method"),
        ("mcp-name", "stale-name"),
        ("mcp-param-region", "stale-region"),
        ("x-tenant", "tenant-a"),
    ]
    .into_iter()
    .map(|(name, value)| {
        (
            HeaderName::from_static(name),
            HeaderValue::from_static(value),
        )
    })
    .collect();
    let client = Client::http(
        fixture.http.clone(),
        StreamableHttpClientTransportConfig::with_uri(fixture.url.clone()).custom_headers(custom),
    )
    .await
    .unwrap();
    client
        .peer()
        .list_tools(Some(
            serde_json::from_value(json!({"detail":"full"})).unwrap(),
        ))
        .await
        .unwrap();
    for (arguments, expected) in [
        (json!({}), "region:"),
        (json!({"region":"eu"}), "region:eu"),
        (json!({}), "region:"),
    ] {
        let result: String = client.call("deploy", &arguments).await.unwrap();
        assert_eq!(result, expected);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    client.close().await.unwrap();
    fixture.close().await;
}

#[cfg(feature = "client-http")]
#[tokio::test]
async fn bounded_routing_retention_restores_evicted_headers_after_uncached_discovery() {
    exercise_bounded_routing_retention(false).await;
}

#[cfg(feature = "client-http")]
#[tokio::test]
async fn routing_byte_budget_restores_evicted_headers_after_uncached_discovery() {
    exercise_bounded_routing_retention(true).await;
}

#[cfg(feature = "client-http")]
async fn exercise_bounded_routing_retention(use_byte_budget: bool) {
    use mcp_rs::{
        Client, model::CacheScope,
        protocol::transport::streamable_http_client::StreamableHttpClientTransportConfig,
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let mut server =
        Server::new("bounded-routing", "1").with_result_cache(60_000, CacheScope::Private);
    for name in ["alpha", "beta"] {
        let invoked = calls.clone();
        server
            .tool(name)
            .group(name)
            .handler(move |_, input: Region| {
                invoked.fetch_add(1, Ordering::SeqCst);
                async move { Ok(format!("{name}:{}", input.region)) }
            })
            .unwrap();
    }
    let fixture = Fixture::new(server).await;
    let config = StreamableHttpClientTransportConfig::with_uri(fixture.url.clone());
    let config = if use_byte_budget {
        // Either projection fits exactly, but both exceed the byte budget;
        // the count limit deliberately leaves room for both tools.
        let projection = json!({"type":"object", "properties":{
            "region":{"type":"string", "x-mcp-header":"Region"}
        }});
        config.max_routing_schemas(8).max_routing_schema_bytes(
            "alpha".len() + serde_json::to_vec(&projection).unwrap().len(),
        )
    } else {
        config.max_routing_schemas(1)
    };
    let client = Client::http(fixture.http.clone(), config).await.unwrap();
    for name in ["alpha", "beta"] {
        let tools = client
            .peer()
            .list_tools(Some(
                serde_json::from_value(json!({"detail":"full", "group":name})).unwrap(),
            ))
            .await
            .unwrap();
        assert_eq!(tools.tools.len(), 1);
        let result: String = client.call(name, &json!({"region":"eu"})).await.unwrap();
        assert_eq!(result, format!("{name}:eu"));
    }
    assert!(
        client
            .call::<_, String>("alpha", &json!({"region":"eu"}))
            .await
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    // A cached list response never crosses the HTTP transport. Force a fresh
    // discovery request to restore an evicted routing projection.
    client.peer().clear_response_cache().await;
    client
        .peer()
        .list_tools(Some(
            serde_json::from_value(json!({"detail":"full", "group":"alpha"})).unwrap(),
        ))
        .await
        .unwrap();
    let result: String = client.call("alpha", &json!({"region":"eu"})).await.unwrap();
    assert_eq!(result, "alpha:eu");
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    client.close().await.unwrap();
    fixture.close().await;
}

#[cfg(feature = "client-http")]
#[tokio::test]
async fn tool_list_notification_blocks_late_http_routing_refresh() {
    use axum::{body::Body, middleware::Next, response::IntoResponse};
    use mcp_rs::{
        Client, protocol::transport::streamable_http_client::StreamableHttpClientTransportConfig,
    };

    let calls = Arc::new(AtomicUsize::new(0));
    let mut server = Server::new("routing-invalidation", "1");
    register_region(&mut server, calls.clone());
    let stop = CancellationToken::new();
    let mut config = mcp_rs::transport::HttpServerConfig::default();
    config.json_response = true;
    let discoveries = Arc::new(AtomicUsize::new(0));
    let router = mcp_rs::transport::http_router_with_config(server, stop.clone(), config)
        .layer(axum::middleware::from_fn(move |request: axum::extract::Request, next: Next| {
            let discoveries = discoveries.clone();
            async move {
                let (parts, body) = request.into_parts();
                let bytes = axum::body::to_bytes(body, 1024 * 1024).await.unwrap();
                let message: Value = serde_json::from_slice(&bytes).unwrap();
                if message["method"] == "tools/list" {
                    let response = json!({"jsonrpc":"2.0", "id":message["id"], "result":{
                        "tools":[{"name":"deploy", "inputSchema":{"type":"object", "properties":{
                            "region":{"type":"string", "x-mcp-header":"Region"}
                        }}}]
                    }});
                    if discoveries.fetch_add(1, Ordering::SeqCst) == 1 {
                        // One POST delivers a change before its now-outdated list
                        // response, exercising the actual transport event loop.
                        return (
                            [("content-type", "text/event-stream")],
                            format!("data: {}\n\ndata: {response}\n\n",
                                json!({"jsonrpc":"2.0", "method":"notifications/tools/list_changed"})),
                        ).into_response();
                    }
                    return ([("content-type", "application/json")], response.to_string()).into_response();
                }
                next.run(axum::extract::Request::from_parts(parts, Body::from(bytes))).await
            }
        }));
    let fixture = Fixture::router(router, stop).await;
    let client = Client::http(
        fixture.http.clone(),
        StreamableHttpClientTransportConfig::with_uri(fixture.url.clone()),
    )
    .await
    .unwrap();
    for step in 0..3 {
        let list = client
            .peer()
            .list_tools(Some(
                serde_json::from_value(json!({"detail":"full"})).unwrap(),
            ))
            .await
            .unwrap();
        assert_eq!(list.tools.len(), 1);
        let result = client
            .call::<_, String>("deploy", &json!({"region":"eu"}))
            .await;
        if step == 1 {
            assert!(
                result.is_err(),
                "outdated discovery must not restore promoted headers"
            );
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        } else {
            assert_eq!(result.unwrap(), "region:eu");
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    client.close().await.unwrap();
    fixture.close().await;
}
