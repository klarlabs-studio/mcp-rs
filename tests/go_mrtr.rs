//! Bounds, replay and framing checks for the explicit modern Go MRTR adapter.
#![cfg(all(feature = "client-go-mrtr", feature = "http"))]
#![allow(clippy::unwrap_used)]
use axum::{Router, body::Body, http::Response, routing::post};
use mcp_rs::{
    client::go_mrtr::{Client, Config, Error},
    model::*,
};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;

const ID: &str = "mcp-rs-go-mrtr";
fn envelope(result: Value) -> Value {
    json!({"jsonrpc":"2.0","id":ID,"result":result})
}
fn input(id: &str) -> Value {
    json!({"id":id,"kind":"elicitation","payload":{"mode":"form","message":"Name?","requestedSchema":{"type":"object","properties":{"name":{"type":"string"}}}}})
}
fn pending(inputs: Vec<Value>) -> Value {
    envelope(
        json!({"resultType":"input_required","inputRequests":inputs,"requestState":{"opaque":[1,"🦀"]}}),
    )
}
fn complete() -> Value {
    envelope(json!({"resultType":"complete","contents":[],"ttlMs":77,"cacheScope":"private"}))
}
fn accept(_: ElicitRequestParams) -> std::future::Ready<Result<ElicitResult, ErrorData>> {
    std::future::ready(Ok(
        ElicitResult::new(ElicitationAction::Accept).with_content(json!({"name":"Ada"}))
    ))
}
struct Fixture {
    config: Config,
    requests: Arc<Mutex<Vec<Value>>>,
    stop: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}
impl Fixture {
    async fn new(content_type: &'static str, replies: Vec<String>, delay: Duration) -> Self {
        let requests = Arc::new(Mutex::new(vec![]));
        let seen = requests.clone();
        let replies = Arc::new(Mutex::new(VecDeque::from(replies)));
        let router = Router::new().route(
            "/mcp",
            post(
                move |headers: axum::http::HeaderMap, bytes: axum::body::Bytes| {
                    let seen = seen.clone();
                    let replies = replies.clone();
                    async move {
                        assert_eq!(headers["mcp-protocol-version"], "2026-07-28");
                        assert_eq!(headers["mcp-method"], "resources/read");
                        assert_eq!(headers["mcp-name"], "test://input");
                        assert_eq!(headers.get_all("mcp-method").iter().count(), 1);
                        seen.lock()
                            .unwrap()
                            .push(serde_json::from_slice(&bytes).unwrap());
                        tokio::time::sleep(delay).await;
                        let data = replies
                            .lock()
                            .unwrap()
                            .pop_front()
                            .unwrap_or_else(|| complete().to_string());
                        Response::builder()
                            .header("content-type", content_type)
                            .body(Body::from(data))
                            .unwrap()
                    }
                },
            ),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = Config::new(
            format!("http://{}/mcp", listener.local_addr().unwrap())
                .parse()
                .unwrap(),
        );
        config.capabilities = serde_json::from_value(json!({"elicitation":{"form":{}}})).unwrap();
        let stop = CancellationToken::new();
        let done = stop.clone();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(done.cancelled_owned())
                .await
                .unwrap();
        });
        Self {
            config,
            requests,
            stop,
            task,
        }
    }
    fn client(&self) -> Client {
        Client::new(reqwest::Client::new(), self.config.clone()).unwrap()
    }
    async fn close(self) {
        self.stop.cancel();
        tokio::time::timeout(Duration::from_secs(3), self.task)
            .await
            .unwrap()
            .unwrap();
    }
}
#[tokio::test]
async fn preserves_opaque_json_state_and_cumulative_answers_without_cross_call_leaks() {
    let f = Fixture::new(
        "application/json",
        vec![
            pending(vec![input("ir-0")]).to_string(),
            pending(vec![input("ir-1")]).to_string(),
            complete().to_string(),
            complete().to_string(),
        ],
        Duration::ZERO,
    )
    .await;
    let client = f.client();
    let result = client.read_resource("test://input", accept).await.unwrap();
    assert_eq!(result.ttl_ms, Some(77));
    assert_eq!(result.cache_scope, Some(CacheScope::Private));
    client.read_resource("test://input", accept).await.unwrap();
    {
        let requests = f.requests.lock().unwrap();
        let meta = &requests[2]["params"]["_meta"];
        assert_eq!(
            meta["io.modelcontextprotocol/requestState"],
            json!({"opaque":[1,"🦀"]})
        );
        assert_eq!(
            meta["io.modelcontextprotocol/inputResponses"],
            json!([
                {"id":"ir-0","payload":{"action":"accept","content":{"name":"Ada"}}},
                {"id":"ir-1","payload":{"action":"accept","content":{"name":"Ada"}}}
            ])
        );
        for i in [0, 3] {
            assert!(
                requests[i]["params"]["_meta"]
                    .get("io.modelcontextprotocol/inputResponses")
                    .is_none()
            );
        }
        assert!(requests[2]["params"].get("inputResponses").is_none());
    }
    f.close().await;
}
#[tokio::test]
async fn validates_entire_input_batch_before_callbacks_and_rejects_replays() {
    let mut first = input("z");
    first["payload"]["message"] = json!("First");
    let mut second = input("a");
    second["payload"]["message"] = json!("Second");
    let f = Fixture::new(
        "application/json",
        vec![
            pending(vec![first, second]).to_string(),
            complete().to_string(),
        ],
        Duration::ZERO,
    )
    .await;
    let mut messages = Vec::new();
    f.client()
        .read_resource("test://input", |input| {
            messages.push(serde_json::to_value(&input).unwrap()["message"].clone());
            accept(input)
        })
        .await
        .unwrap();
    assert_eq!(messages, vec![json!("First"), json!("Second")]);
    f.close().await;
    let mut unsupported = input("ir-1");
    unsupported["kind"] = json!("sampling");
    for inputs in [
        vec![input("ir-0"), input("ir-0")],
        vec![input("ir-0"), unsupported],
        vec![],
    ] {
        let f = Fixture::new(
            "application/json",
            vec![pending(inputs).to_string()],
            Duration::ZERO,
        )
        .await;
        let calls = AtomicUsize::new(0);
        let error = f
            .client()
            .read_resource("test://input", |input| {
                calls.fetch_add(1, Ordering::Relaxed);
                accept(input)
            })
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Invalid(_)));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        f.close().await;
    }
    let f = Fixture::new(
        "application/json",
        vec![pending(vec![input("ir-0")]).to_string(); 2],
        Duration::ZERO,
    )
    .await;
    let calls = AtomicUsize::new(0);
    assert!(matches!(
        f.client()
            .read_resource("test://input", |input| {
                calls.fetch_add(1, Ordering::Relaxed);
                accept(input)
            })
            .await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    f.close().await;
}
#[tokio::test]
async fn decodes_sse_and_rejects_wrong_envelopes_and_content_types() {
    let f = Fixture::new(
        "text/event-stream; charset=utf-8",
        vec![format!("event: message\ndata: {}\n\n", complete())],
        Duration::ZERO,
    )
    .await;
    f.client()
        .read_resource("test://input", accept)
        .await
        .unwrap();
    f.close().await;
    for (kind, data) in [
        (
            "application/json",
            json!({"jsonrpc":"2.0","id":"other","result":{}}).to_string(),
        ),
        (
            "application/json",
            json!({"jsonrpc":"2.0","id":ID,"result":{},"error":{}}).to_string(),
        ),
        ("text/html", complete().to_string()),
        ("text/event-stream", "data: {}\n\n".into()),
    ] {
        let f = Fixture::new(kind, vec![data], Duration::ZERO).await;
        assert!(matches!(
            f.client().read_resource("test://input", accept).await,
            Err(Error::Invalid(_))
        ));
        f.close().await;
    }
}
#[tokio::test]
async fn enforces_round_byte_and_whole_operation_deadlines() {
    let mut f = Fixture::new(
        "application/json",
        vec![
            pending(vec![input("ir-0")]).to_string(),
            pending(vec![input("ir-1")]).to_string(),
        ],
        Duration::ZERO,
    )
    .await;
    f.config.max_input_rounds = NonZeroUsize::new(1).unwrap();
    assert!(matches!(
        f.client().read_resource("test://input", accept).await,
        Err(Error::Limit)
    ));
    f.close().await;
    let mut f = Fixture::new(
        "application/json",
        vec![complete().to_string()],
        Duration::ZERO,
    )
    .await;
    f.config.max_message_bytes = NonZeroUsize::new(1).unwrap();
    assert!(matches!(
        f.client().read_resource("test://input", accept).await,
        Err(Error::Limit)
    ));
    assert!(f.requests.lock().unwrap().is_empty());
    f.close().await;
    let mut f = Fixture::new("application/json", vec![" ".repeat(8192)], Duration::ZERO).await;
    f.config.max_message_bytes = NonZeroUsize::new(4096).unwrap();
    assert!(matches!(
        f.client().read_resource("test://input", accept).await,
        Err(Error::Limit)
    ));
    f.close().await;
    let mut f = Fixture::new(
        "application/json",
        vec![pending(vec![input("ir-0")]).to_string()],
        Duration::ZERO,
    )
    .await;
    f.config.timeout = Duration::from_millis(40);
    assert!(matches!(
        f.client()
            .read_resource("test://input", |_| std::future::pending::<
                Result<ElicitResult, ErrorData>,
            >())
            .await,
        Err(Error::Timeout)
    ));
    f.close().await;
    let mut f = Fixture::new(
        "application/json",
        vec![complete().to_string()],
        Duration::from_millis(100),
    )
    .await;
    f.config.timeout = Duration::from_millis(30);
    assert!(matches!(
        f.client().read_resource("test://input", accept).await,
        Err(Error::Timeout)
    ));
    f.close().await;
}
#[tokio::test]
async fn propagates_peer_and_callback_errors_and_requires_declared_modes() {
    let f = Fixture::new(
        "application/json",
        vec![
            json!({"jsonrpc":"2.0","id":ID,"error":{"code":-32602,"message":"rejected"}})
                .to_string(),
        ],
        Duration::ZERO,
    )
    .await;
    assert!(
        matches!(f.client().read_resource("test://input",accept).await,Err(Error::Protocol(error)) if error.code == ErrorCode::INVALID_PARAMS)
    );
    f.close().await;
    let f = Fixture::new(
        "application/json",
        vec![pending(vec![input("ir-0")]).to_string()],
        Duration::ZERO,
    )
    .await;
    assert!(
        matches!(f.client().read_resource("test://input",|_| std::future::ready(Err(ErrorData::invalid_params("callback rejected",None)))).await,Err(Error::Protocol(error)) if error.message == "callback rejected")
    );
    f.close().await;
    let mut f = Fixture::new(
        "application/json",
        vec![pending(vec![input("ir-0")]).to_string()],
        Duration::ZERO,
    )
    .await;
    f.config.capabilities = ClientCapabilities::default();
    assert!(matches!(
        f.client().read_resource("test://input", accept).await,
        Err(Error::Invalid(_))
    ));
    f.close().await;
}
#[test]
fn rejects_reserved_headers_invalid_endpoints_and_redacts_debug() {
    for endpoint in [
        "ftp://example.test/mcp",
        "https://user:secret@example.test/mcp",
        "https://example.test/mcp#fragment",
    ] {
        assert!(
            Client::new(
                reqwest::Client::new(),
                Config::new(endpoint.parse().unwrap())
            )
            .is_err()
        );
    }
    for name in [
        "mcp-name",
        "mcp-method",
        "mcp-param-mode",
        "content-type",
        "host",
    ] {
        let mut config = Config::new("https://example.test/mcp?secret=private".parse().unwrap());
        config.headers.insert(name, "secret".parse().unwrap());
        assert!(!format!("{config:?}").contains("secret"));
        assert!(Client::new(reqwest::Client::new(), config).is_err());
    }
}
