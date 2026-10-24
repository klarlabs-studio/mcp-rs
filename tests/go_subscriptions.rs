//! Regression tests for explicit pinned-Go HTTP subscriptions.
#![cfg(all(feature = "client-go-subscriptions", feature = "http"))]
#![allow(clippy::unwrap_used)]
use axum::{Router, body::Body, http::Response, routing::post};
use mcp_rs::{
    client::go_subscriptions::{Config, Error, Subscription},
    model::SubscriptionFilter,
};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

fn ack(id: &str) -> String {
    format!(
        "data: {}\n\n",
        json!({"jsonrpc":"2.0","id":"mcp-rs-go-listen","result":{"subscriptionId":id}})
    )
}
fn update(id: &str, uri: &str) -> String {
    format!(
        "data: {}\n\n",
        json!({"jsonrpc":"2.0","method":"notifications/resources/updated","params":{"uri":uri,"_meta":{"io.modelcontextprotocol/subscriptionId":id}}})
    )
}
fn filter() -> SubscriptionFilter {
    SubscriptionFilter::builder()
        .resource_subscription("users://Felix")
        .build()
}
async fn fixture(
    content_type: &'static str,
    data: String,
) -> (
    Config,
    Arc<Mutex<Option<Value>>>,
    CancellationToken,
    tokio::task::JoinHandle<()>,
) {
    let captured = Arc::new(Mutex::new(None));
    let captured_handler = captured.clone();
    let router = Router::new().route(
        "/mcp",
        post(
            move |headers: axum::http::HeaderMap, body: axum::body::Bytes| {
                let captured = captured_handler.clone();
                let data = data.clone();
                async move {
                    assert_eq!(headers["mcp-method"], "subscriptions/listen");
                    assert_eq!(headers.get_all("mcp-method").iter().count(), 1);
                    assert_eq!(headers["content-type"], "application/json");
                    assert_eq!(headers["accept"], "application/json, text/event-stream");
                    assert_eq!(headers["mcp-protocol-version"], "2026-07-28");
                    *captured.lock().unwrap() = Some(serde_json::from_slice(&body).unwrap());
                    Response::builder()
                        .header("content-type", content_type)
                        .body(Body::from(data))
                        .unwrap()
                }
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = Config::new(
        format!("http://{}/mcp", listener.local_addr().unwrap())
            .parse()
            .unwrap(),
    );
    let stop = CancellationToken::new();
    let done = stop.clone();
    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(done.cancelled_owned())
            .await
            .unwrap()
    });
    (config, captured, stop, task)
}
#[tokio::test]
async fn translates_filters_and_preserves_opaque_scoped_ids() {
    let id = "quoted\"slash\\unicode🦀";
    let data = ack(id) + &update(id, "users://Other") + &update(id, "users://Felix");
    let (mut config, captured, stop, task) =
        fixture("text/event-stream; charset=utf-8", data).await;
    for name in [
        "content-type",
        "accept",
        "mcp-method",
        "mcp-protocol-version",
    ] {
        config.headers.append(name, "incorrect".parse().unwrap());
        config.headers.append(name, "duplicate".parse().unwrap());
    }
    let mut stream = Subscription::open(reqwest::Client::new(), config, filter())
        .await
        .unwrap();
    assert_eq!(stream.id(), id);
    assert_eq!(stream.requested_filter(), &filter());
    let request = captured.lock().unwrap().clone().unwrap();
    assert_eq!(request["params"]["uris"], json!(["users://Felix"]));
    assert_eq!(
        request["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"],
        "2026-07-28"
    );
    let event = serde_json::to_value(stream.next().await.unwrap().unwrap()).unwrap();
    assert_eq!(event["params"]["uri"], "users://Felix");
    assert_eq!(
        event["params"]["_meta"]["io.modelcontextprotocol/subscriptionId"],
        id
    );
    assert!(matches!(stream.next().await, Err(Error::Abrupt)));
    assert!(stream.next().await.unwrap().is_none());
    stop.cancel();
    task.await.unwrap();
}
#[tokio::test]
async fn rejects_cross_stream_ids_and_becomes_terminal() {
    let (config, _, stop, task) = fixture(
        "text/event-stream",
        ack("one") + &update("two", "users://Felix"),
    )
    .await;
    let mut stream = Subscription::open(reqwest::Client::new(), config, filter())
        .await
        .unwrap();
    assert!(matches!(stream.next().await, Err(Error::Invalid(_))));
    assert!(stream.next().await.unwrap().is_none());
    stop.cancel();
    task.await.unwrap();
}
#[tokio::test]
async fn validates_ack_envelopes_and_non_streaming_replies() {
    for body in [
        json!({"jsonrpc":"2.0","id":"wrong","result":{"subscriptionId":"one"}}),
        json!({"jsonrpc":"2.0","id":"mcp-rs-go-listen","result":{"subscriptionId":""}}),
        json!({"jsonrpc":"2.0","id":"mcp-rs-go-listen","result":{"subscriptionId":"one"},"error":{"code":-32602,"message":"bad"}}),
    ] {
        let (config, _, stop, task) =
            fixture("text/event-stream", format!("data: {body}\n\n")).await;
        assert!(matches!(
            Subscription::open(reqwest::Client::new(), config, filter()).await,
            Err(Error::Invalid(_))
        ));
        stop.cancel();
        task.await.unwrap();
    }
    let body = json!({"jsonrpc":"2.0","id":"mcp-rs-go-listen","result":{"subscriptionId":"one"}})
        .to_string();
    let (config, _, stop, task) = fixture("application/json", body).await;
    assert!(matches!(
        Subscription::open(reqwest::Client::new(), config, filter()).await,
        Err(Error::Invalid(_))
    ));
    stop.cancel();
    task.await.unwrap();
}
#[tokio::test]
async fn surfaces_json_rpc_rejections_in_json_and_sse() {
    let error = json!({"jsonrpc":"2.0","id":"mcp-rs-go-listen","error":{"code":-32602,"message":"denied","data":{"reason":"policy"}}});
    for content_type in ["application/json", "text/event-stream"] {
        let body = if content_type == "application/json" {
            error.to_string()
        } else {
            format!("data: {error}\n\n")
        };
        let (config, _, stop, task) = fixture(content_type, body).await;
        let error = Subscription::open(reqwest::Client::new(), config, filter())
            .await
            .err()
            .unwrap();
        let Error::Protocol(error) = error else {
            panic!("wrong error")
        };
        assert_eq!(error.data, Some(json!({"reason":"policy"})));
        stop.cancel();
        task.await.unwrap();
    }
}
#[tokio::test]
async fn bounds_requests_rejections_and_incomplete_events() {
    let (mut config, _, stop, task) = fixture(
        "text/event-stream",
        ack("one") + &format!("data: {}", "x".repeat(2000)),
    )
    .await;
    config.max_message_bytes = std::num::NonZeroUsize::new(1024).unwrap();
    let mut stream = Subscription::open(reqwest::Client::new(), config.clone(), filter())
        .await
        .unwrap();
    assert!(matches!(stream.next().await, Err(Error::Limit)));
    config.max_message_bytes = std::num::NonZeroUsize::MIN;
    assert!(matches!(
        Subscription::open(reqwest::Client::new(), config, filter()).await,
        Err(Error::Limit)
    ));
    stop.cancel();
    task.await.unwrap();
    let (mut config, _, stop, task) = fixture("application/json", "x".repeat(2000)).await;
    config.max_message_bytes = std::num::NonZeroUsize::new(1024).unwrap();
    assert!(matches!(
        Subscription::open(reqwest::Client::new(), config, filter()).await,
        Err(Error::Limit)
    ));
    stop.cancel();
    task.await.unwrap();
}
#[tokio::test]
async fn validates_configuration_without_network_and_redacts_debug() {
    let client = reqwest::Client::new();
    let mut config = Config::new(
        "http://user:secret@localhost/mcp?token=private"
            .parse()
            .unwrap(),
    );
    config
        .headers
        .insert("authorization", "Bearer private".parse().unwrap());
    assert!(!format!("{config:?}").contains("private"));
    assert!(!format!("{config:?}").contains("secret"));
    assert!(matches!(
        Subscription::open(client.clone(), config, filter()).await,
        Err(Error::Invalid(_))
    ));
    let mut config = Config::new("http://localhost/mcp".parse().unwrap());
    config.connect_timeout = Duration::ZERO;
    assert!(matches!(
        Subscription::open(client.clone(), config.clone(), filter()).await,
        Err(Error::Invalid(_))
    ));
    config.connect_timeout = Duration::from_secs(1);
    let oversized = SubscriptionFilter::builder()
        .resource_subscriptions(vec!["x"; 1001])
        .build();
    assert!(matches!(
        Subscription::open(client, config, oversized).await,
        Err(Error::Limit)
    ));
}
#[tokio::test]
async fn opening_timeout_includes_ack_and_cancelled_next_preserves_partial_event() {
    use futures_util::stream;
    let chunks = vec![ack("one"), "data: {\"jsonrpc\":\"2.0\",".into(), "\"method\":\"notifications/resources/updated\",\"params\":{\"uri\":\"users://Felix\",\"_meta\":{\"io.modelcontextprotocol/subscriptionId\":\"one\"}}}\n\n".into()];
    let router = Router::new()
        .route(
            "/mcp",
            post(move || {
                let chunks = chunks.clone();
                async move {
                    let bytes =
                        stream::unfold((chunks.into_iter(), 0), |(mut chunks, index)| async move {
                            let chunk = chunks.next()?;
                            if index == 2 {
                                tokio::time::sleep(Duration::from_millis(150)).await;
                            }
                            Some((Ok::<_, std::io::Error>(chunk), (chunks, index + 1)))
                        });
                    Response::builder()
                        .header("content-type", "text/event-stream")
                        .body(Body::from_stream(bytes))
                        .unwrap()
                }
            }),
        )
        .route(
            "/slow",
            post(|| async {
                tokio::time::sleep(Duration::from_secs(2)).await;
                ""
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let stop = CancellationToken::new();
    let done = stop.clone();
    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(done.cancelled_owned())
            .await
            .unwrap()
    });
    let config = Config::new(format!("http://{address}/mcp").parse().unwrap());
    let mut stream = Subscription::open(reqwest::Client::new(), config, filter())
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(25), stream.next())
            .await
            .is_err()
    );
    assert!(stream.next().await.unwrap().is_some());
    stream.close();
    assert!(stream.next().await.unwrap().is_none());
    let mut config = Config::new(format!("http://{address}/slow").parse().unwrap());
    config.connect_timeout = Duration::from_millis(25);
    assert!(matches!(
        Subscription::open(reqwest::Client::new(), config, filter()).await,
        Err(Error::Timeout)
    ));
    stop.cancel();
    task.await.unwrap();
}

fn task_event(id: &str, task_id: &str, status: &str, payload: Value) -> String {
    let mut params = json!({"taskId":task_id,"status":status,
        "createdAt":"2026-10-22T00:00:00Z","lastUpdatedAt":"2026-10-22T00:00:01Z",
        "ttlMs":null,"_meta":{"io.modelcontextprotocol/subscriptionId":id}});
    params
        .as_object_mut()
        .unwrap()
        .extend(payload.as_object().unwrap().clone());
    format!(
        "data: {}\n\n",
        json!({"jsonrpc":"2.0","method":"notifications/tasks","params":params})
    )
}

#[tokio::test]
async fn task_filters_preserve_typed_lifecycle_payloads_and_mix_with_resources() {
    let task_id = "opaque\"🦀\\task";
    let states = [
        ("working", json!({})),
        ("input_required", json!({"inputRequests":{}})),
        (
            "completed",
            json!({"result":{"content":[{"type":"text","text":"done"}]}}),
        ),
        (
            "failed",
            json!({"error":{"code":-32603,"message":"failed"}}),
        ),
        ("cancelled", json!({})),
    ];
    let mut data = ack("one") + &task_event("one", "unrequested", "working", json!({}));
    for (status, payload) in &states {
        data += &task_event("one", task_id, status, payload.clone());
    }
    data += &update("one", "users://Felix");
    let (config, captured, stop, server) = fixture("text/event-stream", data).await;
    let mut stream =
        Subscription::open_with_tasks(reqwest::Client::new(), config, filter(), [task_id])
            .await
            .unwrap();
    assert_eq!(stream.requested_task_ids(), &[task_id]);
    let request = captured.lock().unwrap().clone().unwrap();
    assert_eq!(
        request["params"]["notifications"]["taskIds"],
        json!([task_id])
    );
    assert_eq!(request["params"]["uris"], json!(["users://Felix"]));
    for (status, payload) in states {
        let event = stream.next().await.unwrap().unwrap();
        assert!(matches!(
            event,
            mcp_rs::model::ServerNotification::TaskStatusNotification(_)
        ));
        let value = serde_json::to_value(event).unwrap();
        assert_eq!(value["params"]["status"], status);
        assert_eq!(value["params"]["taskId"], task_id);
        assert_eq!(
            value["params"]["_meta"]["io.modelcontextprotocol/subscriptionId"],
            "one"
        );
        for (key, expected) in payload.as_object().unwrap() {
            assert_eq!(&value["params"][key], expected);
        }
    }
    assert_eq!(
        serde_json::to_value(stream.next().await.unwrap().unwrap()).unwrap()["params"]["uri"],
        "users://Felix"
    );
    stream.close();
    stop.cancel();
    server.await.unwrap();
}

#[tokio::test]
async fn malformed_known_task_notifications_cannot_fall_back_to_custom() {
    for (status, payload) in [
        ("completed", json!({})),
        ("failed", json!({})),
        ("unknown", json!({})),
    ] {
        let (config, _, stop, server) = fixture(
            "text/event-stream",
            ack("one") + &task_event("one", "task", status, payload),
        )
        .await;
        let mut stream = Subscription::open_with_tasks(
            reqwest::Client::new(),
            config,
            SubscriptionFilter::default(),
            ["task"],
        )
        .await
        .unwrap();
        assert!(matches!(stream.next().await, Err(Error::Json(_))));
        assert!(stream.next().await.unwrap().is_none());
        stop.cancel();
        server.await.unwrap();
    }
}

#[tokio::test]
async fn task_only_filters_skip_unrequested_tasks_and_reject_cross_stream_events() {
    let (config, captured, stop, server) = fixture(
        "text/event-stream",
        ack("one")
            + &task_event("one", "other", "working", json!({}))
            + &task_event("two", "task", "working", json!({})),
    )
    .await;
    let mut stream = Subscription::open_with_tasks(
        reqwest::Client::new(),
        config,
        SubscriptionFilter::default(),
        ["task"],
    )
    .await
    .unwrap();
    assert_eq!(
        captured.lock().unwrap().as_ref().unwrap()["params"]["notifications"],
        json!({"taskIds":["task"]})
    );
    assert!(matches!(stream.next().await, Err(Error::Invalid(_))));
    assert!(stream.next().await.unwrap().is_none());
    stop.cancel();
    server.await.unwrap();
}

#[tokio::test]
async fn task_filter_limits_stop_unbounded_iterators_before_network() {
    let config = Config::new("http://127.0.0.1:1/mcp".parse().unwrap());
    for ids in [
        vec![String::new()],
        vec!["x".repeat(4097)],
        vec!["duplicate".to_owned(); 1001],
    ] {
        assert!(matches!(
            Subscription::open_with_tasks(
                reqwest::Client::new(),
                config.clone(),
                SubscriptionFilter::default(),
                ids
            )
            .await,
            Err(Error::Limit)
        ));
    }
    assert!(matches!(
        Subscription::open_with_tasks(
            reqwest::Client::new(),
            config.clone(),
            SubscriptionFilter::default(),
            std::iter::repeat("task")
        )
        .await,
        Err(Error::Limit)
    ));
    let mut small = config;
    small.max_message_bytes = std::num::NonZeroUsize::MIN;
    assert!(matches!(
        Subscription::open_with_tasks(
            reqwest::Client::new(),
            small,
            SubscriptionFilter::default(),
            ["task"]
        )
        .await,
        Err(Error::Limit)
    ));
}

#[tokio::test]
async fn shared_task_filter_is_validated_and_merged_with_explicit_go_ids() {
    for explicit in [false, true] {
        let data = ack("one") + &task_event("one", "shared", "working", json!({}));
        let (config, captured, stop, server) = fixture("text/event-stream", data).await;
        let requested = SubscriptionFilter::builder().task_id("shared").build();
        let mut stream = if explicit {
            Subscription::open_with_tasks(reqwest::Client::new(), config, requested, ["extra"])
                .await
                .unwrap()
        } else {
            Subscription::open(reqwest::Client::new(), config, requested)
                .await
                .unwrap()
        };
        let expected = if explicit {
            vec!["shared", "extra"]
        } else {
            vec!["shared"]
        };
        assert_eq!(stream.requested_task_ids(), &expected);
        assert_eq!(
            captured.lock().unwrap().as_ref().unwrap()["params"]["notifications"]["taskIds"],
            json!(expected)
        );
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            mcp_rs::model::ServerNotification::TaskStatusNotification(_)
        ));
        stream.close();
        stop.cancel();
        server.await.unwrap();
    }
    let config = Config::new("http://127.0.0.1:1/mcp".parse().unwrap());
    assert!(matches!(
        Subscription::open(
            reqwest::Client::new(),
            config,
            SubscriptionFilter::builder().task_id("").build()
        )
        .await,
        Err(Error::Limit)
    ));
}
