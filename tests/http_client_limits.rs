//! Buffered response limits apply before parsing and remain independent of SSE.
#![cfg(all(feature = "http", feature = "client-http"))]
#![allow(clippy::unwrap_used)]

use std::{collections::HashMap, sync::Arc, time::Duration};

use axum::{
    body::{Body, Bytes},
    http::StatusCode,
    response::IntoResponse,
    routing::post,
};
use futures_util::{StreamExt, stream};
use mcp_rs::{
    Client, Server,
    protocol::{
        model::ClientJsonRpcMessage,
        transport::streamable_http_client::{
            StreamableHttpClient, StreamableHttpClientLimits, StreamableHttpClientTransportConfig,
            StreamableHttpError, StreamableHttpPostResponse,
        },
    },
};
use serde_json::json;
use tokio_util::sync::CancellationToken;

#[derive(serde::Deserialize, schemars::JsonSchema)]
struct Empty {}

struct Fixture {
    http: reqwest::Client,
    url: Arc<str>,
    stop: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}
impl Fixture {
    async fn router(router: axum::Router) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/mcp", listener.local_addr().unwrap()).into();
        let stop = CancellationToken::new();
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
    async fn body(status: StatusCode, mime: &'static str, body: String) -> Self {
        Self::router(axum::Router::new().route(
            "/mcp",
            post(move || {
                let body = body.clone();
                async move { (status, [("content-type", mime)], body) }
            }),
        ))
        .await
    }
    async fn send(
        &self,
        limit: usize,
    ) -> Result<StreamableHttpPostResponse, StreamableHttpError<reqwest::Error>> {
        let mut limits = StreamableHttpClientLimits::default();
        limits.max_response_body_size = limit;
        self.http
            .post_message_with_limits(
                self.url.clone(),
                request(),
                None,
                None,
                HashMap::new(),
                limits,
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
fn request() -> ClientJsonRpcMessage {
    serde_json::from_value(json!({"jsonrpc":"2.0", "id":1, "method":"tools/list", "params":{}}))
        .unwrap()
}
fn response() -> String {
    json!({"jsonrpc":"2.0", "id":1, "result":{"tools":[]}}).to_string()
}
fn assert_limit(
    result: Result<StreamableHttpPostResponse, StreamableHttpError<reqwest::Error>>,
    expected: usize,
) {
    assert!(
        matches!(result, Err(StreamableHttpError::ResponseBodyTooLarge { limit }) if limit == expected)
    );
}

#[tokio::test]
async fn known_length_boundary_and_zero_are_enforced_before_json_parsing() {
    let body = response();
    let fixture = Fixture::body(StatusCode::OK, "application/json", body.clone()).await;
    assert!(matches!(
        fixture.send(body.len()).await.unwrap(),
        StreamableHttpPostResponse::Json(_, _)
    ));
    assert_limit(fixture.send(body.len() - 1).await, body.len() - 1);
    assert_limit(fixture.send(0).await, 0);
    // An oversized rejection does not poison the underlying client.
    assert!(matches!(
        fixture.send(body.len()).await.unwrap(),
        StreamableHttpPostResponse::Json(_, _)
    ));
    fixture.close().await;
    let fixture = Fixture::body(StatusCode::OK, "application/json", "invalid".repeat(20)).await;
    assert_limit(fixture.send(1).await, 1);
    fixture.close().await;
}

#[tokio::test]
async fn chunked_body_stops_at_limit_without_waiting_for_end() {
    let fixture = Fixture::router(axum::Router::new().route(
        "/mcp",
        post(|| async {
            let chunks = stream::iter([
                Ok::<_, std::io::Error>(Bytes::from_static(b"1234")),
                Ok(Bytes::from_static(b"5678")),
            ])
            .chain(stream::pending());
            (
                [("content-type", "application/json")],
                Body::from_stream(chunks),
            )
                .into_response()
        }),
    ))
    .await;
    let result = tokio::time::timeout(Duration::from_secs(1), fixture.send(7))
        .await
        .unwrap();
    assert_limit(result, 7);
    fixture.close().await;
}

#[tokio::test]
async fn http_error_bodies_are_bounded_and_small_rpc_errors_are_preserved() {
    let body =
        json!({"jsonrpc":"2.0", "id":1, "error":{"code":-32602,"message":"bad input"}}).to_string();
    let fixture = Fixture::body(StatusCode::BAD_REQUEST, "application/json", body.clone()).await;
    assert!(matches!(
        fixture.send(body.len()).await.unwrap(),
        StreamableHttpPostResponse::Json(mcp_rs::model::JsonRpcMessage::Error(_), _)
    ));
    assert_limit(fixture.send(body.len() - 1).await, body.len() - 1);
    fixture.close().await;
    let fixture = Fixture::body(StatusCode::BAD_GATEWAY, "text/html", "x".repeat(1024)).await;
    assert_limit(fixture.send(32).await, 32);
    fixture.close().await;
}

#[tokio::test]
async fn successful_sse_uses_event_limit_instead_of_buffered_body_limit() {
    let body = format!("data: {}\n\n", response());
    let fixture = Fixture::body(StatusCode::OK, "text/event-stream", body).await;
    let result = fixture.send(0).await.unwrap();
    let StreamableHttpPostResponse::Sse(mut events, _) = result else {
        panic!("expected SSE")
    };
    assert!(events.next().await.unwrap().is_ok());
    drop(events);
    let mut limits = StreamableHttpClientLimits::default();
    limits.max_sse_event_size = 1;
    let result = fixture
        .http
        .post_message_with_limits(
            fixture.url.clone(),
            request(),
            None,
            None,
            HashMap::new(),
            limits,
        )
        .await
        .unwrap();
    let StreamableHttpPostResponse::Sse(mut events, _) = result else {
        panic!("expected SSE")
    };
    assert!(events.next().await.unwrap().is_err());
    drop(events);
    fixture.close().await;
}

#[tokio::test]
async fn configured_limit_covers_discovery_and_client_recovers_after_oversized_result() {
    let mut server = Server::new("response-limits", "1");
    server
        .tool("large")
        .handler(|_, _: Empty| async { Ok("x".repeat(8192)) })
        .unwrap();
    server
        .tool("small")
        .handler(|_, _: Empty| async { Ok("ok".to_owned()) })
        .unwrap();
    let stop = CancellationToken::new();
    let mut config = mcp_rs::transport::HttpServerConfig::default();
    config.json_response = true;
    let fixture = Fixture::router(mcp_rs::transport::http_router_with_config(
        server,
        stop.clone(),
        config,
    ))
    .await;
    let tiny = StreamableHttpClientTransportConfig::with_uri(fixture.url.clone())
        .max_response_body_size(1);
    assert!(Client::http(fixture.http.clone(), tiny).await.is_err());
    let config = StreamableHttpClientTransportConfig::with_uri(fixture.url.clone())
        .max_response_body_size(4096);
    let client = Client::http(fixture.http.clone(), config).await.unwrap();
    let error = client
        .call::<_, String>("large", &json!({}))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("4096"), "{error}");
    assert_eq!(
        client.call::<_, String>("small", &json!({})).await.unwrap(),
        "ok"
    );
    client.close().await.unwrap();
    stop.cancel();
    fixture.close().await;
}
