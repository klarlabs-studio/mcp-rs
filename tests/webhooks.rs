//! Bounded out-of-band delivery, framing, deadlines, and fan-out behavior.
#![cfg(feature = "webhooks")]
#![allow(clippy::unwrap_used)]
use mcp_rs::transport::webhook::*;
use reqwest::{Url, header::HeaderValue};
use serde_json::{Value, json};
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Duration,
};

#[test]
fn validates_configuration_and_redacts_secrets() {
    let client = reqwest::Client::new();
    for url in [
        "ftp://example.com/",
        "https://user:secret@example.com/",
        "https://example.com/#fragment",
    ] {
        let config = WebhookConfig::new(Url::parse(url).unwrap());
        assert!(matches!(
            WebhookNotifier::with_client(client.clone(), config),
            Err(WebhookError::InvalidConfig(_))
        ));
    }
    let mut config = WebhookConfig::new(Url::parse("https://example.com/?secret=private").unwrap());
    config.headers.insert(
        "authorization",
        HeaderValue::from_static("Bearer private-token"),
    );
    let notifier = WebhookNotifier::with_client(client.clone(), config.clone()).unwrap();
    for debug in [format!("{config:?}"), format!("{notifier:?}")] {
        assert!(!debug.contains("private"));
        assert!(!debug.contains("example.com"));
    }
    config.timeout = Duration::ZERO;
    assert!(matches!(
        WebhookNotifier::with_client(client.clone(), config),
        Err(WebhookError::InvalidConfig(_))
    ));
    for name in [
        "content-type",
        "Mcp-Method",
        "Content-Length",
        "Transfer-Encoding",
        "Host",
    ] {
        let mut config = WebhookConfig::new(Url::parse("http://localhost/").unwrap());
        config.headers.insert(
            reqwest::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
            HeaderValue::from_static("override"),
        );
        assert!(matches!(
            WebhookNotifier::with_client(client.clone(), config),
            Err(WebhookError::InvalidConfig(_))
        ));
    }
}

struct Spy {
    index: usize,
    status: Option<u16>,
    seen: Arc<Mutex<Vec<usize>>>,
}
impl NotificationSender for Spy {
    fn send_notification<'a>(
        &'a self,
        method: &'a str,
        params: &'a Value,
    ) -> futures_util::future::BoxFuture<'a, Result<(), WebhookError>> {
        Box::pin(async move {
            assert_eq!(method, "notifications/test");
            assert_eq!(params, &json!({"hello":"world"}));
            self.seen.lock().unwrap().push(self.index);
            match self.status {
                Some(code) => Err(WebhookError::Status(code)),
                None => Ok(()),
            }
        })
    }
}
#[tokio::test]
async fn fanout_attempts_every_target_in_order_and_preserves_typed_failures() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let senders: Vec<Arc<dyn NotificationSender>> = [Some(503), None, Some(401)]
        .into_iter()
        .enumerate()
        .map(|(index, status)| {
            Arc::new(Spy {
                index,
                status,
                seen: seen.clone(),
            }) as Arc<dyn NotificationSender>
        })
        .collect();
    let multi = MultiNotifier::new(senders);
    let error = multi
        .send_notification("notifications/test", &json!({"hello":"world"}))
        .await
        .unwrap_err();
    assert_eq!(*seen.lock().unwrap(), vec![0, 1, 2]);
    let WebhookError::Fanout(failures) = error else {
        panic!("expected fanout failures")
    };
    assert_eq!(failures.len(), 2);
    assert_eq!((failures[0].index, failures[1].index), (0, 2));
    assert!(matches!(failures[0].error, WebhookError::Status(503)));
    assert!(matches!(failures[1].error, WebhookError::Status(401)));
    MultiNotifier::default()
        .send_notification("notifications/test", &Value::Null)
        .await
        .unwrap();
}

#[tokio::test]
async fn dropping_fanout_future_cancels_active_target_and_skips_later_ones() {
    use std::sync::atomic::{AtomicBool, Ordering};
    struct Pending {
        dropped: Arc<AtomicBool>,
    }
    struct Guard(Arc<AtomicBool>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    impl NotificationSender for Pending {
        fn send_notification<'a>(
            &'a self,
            _: &'a str,
            _: &'a Value,
        ) -> futures_util::future::BoxFuture<'a, Result<(), WebhookError>> {
            Box::pin(async move {
                let _guard = Guard(self.dropped.clone());
                futures_util::future::pending().await
            })
        }
    }
    let dropped = Arc::new(AtomicBool::new(false));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let multi = MultiNotifier::new(vec![
        Arc::new(Pending {
            dropped: dropped.clone(),
        }) as Arc<dyn NotificationSender>,
        Arc::new(Spy {
            index: 1,
            status: None,
            seen: seen.clone(),
        }),
    ]);
    let params = Value::Null;
    let mut future = Box::pin(multi.send_notification("notifications/test", &params));
    assert!(matches!(
        futures_util::poll!(&mut future),
        std::task::Poll::Pending
    ));
    drop(future);
    assert!(dropped.load(Ordering::SeqCst));
    assert!(seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn encoding_limits_and_invalid_methods_fail_before_network_access() {
    use serde::{Serialize, Serializer};
    struct Fails;
    impl Serialize for Fails {
        fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("intentional"))
        }
    }
    let url = Url::parse("http://127.0.0.1:1/?private=token").unwrap();
    let notifier = WebhookNotifier::new(url.clone()).unwrap();
    assert!(matches!(
        notifier
            .send_notification("notifications/test", &Fails)
            .await,
        Err(WebhookError::Serialize(_))
    ));
    for method in ["", "notifications/test\r\ninjected:true"] {
        assert!(matches!(
            notifier.send_notification(method, &Value::Null).await,
            Err(WebhookError::InvalidMethod)
        ));
    }
    let mut config = WebhookConfig::new(url);
    config.max_notification_bytes = NonZeroUsize::new(50).unwrap();
    let notifier = WebhookNotifier::from_config(config).unwrap();
    assert!(matches!(
        notifier
            .send_notification("notifications/test", &"x".repeat(100))
            .await,
        Err(WebhookError::TooLarge { limit: 50 })
    ));
}

#[cfg(feature = "http")]
mod delivery {
    use super::*;
    use axum::{
        Router,
        body::{Body, Bytes},
        extract::State,
        http::{Method, StatusCode, Uri},
        response::{IntoResponse, Response},
    };
    use reqwest::header::HeaderMap;
    use tokio_util::sync::CancellationToken;
    #[derive(Clone)]
    struct Captured {
        method: Method,
        headers: HeaderMap,
        body: Value,
    }
    struct Receiver {
        endpoint: Url,
        seen: Arc<Mutex<Vec<Captured>>>,
        stop: CancellationToken,
        task: tokio::task::JoinHandle<()>,
    }
    impl Receiver {
        async fn start() -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint =
                Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
            let seen = Arc::new(Mutex::new(Vec::new()));
            let router = Router::new().fallback(receive).with_state(seen.clone());
            let stop = CancellationToken::new();
            let token = stop.clone();
            let task = tokio::spawn(async move {
                axum::serve(listener, router)
                    .with_graceful_shutdown(token.cancelled_owned())
                    .await
                    .unwrap();
            });
            Self {
                endpoint,
                seen,
                stop,
                task,
            }
        }
        fn url(&self, path: &str) -> Url {
            self.endpoint.join(path).unwrap()
        }
        async fn close(self) {
            self.stop.cancel();
            tokio::time::timeout(Duration::from_secs(3), self.task)
                .await
                .unwrap()
                .unwrap();
        }
    }
    async fn receive(
        State(seen): State<Arc<Mutex<Vec<Captured>>>>,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        body: Bytes,
    ) -> Response {
        seen.lock().unwrap().push(Captured {
            method,
            headers,
            body: serde_json::from_slice(&body).unwrap(),
        });
        match uri.path() {
            "/redirect" => {
                (StatusCode::TEMPORARY_REDIRECT, [("location", "/capture")]).into_response()
            }
            "/delay" => {
                tokio::time::sleep(Duration::from_millis(300)).await;
                StatusCode::NO_CONTENT.into_response()
            }
            "/body" => {
                let stream = futures_util::stream::once(async {
                    Ok::<_, std::io::Error>(Bytes::from_static(b"ignored"))
                })
                .chain(futures_util::stream::pending());
                use futures_util::StreamExt;
                Response::new(Body::from_stream(stream))
            }
            path if path.starts_with("/status/") => {
                StatusCode::from_u16(path[8..].parse().unwrap())
                    .unwrap()
                    .into_response()
            }
            _ => StatusCode::ACCEPTED.into_response(),
        }
    }
    #[tokio::test]
    async fn channel_and_generic_notifications_match_go_shape_and_headers() {
        let receiver = Receiver::start().await;
        let mut config = WebhookConfig::new(receiver.url("capture"));
        config
            .headers
            .insert("x-hook", HeaderValue::from_static("1"));
        config.headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer test-token"),
        );
        let notifier = WebhookNotifier::from_config(config).unwrap();
        notifier
            .send_channel_message("status", &json!({"type":"text","text":"hi"}))
            .await
            .unwrap();
        notifier
            .clone()
            .send_notification(
                "notifications/resources/updated",
                &json!({"uri":"data://item"}),
            )
            .await
            .unwrap();
        notifier
            .send_notification("notifications/test", &Value::Null)
            .await
            .unwrap();
        {
            let seen = receiver.seen.lock().unwrap();
            assert_eq!(seen.len(), 3);
            assert_eq!(seen[0].method, Method::POST);
            assert_eq!(seen[0].headers["content-type"], "application/json");
            assert_eq!(
                seen[0].headers["mcp-method"],
                "notifications/channel/message"
            );
            assert_eq!(seen[0].headers["x-hook"], "1");
            assert_eq!(seen[0].headers["authorization"], "Bearer test-token");
            assert_eq!(
                seen[0].body,
                json!({"jsonrpc":"2.0","method":"notifications/channel/message","params":{"channel":"status","message":{"type":"text","text":"hi"}}})
            );
            assert_eq!(
                seen[1].body,
                json!({"jsonrpc":"2.0","method":"notifications/resources/updated","params":{"uri":"data://item"}})
            );
            assert_eq!(seen[2].body["params"], Value::Null);
            assert!(seen.iter().all(|request| request.body.get("id").is_none()));
        }
        drop(notifier);
        receiver.close().await;
    }
    #[tokio::test]
    async fn accepts_all_2xx_rejects_other_statuses_and_does_not_follow_redirects_or_retry() {
        let receiver = Receiver::start().await;
        for status in [200, 201, 202, 204, 206, 299, 300, 400, 401, 429, 500, 503] {
            let notifier = WebhookNotifier::new(receiver.url(&format!("status/{status}"))).unwrap();
            let result = notifier
                .send_notification("notifications/test", &Value::Null)
                .await;
            if status < 300 {
                result.unwrap()
            } else {
                assert!(matches!(result,Err(WebhookError::Status(code)) if code==status));
            }
        }
        let notifier = WebhookNotifier::new(receiver.url("redirect")).unwrap();
        assert!(matches!(
            notifier
                .send_notification("notifications/test", &Value::Null)
                .await,
            Err(WebhookError::Status(307))
        ));
        assert_eq!(receiver.seen.lock().unwrap().len(), 13);
        drop(notifier);
        receiver.close().await;
    }
    #[tokio::test]
    async fn deadline_and_exact_encoded_size_limit_are_enforced() {
        let receiver = Receiver::start().await;
        let params = json!({"text":"hello"});
        let expected = serde_json::to_vec(
            &json!({"jsonrpc":"2.0","method":"notifications/test","params":params}),
        )
        .unwrap()
        .len();
        let mut config = WebhookConfig::new(receiver.url("capture"));
        config.max_notification_bytes = NonZeroUsize::new(expected).unwrap();
        WebhookNotifier::from_config(config.clone())
            .unwrap()
            .send_notification("notifications/test", &params)
            .await
            .unwrap();
        config.max_notification_bytes = NonZeroUsize::new(expected - 1).unwrap();
        assert!(matches!(
            WebhookNotifier::from_config(config)
                .unwrap()
                .send_notification("notifications/test", &params)
                .await,
            Err(WebhookError::TooLarge { .. })
        ));
        assert_eq!(receiver.seen.lock().unwrap().len(), 1);
        let mut config = WebhookConfig::new(receiver.url("delay"));
        config.timeout = Duration::from_millis(50);
        let notifier = WebhookNotifier::from_config(config).unwrap();
        assert!(matches!(
            tokio::time::timeout(
                Duration::from_secs(2),
                notifier.send_notification("notifications/test", &Value::Null)
            )
            .await
            .unwrap(),
            Err(WebhookError::Timeout)
        ));
        assert_eq!(receiver.seen.lock().unwrap().len(), 2);
        drop(notifier);
        receiver.close().await;
    }
    #[tokio::test]
    async fn connection_errors_redact_endpoint_query_data() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = Url::parse(&format!(
            "http://{}/?private=secret-token",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        drop(listener);
        let notifier = WebhookNotifier::new(endpoint).unwrap();
        let error = notifier
            .send_notification("notifications/test", &Value::Null)
            .await
            .unwrap_err();
        assert!(matches!(error, WebhookError::Http(_)));
        assert!(!format!("{error:?}").contains("secret-token"));
        assert!(!error.to_string().contains("private"));
    }
    #[tokio::test]
    async fn response_bodies_are_not_buffered_or_drained() {
        let receiver = Receiver::start().await;
        let notifier = WebhookNotifier::new(receiver.url("body")).unwrap();
        tokio::time::timeout(
            Duration::from_secs(2),
            notifier.send_notification("notifications/test", &Value::Null),
        )
        .await
        .unwrap()
        .unwrap();
        drop(notifier);
        receiver.close().await;
    }
}
