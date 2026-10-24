//! Framework-owned modern subscription state and lifecycle behavior.
#![cfg(feature = "client")]
#![allow(clippy::unwrap_used, clippy::expect_used)]
use mcp_rs::{
    Client, Server, SubscriptionConfig, SubscriptionError, SubscriptionHub,
    model::*,
    protocol::{
        ServerHandler, ServiceExt,
        service::{Subscription, SubscriptionEnd},
    },
};
use std::{num::NonZeroUsize, time::Duration};
use tokio::{task::JoinHandle, time::timeout};
fn application(hub: SubscriptionHub) -> Server {
    let mut server = Server::new("subscriptions", "1").with_subscription_hub(hub);
    for uri in ["data://a", "data://b"] {
        server
            .resource(uri)
            .handler(|_, _| async { Ok("snapshot".into()) })
            .unwrap();
    }
    server
        .resource("users://{id}")
        .handler(|_, _| async { Ok("user".into()) })
        .unwrap();
    server
}
async fn connect<H: ServerHandler>(server: H) -> (Client, JoinHandle<()>) {
    let (client, transport) = tokio::io::duplex(65536);
    let task = tokio::spawn(async move {
        let running = server.serve(transport).await.unwrap();
        running.waiting().await.unwrap();
    });
    (Client::connect(client).await.unwrap(), task)
}
async fn close(client: Client, task: JoinHandle<()>) {
    client.close().await.unwrap();
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
}
fn filter(uri: &str) -> SubscriptionFilter {
    SubscriptionFilter::builder()
        .resource_subscription(uri)
        .build()
}
async fn next(subscription: &mut Subscription) -> ServerNotification {
    timeout(Duration::from_secs(2), subscription.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}
async fn empty(hub: &SubscriptionHub) {
    timeout(Duration::from_secs(2), async {
        while hub.stats().active_streams != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
fn assert_resource(notification: ServerNotification, id: &RequestId, uri: &str) {
    assert_eq!(notification.get_meta().subscription_id().as_ref(), Some(id));
    let ServerNotification::ResourceUpdatedNotification(update) = notification else {
        panic!("expected resource update")
    };
    assert_eq!(update.params.uri, uri);
}
#[tokio::test]
async fn exact_templates_scoped_ids_and_client_isolation() {
    let hub = SubscriptionHub::default();
    let server = application(hub.clone());
    let (first, t1) = connect(server.clone()).await;
    let (second, t2) = connect(server).await;
    let mut a = first.listen(filter("users://Felix")).await.unwrap();
    let mut b = second.listen(filter("data://b")).await.unwrap();
    assert_eq!(a.id(), b.id());
    assert_resource(next(&mut a).await, a.id(), "users://Felix");
    assert_resource(next(&mut b).await, b.id(), "data://b");
    let report = hub.notify_resource_updated("users://Felix").unwrap();
    assert_eq!(report.matched, 1);
    assert_eq!(report.enqueued, 1);
    assert_resource(next(&mut a).await, a.id(), "users://Felix");
    assert!(timeout(Duration::from_millis(30), b.next()).await.is_err());
    a.cancel().await.unwrap();
    b.cancel().await.unwrap();
    empty(&hub).await;
    assert_eq!(hub.stats().resource_filters, 0);
    close(first, t1).await;
    close(second, t2).await;
}
#[tokio::test]
async fn accepted_filters_drop_unknown_uris_and_preserve_list_categories() {
    let hub = SubscriptionHub::default();
    let (client, task) = connect(application(hub.clone())).await;
    let requested = SubscriptionFilter::builder()
        .tools_list_changed()
        .resources_list_changed()
        .prompts_list_changed()
        .resource_subscription("data://a")
        .resource_subscription("missing://private")
        .resource_subscription("data://a")
        .build();
    let mut stream = client.listen(requested).await.unwrap();
    assert_eq!(
        stream
            .acknowledged()
            .resource_subscriptions
            .as_ref()
            .unwrap(),
        &vec!["data://a".to_owned(), "data://a".to_owned()]
    );
    // Initial hints are deduplicated even though the SDK preserves duplicate filters.
    for _ in 0..4 {
        let event = next(&mut stream).await;
        assert_eq!(
            event.get_meta().subscription_id().as_ref(),
            Some(stream.id())
        );
    }
    assert_eq!(hub.stats().resource_filters, 3);
    assert_eq!(
        hub.notify_resource_updated("missing://private")
            .unwrap()
            .matched,
        0
    );
    assert_eq!(hub.notify_tools_changed().unwrap().enqueued, 1);
    assert!(matches!(
        next(&mut stream).await,
        ServerNotification::ToolListChangedNotification(_)
    ));
    assert_eq!(hub.notify_resources_changed().unwrap().enqueued, 1);
    assert!(matches!(
        next(&mut stream).await,
        ServerNotification::ResourceListChangedNotification(_)
    ));
    assert_eq!(hub.notify_prompts_changed().unwrap().enqueued, 1);
    assert!(matches!(
        next(&mut stream).await,
        ServerNotification::PromptListChangedNotification(_)
    ));
    drop(stream);
    empty(&hub).await;
    close(client, task).await;
}
#[tokio::test]
async fn policy_rejection_and_oversized_filters_allocate_no_watchers() {
    let hub = SubscriptionHub::new(SubscriptionConfig {
        max_resources_per_stream: NonZeroUsize::new(1).unwrap(),
        ..Default::default()
    })
    .unwrap();
    let denied = application(hub.clone()).with_middleware(mcp_rs::middleware::authorize(|_| false));
    let (client, task) = connect(denied).await;
    let mut stream = client.listen(filter("data://a")).await.unwrap();
    assert!(
        timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .is_err()
    );
    assert_eq!(hub.stats().active_streams, 0);
    close(client, task).await;
    let (client, task) = connect(application(hub.clone())).await;
    let mut requested = filter("data://a");
    requested
        .resource_subscriptions
        .as_mut()
        .unwrap()
        .push("data://b".into());
    let mut stream = client.listen(requested).await.unwrap();
    assert!(stream.acknowledged().resource_subscriptions.is_none());
    assert!(
        timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .is_err()
    );
    assert_eq!(hub.stats().active_streams, 0);
    close(client, task).await;
}
#[tokio::test]
async fn global_capacity_is_released_when_client_disconnects() {
    let hub = SubscriptionHub::new(SubscriptionConfig {
        max_resource_filters: NonZeroUsize::new(1).unwrap(),
        ..Default::default()
    })
    .unwrap();
    let server = application(hub.clone());
    let (first, t1) = connect(server.clone()).await;
    let mut active = first.listen(filter("data://a")).await.unwrap();
    next(&mut active).await;
    let (second, t2) = connect(server).await;
    let mut refused = second.listen(filter("data://b")).await.unwrap();
    assert!(
        timeout(Duration::from_secs(2), refused.next())
            .await
            .unwrap()
            .is_err()
    );
    // An ungraceful EOF leaves the listen request in flight. The SDK deliberately
    // drains handlers for up to five seconds before dropping the service guard.
    // Keep a finite bound that includes that documented SDK drain window.
    first.close().await.unwrap();
    timeout(Duration::from_secs(6), t1).await.unwrap().unwrap();
    drop(active);
    empty(&hub).await;
    let mut accepted = second.listen(filter("data://b")).await.unwrap();
    assert_resource(next(&mut accepted).await, accepted.id(), "data://b");
    accepted.cancel().await.unwrap();
    empty(&hub).await;
    close(second, t2).await;
}
#[tokio::test]
async fn slow_streams_close_explicitly_and_publish_never_blocks() {
    let hub = SubscriptionHub::new(SubscriptionConfig {
        queue_capacity: NonZeroUsize::new(1).unwrap(),
        ..Default::default()
    })
    .unwrap();
    let (client, task) = connect(application(hub.clone())).await;
    let mut stream = client.listen(filter("data://a")).await.unwrap();
    next(&mut stream).await;
    assert_eq!(hub.notify_resource_updated("data://a").unwrap().enqueued, 1);
    let overflow = hub.notify_resource_updated("data://a").unwrap();
    assert_eq!(overflow.slow_streams, 1);
    assert_eq!(overflow.enqueued, 0);
    assert!(
        timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .is_err()
    );
    empty(&hub).await;
    close(client, task).await;
}
#[tokio::test]
async fn snapshot_replacement_and_handler_deadlines_do_not_end_active_streams() {
    let hub = SubscriptionHub::default();
    let live = application(hub.clone())
        .with_timeout(Duration::from_millis(10))
        .into_live();
    let (client, task) = connect(live.clone()).await;
    let mut stream = client.listen(filter("data://a")).await.unwrap();
    next(&mut stream).await;
    let mut changed = live.snapshot();
    changed
        .resource("data://new")
        .handler(|_, _| async { Ok("new".into()) })
        .unwrap();
    live.replace(changed);
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(hub.notify_resource_updated("data://a").unwrap().enqueued, 1);
    next(&mut stream).await;
    timeout(Duration::from_secs(2), hub.shutdown())
        .await
        .unwrap();
    assert!(
        timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .is_none()
    );
    assert!(matches!(stream.end(), Some(SubscriptionEnd::Graceful(_))));
    assert_eq!(hub.notify_tools_changed(), Err(SubscriptionError::Closed));
    close(client, task).await;
}
#[tokio::test]
async fn subscriptions_are_opt_in_and_configurations_validate_before_allocation() {
    assert!(
        SubscriptionHub::new(SubscriptionConfig {
            send_timeout: Duration::ZERO,
            ..Default::default()
        })
        .is_err()
    );
    assert!(
        SubscriptionHub::new(SubscriptionConfig {
            queue_capacity: NonZeroUsize::new(usize::MAX).unwrap(),
            ..Default::default()
        })
        .is_err()
    );
    let hub = SubscriptionHub::default();
    assert_eq!(
        hub.notify_resource_updated(""),
        Err(SubscriptionError::InvalidUri)
    );
    let (client, task) = connect(Server::new("disabled", "1")).await;
    assert!(client.listen(filter("data://a")).await.is_err());
    close(client, task).await;
}

#[cfg(feature = "client-http")]
#[tokio::test]
async fn streamable_http_routes_updates_and_disconnect_cleanup() {
    use mcp_rs::protocol::transport::streamable_http_client::StreamableHttpClientTransportConfig;
    use tokio_util::sync::CancellationToken;
    let hub = SubscriptionHub::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let stop = CancellationToken::new();
    let token = stop.clone();
    let server = application(hub.clone());
    let task = tokio::spawn(async move {
        mcp_rs::transport::serve_http(server, listener, token)
            .await
            .unwrap();
    });
    let client = Client::http(
        reqwest::Client::new(),
        StreamableHttpClientTransportConfig::with_uri(format!("http://{addr}/mcp")),
    )
    .await
    .unwrap();
    let mut stream = client.listen(filter("data://a")).await.unwrap();
    assert_resource(next(&mut stream).await, stream.id(), "data://a");
    hub.notify_resource_updated("data://a").unwrap();
    assert_resource(next(&mut stream).await, stream.id(), "data://a");
    stream.cancel().await.unwrap();
    empty(&hub).await;
    client.close().await.unwrap();
    stop.cancel();
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn typed_client_buffers_large_initial_refresh_sets() {
    let hub = SubscriptionHub::default();
    let (client, task) = connect(application(hub.clone())).await;
    let uris = (0..128)
        .map(|id| format!("users://{id}"))
        .collect::<Vec<_>>();
    let mut stream = client
        .listen(
            SubscriptionFilter::builder()
                .resource_subscriptions(uris.clone())
                .build(),
        )
        .await
        .unwrap();
    let mut received = std::collections::BTreeSet::new();
    for _ in &uris {
        let ServerNotification::ResourceUpdatedNotification(update) = next(&mut stream).await
        else {
            panic!("expected initial resource hint")
        };
        received.insert(update.params.uri);
    }
    assert_eq!(received, uris.into_iter().collect());
    drop(stream);
    empty(&hub).await;
    close(client, task).await;
}

#[tokio::test(start_paused = true)]
async fn establishment_deadline_ends_at_acknowledgment_and_zero_sends_nothing() {
    use mcp_rs::{ClientError, protocol::service::ServiceError};
    let hub = SubscriptionHub::default();
    let (client, task) = connect(application(hub.clone())).await;
    assert!(matches!(
        client.listen_with_timeout(filter("data://a"), Duration::ZERO).await,
        Err(ClientError::Service(error)) if matches!(*error, ServiceError::Timeout { timeout } if timeout.is_zero())
    ));
    assert_eq!(hub.stats().active_streams, 0);
    let mut stream = client
        .listen_with_timeout(filter("data://a"), Duration::from_secs(1))
        .await
        .unwrap();
    // Zero deadline did not consume a request ID: discovery was request zero.
    assert_eq!(stream.id(), &RequestId::Number(1));
    assert_resource(next(&mut stream).await, stream.id(), "data://a");
    tokio::time::advance(Duration::from_secs(30)).await;
    hub.notify_resource_updated("data://a").unwrap();
    assert_resource(next(&mut stream).await, stream.id(), "data://a");
    stream.cancel().await.unwrap();
    empty(&hub).await;
    close(client, task).await;
}

#[tokio::test(start_paused = true)]
async fn silent_acknowledgment_uses_default_or_custom_deadline_and_keeps_peer_usable() {
    use mcp_rs::{ClientError, protocol::service::ServiceError};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    for custom in [false, true] {
        let (client_io, server_io) = tokio::io::duplex(65536);
        let server = tokio::spawn(async move {
            let (reader, mut writer) = tokio::io::split(server_io);
            let mut lines = BufReader::new(reader).lines();
            let discovery: serde_json::Value =
                serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
            assert_eq!(discovery["method"], "server/discover");
            let result = DiscoverResult::new(
                vec![ProtocolVersion::V_2026_07_28],
                ServerCapabilities::default(),
            );
            let response =
                serde_json::json!({"jsonrpc":"2.0","id":discovery["id"],"result":result});
            writer
                .write_all(format!("{response}\n").as_bytes())
                .await
                .unwrap();
            let listen: serde_json::Value =
                serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
            assert_eq!(listen["method"], "subscriptions/listen");
            let cancel: serde_json::Value =
                serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
            assert_eq!(cancel["method"], "notifications/cancelled");
            assert_eq!(cancel["params"]["requestId"], listen["id"]);
            // A late acknowledgment must not revive or misroute the cancelled stream.
            let late = serde_json::json!({"jsonrpc":"2.0","method":"notifications/subscriptions/acknowledged","params":{"notifications":{},"_meta":{"io.modelcontextprotocol/subscriptionId":listen["id"]}}});
            writer
                .write_all(format!("{late}\n").as_bytes())
                .await
                .unwrap();
            let list: serde_json::Value =
                serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
            assert_eq!(list["method"], "tools/list");
            let response = serde_json::json!({"jsonrpc":"2.0","id":list["id"],"result":ListToolsResult::default()});
            writer
                .write_all(format!("{response}\n").as_bytes())
                .await
                .unwrap();
            assert!(lines.next_line().await.unwrap().is_none());
        });
        let client = Client::connect(client_io).await.unwrap();
        let duration = Duration::from_secs(if custom { 2 } else { 10 });
        let start = tokio::time::Instant::now();
        let result = if custom {
            client
                .listen_with_timeout(filter("data://a"), duration)
                .await
        } else {
            client.listen(filter("data://a")).await
        };
        assert!(
            matches!(result, Err(ClientError::Service(error)) if matches!(*error, ServiceError::Timeout { timeout } if timeout == duration))
        );
        assert_eq!(start.elapsed(), duration);
        assert!(
            client
                .peer()
                .list_tools(None)
                .await
                .unwrap()
                .tools
                .is_empty()
        );
        client.close().await.unwrap();
        server.await.unwrap();
    }
}
