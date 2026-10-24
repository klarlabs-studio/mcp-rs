//! Native task snapshots, ordered updates, admission, and bounded delivery.
#![cfg(feature = "client")]
#![allow(clippy::unwrap_used, clippy::expect_used)]
use mcp_rs::{
    Client, Server, SubscriptionConfig, SubscriptionHub,
    model::*,
    protocol::{
        ServerHandler, ServiceExt,
        service::{Subscription, SubscriptionContext, SubscriptionSendError},
        task_manager::{TaskContext, TaskExit, TaskManager, TaskOptions},
    },
};
use serde_json::json;
use std::{future::Future, num::NonZeroUsize, time::Duration};
use tokio::{sync::oneshot, time::timeout};

async fn bounded(future: impl Future<Output = ()>) {
    timeout(Duration::from_secs(10), future).await.unwrap();
}
fn capacity(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}
fn config() -> ClientConfig {
    ClientConfig::new(
        ClientCapabilities::builder().enable_tasks().build(),
        Implementation::new("task-subscriptions", "1"),
    )
}
async fn connect(server: impl ServerHandler) -> (Client, tokio::task::JoinHandle<()>) {
    let (client, transport) = tokio::io::duplex(65536);
    let task = tokio::spawn(async move {
        server
            .serve(transport)
            .await
            .unwrap()
            .waiting()
            .await
            .unwrap();
    });
    (
        Client::from_service(config().serve(client).await.unwrap()).unwrap(),
        task,
    )
}
async fn next(stream: &mut Subscription) -> DetailedTask {
    let event = timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        event.get_meta().subscription_id().as_ref(),
        Some(stream.id())
    );
    let ServerNotification::TaskStatusNotification(update) = event else {
        panic!("expected task status")
    };
    update.params.task
}
async fn empty(hub: &SubscriptionHub) {
    timeout(Duration::from_secs(2), async {
        while hub.stats().active_streams != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(hub.stats().task_filters, 0);
}
fn gated(tasks: &TaskManager) -> (Task, oneshot::Sender<()>) {
    let (tx, rx) = oneshot::channel();
    let task = tasks.spawn(TaskOptions::default(), move |_| {
        Box::pin(async move {
            rx.await.unwrap();
            Ok(CallToolResult::success(vec![ContentBlock::text("done")]))
        })
    });
    (task, tx)
}
#[test]
fn filter_wire_intersection_subset_and_capability_checks() {
    let requested = SubscriptionFilter::builder()
        .task_ids(["a", "b", "a"])
        .tools_list_changed()
        .build();
    assert_eq!(
        serde_json::to_value(&requested).unwrap()["taskIds"],
        json!(["a", "b", "a"])
    );
    let accepted = SubscriptionFilter::builder().task_id("a").build();
    assert!(accepted.is_subset_of(&requested));
    assert!(!requested.is_subset_of(&accepted));
    assert_eq!(
        requested.intersection(&accepted).task_ids.unwrap(),
        ["a", "a"]
    );
    assert!(
        requested
            .supported_by(&ServerCapabilities::default())
            .task_ids
            .is_none()
    );
    assert_eq!(
        requested
            .supported_by(&ServerCapabilities::builder().enable_tasks().build())
            .task_ids,
        requested.task_ids
    );
    assert!(
        serde_json::to_value(SubscriptionFilter::new())
            .unwrap()
            .get("taskIds")
            .is_none()
    );
    assert_eq!(
        serde_json::from_value::<SubscriptionFilter>(json!({"taskIds":["a"]})).unwrap(),
        accepted
    );
}
#[tokio::test]
async fn snapshots_precede_matching_updates_and_unrelated_tasks_do_not_fill_queue() {
    bounded(async {
        let tasks = TaskManager::new();
        let (a, release_a) = gated(&tasks);
        let (b, release_b) = gated(&tasks);
        let mut updates = tasks
            .subscribe(&[a.task_id.clone(), a.task_id.clone()], capacity(1))
            .unwrap();
        let (context_tx, context_rx) = oneshot::channel();
        tasks.spawn(TaskOptions::default(), |ctx| {
            context_tx.send(ctx).ok().unwrap();
            Box::pin(std::future::pending())
        });
        let unrelated = context_rx.await.unwrap();
        for i in 0..64 {
            unrelated.set_status_message(format!("unrelated {i}"));
        }
        release_a.send(()).unwrap();
        // Ensure completion has happened before reading the initial snapshot.
        while tasks.get_task(&a.task_id).unwrap().status() != TaskStatus::Completed {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            updates.next().await.unwrap().unwrap().status(),
            TaskStatus::Working
        );
        let completed = updates.next().await.unwrap().unwrap();
        assert_eq!(completed.task.task_id, a.task_id);
        assert_eq!(completed.status(), TaskStatus::Completed);
        assert!(
            timeout(Duration::from_millis(20), updates.next())
                .await
                .is_err()
        );
        release_b.send(()).unwrap();
        assert_ne!(a.task_id, b.task_id);
        drop(updates);
        tasks.shutdown();
    })
    .await;
}
#[tokio::test]
async fn overflow_is_explicit_unknown_ids_are_atomic_and_shutdown_ends_receiver() {
    bounded(async {
        let tasks = TaskManager::new();
        let (tx, rx) = oneshot::channel();
        let task = tasks.spawn(TaskOptions::default(), |ctx| {
            tx.send(ctx).ok().unwrap();
            Box::pin(std::future::pending())
        });
        let ctx = rx.await.unwrap();
        assert!(
            tasks
                .subscribe(&[task.task_id.clone(), "unknown".into()], capacity(1))
                .is_err()
        );
        let mut updates = tasks
            .subscribe(std::slice::from_ref(&task.task_id), capacity(1))
            .unwrap();
        assert_eq!(
            updates.next().await.unwrap().unwrap().status(),
            TaskStatus::Working
        );
        ctx.set_status_message("one");
        ctx.set_status_message("two");
        assert!(updates.next().await.is_err());
        drop(updates);
        let mut updates = tasks
            .subscribe(std::slice::from_ref(&task.task_id), capacity(1))
            .unwrap();
        assert_eq!(
            updates
                .next()
                .await
                .unwrap()
                .unwrap()
                .task
                .status_message
                .as_deref(),
            Some("two")
        );
        tasks.shutdown();
        assert!(updates.next().await.unwrap().is_none());
    })
    .await;
}
#[tokio::test]
async fn native_streams_route_complete_failed_cancelled_and_input_payloads() {
    bounded(async {
        let tasks = TaskManager::new();
        let hub = SubscriptionHub::default();
        let server = Server::new("native-tasks", "1").with_task_manager(tasks.clone()).with_subscription_hub(hub.clone());
        let (client, server_task) = connect(server).await;
        let (task, release) = gated(&tasks);
        let mut stream = client.listen(SubscriptionFilter::builder().task_id(&task.task_id).task_id(&task.task_id).build()).await.unwrap();
        assert_eq!(stream.acknowledged().task_ids.as_ref().unwrap().len(), 2);
        assert_eq!(next(&mut stream).await.status(), TaskStatus::Working);
        assert_eq!(hub.stats().task_filters, 2);
        release.send(()).unwrap();
        let completed = next(&mut stream).await;
        assert_eq!(completed.status(), TaskStatus::Completed);
        assert_eq!(serde_json::to_value(&completed).unwrap()["result"]["content"][0]["text"], "done");
        stream.cancel().await.unwrap();
        empty(&hub).await;
        for fail in [false, true] {
            let task = tasks.spawn(TaskOptions::default(), move |ctx| Box::pin(async move {
                ctx.cancelled().await;
                if fail { Err(TaskExit::Error(mcp_rs::protocol::ErrorData::internal_error("failed deliberately", None))) }
                else { Err(TaskExit::Cancelled) }
            }));
            let mut stream = client.listen(SubscriptionFilter::builder().task_id(&task.task_id).build()).await.unwrap();
            assert_eq!(next(&mut stream).await.status(), TaskStatus::Working);
            client.peer().cancel_task(CancelTaskParams::new(&task.task_id)).await.unwrap();
            let terminal = loop {
                let status = next(&mut stream).await;
                if status.status().is_terminal() { break status; }
            };
            assert_eq!(terminal.status(), if fail { TaskStatus::Failed } else { TaskStatus::Cancelled });
            if fail { assert_eq!(serde_json::to_value(terminal).unwrap()["error"]["message"], "failed deliberately"); }
            stream.cancel().await.unwrap();
            empty(&hub).await;
        }
        let (start, started) = oneshot::channel();
        let task = tasks.spawn(TaskOptions::default(), move |ctx: TaskContext| Box::pin(async move {
            started.await.unwrap();
            let request: InputRequest = serde_json::from_value(json!({"method":"elicitation/create","params":{"mode":"form","message":"Name?","requestedSchema":{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}}})).unwrap();
            ctx.request_input("name", request).await?;
            Ok(CallToolResult::success(vec![]))
        }));
        let mut stream = client.listen(SubscriptionFilter::builder().task_id(&task.task_id).build()).await.unwrap();
        assert_eq!(next(&mut stream).await.status(), TaskStatus::Working);
        start.send(()).unwrap();
        let input = next(&mut stream).await;
        assert_eq!(input.status(), TaskStatus::InputRequired);
        assert!(serde_json::to_value(input).unwrap()["inputRequests"]["name"].is_object());
        tasks.update_task(&task.task_id, [("name".to_owned(), json!({"action":"accept"}))]).unwrap();
        assert_eq!(next(&mut stream).await.status(), TaskStatus::Working);
        assert_eq!(next(&mut stream).await.status(), TaskStatus::Completed);
        stream.cancel().await.unwrap();
        empty(&hub).await;
        client.close().await.unwrap();
        server_task.await.unwrap();
        tasks.shutdown();
    }).await;
}
#[tokio::test]
async fn task_limits_middleware_and_unknown_ids_release_all_allocations() {
    bounded(async {
        let tasks = TaskManager::new();
        let (task, _release) = gated(&tasks);
        let hub = SubscriptionHub::new(SubscriptionConfig {
            max_tasks_per_stream: capacity(1),
            max_task_filters: capacity(1),
            max_task_id_bytes: capacity(40),
            ..Default::default()
        })
        .unwrap();
        let server = Server::new("task-limits", "1")
            .with_task_manager(tasks.clone())
            .with_subscription_hub(hub.clone());
        let (client, runner) = connect(server.clone()).await;
        for ids in [
            vec![task.task_id.clone(), task.task_id.clone()],
            vec!["".into()],
            vec!["x".repeat(41)],
            vec!["unknown".into()],
        ] {
            let mut stream = client
                .listen(SubscriptionFilter::builder().task_ids(ids).build())
                .await
                .unwrap();
            assert!(stream.next().await.is_err());
            empty(&hub).await;
        }
        let mut accepted = client
            .listen(SubscriptionFilter::builder().task_id(&task.task_id).build())
            .await
            .unwrap();
        next(&mut accepted).await;
        let mut excess = client
            .listen(SubscriptionFilter::builder().task_id(&task.task_id).build())
            .await
            .unwrap();
        assert!(excess.next().await.is_err());
        assert_eq!(hub.stats().task_filters, 1);
        accepted.cancel().await.unwrap();
        empty(&hub).await;
        client.close().await.unwrap();
        runner.await.unwrap();
        let denied = server.with_middleware(mcp_rs::middleware::authorize(|_| false));
        let (client, runner) = connect(denied).await;
        let mut stream = client
            .listen(SubscriptionFilter::builder().task_id(&task.task_id).build())
            .await
            .unwrap();
        assert!(stream.next().await.is_err());
        empty(&hub).await;
        client.close().await.unwrap();
        runner.await.unwrap();
        tasks.shutdown();
    })
    .await;
}
#[tokio::test]
async fn task_notifications_require_both_peers_to_advertise_tasks() {
    bounded(async {
        let tasks = TaskManager::new();
        let (task, _release) = gated(&tasks);
        let hub = SubscriptionHub::default();
        let server = Server::new("task-capabilities", "1")
            .with_task_manager(tasks.clone())
            .with_subscription_hub(hub.clone());
        let (client_io, transport) = tokio::io::duplex(65536);
        let runner = tokio::spawn(async move {
            server
                .serve(transport)
                .await
                .unwrap()
                .waiting()
                .await
                .unwrap();
        });
        let client = Client::connect(client_io).await.unwrap();
        let mut stream = client
            .listen(SubscriptionFilter::builder().task_id(&task.task_id).build())
            .await
            .unwrap();
        assert!(stream.acknowledged().task_ids.is_none());
        stream.cancel().await.unwrap();
        empty(&hub).await;
        client.close().await.unwrap();
        runner.await.unwrap();
        let (client, runner) =
            connect(Server::new("no-tasks", "1").with_subscription_hub(hub.clone())).await;
        let mut stream = client
            .listen(SubscriptionFilter::builder().task_id(&task.task_id).build())
            .await
            .unwrap();
        assert!(stream.acknowledged().task_ids.is_none());
        stream.cancel().await.unwrap();
        empty(&hub).await;
        client.close().await.unwrap();
        runner.await.unwrap();
        tasks.shutdown();
    })
    .await;
}
#[tokio::test]
async fn ttl_expiry_is_published_when_the_manager_sweeps() {
    bounded(async {
        let tasks = TaskManager::new();
        let task = tasks.spawn(TaskOptions::default().with_ttl_ms(20), |_| {
            Box::pin(std::future::pending())
        });
        let mut updates = tasks
            .subscribe(std::slice::from_ref(&task.task_id), capacity(2))
            .unwrap();
        assert_eq!(
            updates.next().await.unwrap().unwrap().status(),
            TaskStatus::Working
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(
            tasks.get_task(&task.task_id).unwrap().status(),
            TaskStatus::Failed
        );
        assert_eq!(
            updates.next().await.unwrap().unwrap().status(),
            TaskStatus::Failed
        );
        tasks.shutdown();
    })
    .await;
}

struct OutsideFilter {
    forged: bool,
}
impl ServerHandler for OutsideFilter {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tasks().build())
    }
    fn accepted_subscription_filter(
        &self,
        requested: &SubscriptionFilter,
    ) -> Option<SubscriptionFilter> {
        Some(requested.clone())
    }
    async fn listen(
        &self,
        context: SubscriptionContext,
    ) -> Result<(), mcp_rs::protocol::ErrorData> {
        let task = DetailedTask::new(
            Task::new("unrequested", TaskStatus::Working, "now", "now"),
            TaskPayload::Working,
        );
        let notification = ServerNotification::TaskStatusNotification(TaskStatusNotification::new(
            TaskStatusNotificationParams::new(task),
        ));
        assert!(matches!(
            context.sink().send(notification.clone()).await,
            Err(SubscriptionSendError::NotificationNotAccepted(
                "notifications/tasks"
            ))
        ));
        if self.forged {
            let mut notification = notification;
            notification
                .get_meta_mut()
                .set_subscription_id(context.request_context().id.clone());
            context
                .request_context()
                .peer
                .send_notification(notification)
                .await
                .unwrap();
            context.request_context().ct.cancelled().await;
        }
        Ok(())
    }
}
#[tokio::test]
async fn sink_and_client_reject_unrequested_task_ids() {
    bounded(async {
        for forged in [false, true] {
            let (client, runner) = connect(OutsideFilter { forged }).await;
            let mut stream = client
                .listen(SubscriptionFilter::builder().task_id("requested").build())
                .await
                .unwrap();
            let result = stream.next().await;
            if forged {
                assert!(result.is_err());
            } else {
                assert!(result.unwrap().is_none());
            }
            client.close().await.unwrap();
            runner.await.unwrap();
        }
    })
    .await;
}

#[tokio::test]
async fn large_snapshot_sets_fit_the_typed_client_and_clients_are_isolated() {
    bounded(async {
        let tasks = TaskManager::new();
        let hub = SubscriptionHub::default();
        let server = Server::new("snapshot-set", "1")
            .with_task_manager(tasks.clone())
            .with_subscription_hub(hub.clone());
        let mut ids = Vec::new();
        let mut releases = Vec::new();
        for _ in 0..128 {
            let (task, release) = gated(&tasks);
            ids.push(task.task_id);
            releases.push(release);
        }
        let (client, runner) = connect(server.clone()).await;
        let (other, other_runner) = connect(server).await;
        let mut stream = client
            .listen(SubscriptionFilter::builder().task_ids(ids.clone()).build())
            .await
            .unwrap();
        let mut isolated = other
            .listen(SubscriptionFilter::builder().task_id(&ids[127]).build())
            .await
            .unwrap();
        assert_eq!(next(&mut isolated).await.task.task_id, ids[127]);
        // Let the server enqueue all initial snapshots before draining the client.
        tokio::time::sleep(Duration::from_millis(20)).await;
        for id in &ids {
            assert_eq!(next(&mut stream).await.task.task_id, *id);
        }
        releases.remove(0).send(()).unwrap();
        assert_eq!(next(&mut stream).await.task.task_id, ids[0]);
        assert!(
            timeout(Duration::from_millis(20), isolated.next())
                .await
                .is_err()
        );
        stream.cancel().await.unwrap();
        isolated.cancel().await.unwrap();
        empty(&hub).await;
        client.close().await.unwrap();
        other.close().await.unwrap();
        runner.await.unwrap();
        other_runner.await.unwrap();
        tasks.shutdown();
    })
    .await;
}

#[cfg(feature = "client-http")]
#[tokio::test]
async fn http_task_snapshots_and_completion_use_scoped_streams() {
    bounded(async {
        use mcp_rs::protocol::transport::{
            StreamableHttpClientTransport,
            streamable_http_client::StreamableHttpClientTransportConfig,
        };
        use tokio_util::sync::CancellationToken;
        let tasks = TaskManager::new();
        let hub = SubscriptionHub::default();
        let server = Server::new("http-tasks", "1")
            .with_task_manager(tasks.clone())
            .with_subscription_hub(hub.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let stop = CancellationToken::new();
        let token = stop.clone();
        let runner = tokio::spawn(async move {
            mcp_rs::transport::serve_http(server, listener, token)
                .await
                .unwrap();
        });
        let transport = StreamableHttpClientTransport::with_client(
            reqwest::Client::new(),
            StreamableHttpClientTransportConfig::with_uri(format!("http://{address}/mcp")),
        );
        let client = Client::from_service(config().serve(transport).await.unwrap()).unwrap();
        let (task, release) = gated(&tasks);
        let mut stream = client
            .listen(SubscriptionFilter::builder().task_id(&task.task_id).build())
            .await
            .unwrap();
        assert_eq!(next(&mut stream).await.status(), TaskStatus::Working);
        release.send(()).unwrap();
        assert_eq!(next(&mut stream).await.status(), TaskStatus::Completed);
        stream.cancel().await.unwrap();
        empty(&hub).await;
        client.close().await.unwrap();
        stop.cancel();
        runner.await.unwrap();
        tasks.shutdown();
    })
    .await;
}
