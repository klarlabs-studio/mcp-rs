//! The same modern extension workflow over every native transport family.
#![cfg(all(feature = "client-http", feature = "websocket", feature = "grpc"))]
#![allow(clippy::unwrap_used)]
use mcp_rs::{
    Client, LiveServer, Server, Session, SubscriptionHub,
    model::*,
    protocol::{
        ServiceExt,
        task_manager::{TaskManager, TaskOptions},
        transport::{
            StreamableHttpClientTransport,
            streamable_http_client::StreamableHttpClientTransportConfig,
        },
    },
    transport,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy)]
enum Kind {
    Framed,
    Http,
    WebSocket,
    Grpc,
}
#[derive(Deserialize, JsonSchema)]
struct Input {
    q: String,
}
fn app(hub: SubscriptionHub, tasks: TaskManager, calls: Arc<AtomicUsize>) -> Server {
    let mut server = Server::new("conformance", "1")
        .with_task_manager(tasks)
        .with_subscription_hub(hub)
        .with_result_cache(60_000, CacheScope::Private);
    for i in 0..103 {
        server
            .tool(format!("read_{i:03}"))
            .group("research")
            .tags(["read", "papers"])
            .defer_schema()
            .handler(|_, input: Input| async move { Ok(input.q) })
            .unwrap();
    }
    server
        .resource("data://counter")
        .handler(move |_, _| {
            let calls = calls.clone();
            async move { Ok(calls.fetch_add(1, Ordering::SeqCst).to_string()) }
        })
        .unwrap();
    server.prompt("rounds").protocol_handler(|ctx, _| async move {
        let session = Session::from_context(&ctx);
        let request = session.prompt_request().unwrap();
        if let Some(responses) = &request.input_responses {
            assert_eq!(request.request_state.as_deref(), Some("opaque"));
            assert_eq!(responses["answer"]["content"]["name"], "Ada");
            Ok(GetPromptResult::new(vec![PromptMessage::new_text(Role::User, "Ada")]).into())
        } else {
            let input: InputRequest = serde_json::from_value(json!({"method":"elicitation/create","params":{"mode":"form","message":"Name?","requestedSchema":{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}}})).unwrap();
            Ok(InputRequiredResult::new(Some(BTreeMap::from([("answer".into(), input)])), Some("opaque".into())).into())
        }
    }).unwrap();
    server
}
fn config() -> ClientConfig {
    ClientConfig::new(
        ClientCapabilities::builder()
            .enable_tasks()
            .enable_elicitation()
            .build(),
        Implementation::new("conformance-client", "1"),
    )
}
async fn connect(
    kind: Kind,
    server: LiveServer,
    stop: CancellationToken,
) -> (Client, JoinHandle<()>) {
    if matches!(kind, Kind::Framed) {
        let (client, io) = tokio::io::duplex(65536);
        let task = tokio::spawn(async move {
            let running = server.serve(io).await.unwrap();
            let cancel = running.cancellation_token();
            let waiting = running.waiting();
            tokio::pin!(waiting);
            tokio::select! { _ = stop.cancelled() => { cancel.cancel(); waiting.await.unwrap(); }, result = &mut waiting => { result.unwrap(); } }
        });
        return (
            Client::from_service(config().serve(client).await.unwrap()).unwrap(),
            task,
        );
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        match kind {
            Kind::Http => transport::serve_http(server, listener, stop).await.unwrap(),
            Kind::WebSocket => {
                transport::websocket::serve_websocket(server, listener, stop, Default::default())
                    .await
                    .unwrap()
            }
            Kind::Grpc => transport::grpc::serve_grpc(server, listener, stop)
                .await
                .unwrap(),
            Kind::Framed => unreachable!(),
        }
    });
    let running = match kind {
        Kind::Http => config()
            .serve(StreamableHttpClientTransport::with_client(
                reqwest::Client::new(),
                StreamableHttpClientTransportConfig::with_uri(format!("http://{address}/mcp")),
            ))
            .await
            .unwrap(),
        Kind::WebSocket => {
            let (socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}/mcp"))
                .await
                .unwrap();
            config()
                .serve(transport::websocket::WebSocketTransport::<
                    mcp_rs::protocol::RoleClient,
                    _,
                >::new(socket))
                .await
                .unwrap()
        }
        Kind::Grpc => {
            let channel = transport::grpc::tonic::transport::Endpoint::from_shared(format!(
                "http://{address}"
            ))
            .unwrap()
            .connect()
            .await
            .unwrap();
            config()
                .serve(transport::grpc::connect(
                    transport::grpc::proto::mcp_client::McpClient::new(channel),
                    Default::default(),
                ))
                .await
                .unwrap()
        }
        Kind::Framed => unreachable!(),
    };
    (Client::from_service(running).unwrap(), task)
}
async fn workflow(kind: Kind) {
    tokio::time::timeout(Duration::from_secs(15), async {
        let hub = SubscriptionHub::default();
        let tasks = TaskManager::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let live = LiveServer::new(app(hub.clone(), tasks.clone(), calls.clone()));
        let stop = CancellationToken::new();
        let _guard = stop.clone().drop_guard();
        let (client, runner) = connect(kind, live.clone(), stop.clone()).await;
        // Concurrent filtered pagination must preserve schemas and avoid cache cross-talk.
        let queries = (0..12).map(|i| {
            let client = &client;
            async move {
                let detail = if i % 2 == 0 { "names" } else { "full" };
                let query = serde_json::from_value(
                    json!({"group":"research","tags":["read","papers"],"detail":detail}),
                )
                .unwrap();
                let tools = client.peer().list_all_tools_with(query).await.unwrap();
                assert_eq!(tools.len(), 103);
                assert!(
                    tools
                        .iter()
                        .all(|tool| tool.input_schema.is_some() == (detail == "full"))
                );
                assert_eq!(tools.first().unwrap().name, "read_000");
                assert_eq!(tools.last().unwrap().name, "read_102");
            }
        });
        futures_util::future::join_all(queries).await;
        assert!(
            client
                .call::<_, String>("read_000", &json!({}))
                .await
                .is_err()
        );
        assert_eq!(
            client
                .call::<_, String>("read_000", &json!({"q":"paper"}))
                .await
                .unwrap(),
            "paper"
        );
        // Prompt MRTR is request-bound across independent transport messages.
        let GetPromptResponse::InputRequired(input) = client
            .peer()
            .get_prompt_once(GetPromptRequestParams::new("rounds"))
            .await
            .unwrap()
        else {
            panic!("expected MRTR input")
        };
        let GetPromptResponse::Complete(answer) = client
            .peer()
            .get_prompt_once(
                GetPromptRequestParams::new("rounds")
                    .with_request_state(input.request_state.unwrap())
                    .with_input_responses(BTreeMap::from([(
                        "answer".into(),
                        json!({"action":"accept","content":{"name":"Ada"}}),
                    )])),
            )
            .await
            .unwrap()
        else {
            panic!("expected MRTR completion")
        };
        assert_eq!(
            serde_json::to_value(answer).unwrap()["messages"][0]["content"]["text"],
            "Ada"
        );
        // Notifications invalidate cached reads before the host consumes the event.
        let mut resources = client
            .listen(
                SubscriptionFilter::builder()
                    .resource_subscription("data://counter")
                    .build(),
            )
            .await
            .unwrap();
        resources.next().await.unwrap().unwrap(); // initial refresh hint
        let read = || {
            client
                .peer()
                .read_resource(ReadResourceRequestParams::new("data://counter"))
        };
        let first = read().await.unwrap();
        assert_eq!(read().await.unwrap(), first);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            hub.notify_resource_updated("data://counter")
                .unwrap()
                .enqueued,
            1
        );
        resources.next().await.unwrap().unwrap();
        assert_ne!(read().await.unwrap(), first);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        // Task streams deliver a snapshot then a terminal result on the same scoped ID.
        let (tx, rx) = oneshot::channel();
        let task = tasks.spawn(TaskOptions::default(), |_| {
            Box::pin(async move {
                rx.await.unwrap();
                Ok(CallToolResult::success(vec![ContentBlock::text("done")]))
            })
        });
        let mut statuses = client
            .listen(SubscriptionFilter::builder().task_id(&task.task_id).build())
            .await
            .unwrap();
        let event = statuses.next().await.unwrap().unwrap();
        assert_eq!(
            event.get_meta().subscription_id().as_ref(),
            Some(statuses.id())
        );
        let ServerNotification::TaskStatusNotification(initial) = event else {
            panic!("expected task")
        };
        assert_eq!(initial.params.task.status(), TaskStatus::Working);
        tx.send(()).unwrap();
        let ServerNotification::TaskStatusNotification(completed) =
            statuses.next().await.unwrap().unwrap()
        else {
            panic!("expected completion")
        };
        assert_eq!(completed.params.task.status(), TaskStatus::Completed);
        assert_eq!(
            serde_json::to_value(completed.params.task).unwrap()["result"]["content"][0]["text"],
            "done"
        );
        statuses.cancel().await.unwrap();
        resources.cancel().await.unwrap();
        while hub.stats().active_streams != 0 {
            tokio::task::yield_now().await;
        }
        // Live replacement plus explicit cache invalidation exposes the current catalog.
        let mut replacement = app(hub.clone(), tasks.clone(), calls);
        replacement.remove_tool("read_000");
        live.replace(replacement);
        client.peer().clear_response_cache().await;
        let tools = client
            .peer()
            .list_all_tools_with(serde_json::from_value(json!({"group":"research"})).unwrap())
            .await
            .unwrap();
        assert_eq!(tools.len(), 102);
        assert!(!tools.iter().any(|tool| tool.name == "read_000"));
        let mut ending = client
            .listen(
                SubscriptionFilter::builder()
                    .resource_subscription("data://counter")
                    .build(),
            )
            .await
            .unwrap();
        ending.next().await.unwrap().unwrap();
        hub.shutdown().await;
        assert!(ending.next().await.unwrap().is_none());
        assert!(matches!(
            ending.end(),
            Some(mcp_rs::protocol::service::SubscriptionEnd::Graceful(_))
        ));
        client.close().await.unwrap();
        stop.cancel();
        runner.await.unwrap();
        tasks.shutdown();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn framed_modern_extensions() {
    workflow(Kind::Framed).await;
}
#[tokio::test]
async fn http_modern_extensions() {
    workflow(Kind::Http).await;
}
#[tokio::test]
async fn websocket_modern_extensions() {
    workflow(Kind::WebSocket).await;
}
#[tokio::test]
async fn grpc_modern_extensions() {
    workflow(Kind::Grpc).await;
}
