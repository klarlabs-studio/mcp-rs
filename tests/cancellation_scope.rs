//! Handler-owned cancellation scopes release cooperative helper work.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use mcp_rs::{
    Server, Session, ToolError,
    model::*,
    protocol::{
        ClientLifecycleMode, ClientServiceExt, ServerHandler, ServiceExt,
        task_manager::{TaskManager, TaskOptions},
    },
};
use schemars::JsonSchema;
use serde::Deserialize;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Deserialize, JsonSchema)]
struct Input {}

async fn connect<H: ServerHandler>(
    server: H,
) -> (
    mcp_rs::protocol::service::RunningService<mcp_rs::protocol::RoleClient, ClientConfig>,
    tokio::task::JoinHandle<()>,
) {
    let (client_io, server_io) = tokio::io::duplex(65536);
    let running = tokio::spawn(async move {
        server
            .serve(server_io)
            .await
            .unwrap()
            .waiting()
            .await
            .unwrap();
    });
    let client = ClientConfig::new(
        ClientCapabilities::builder().enable_tasks().build(),
        Implementation::new("scope-test", "1"),
    )
    .serve_with_lifecycle(
        client_io,
        ClientLifecycleMode::Discover {
            preferred_versions: vec![ProtocolVersion::V_2026_07_28],
        },
    )
    .await
    .unwrap();
    (client, running)
}

fn call(name: &str) -> CallToolRequestParams {
    CallToolRequestParams::new(name.to_owned()).with_arguments(Default::default())
}

async fn stopped(session: Session) {
    tokio::time::timeout(Duration::from_secs(1), session.cancelled())
        .await
        .expect("handler exit must notify cooperative helper work");
}

async fn request_scopes(live: bool) {
    let (sender, mut sessions) = mpsc::unbounded_channel();
    let mut server = Server::new("request-scopes", "1").with_timeout(Duration::from_millis(50));
    for name in ["success", "error", "panic", "timeout", "cancel"] {
        let sender = sender.clone();
        server
            .tool(name)
            .handler(move |ctx, _: Input| {
                sender.send(Session::from_context(&ctx)).unwrap();
                async move {
                    match name {
                        "success" => Ok("done"),
                        "error" => Err(ToolError::new("expected failure")),
                        "panic" => panic!("expected panic"),
                        "cancel" => {
                            ctx.ct.cancel();
                            std::future::pending().await
                        }
                        _ => std::future::pending().await,
                    }
                }
            })
            .unwrap();
    }
    let (client, running) = if live {
        connect(server.into_live()).await
    } else {
        connect(server).await
    };
    for name in ["success", "error", "panic", "timeout", "cancel"] {
        let result = client.call_tool(call(name)).await;
        match name {
            "success" => assert_ne!(result.unwrap().is_error, Some(true)),
            "error" => assert_eq!(result.unwrap().is_error, Some(true)),
            _ => assert!(result.is_err()),
        }
        stopped(sessions.recv().await.unwrap()).await;
        // Cancelling a handler's request scope must not cancel the SDK connection.
        assert!(!client.list_tools(None).await.unwrap().tools.is_empty());
    }
    client.cancel().await.unwrap();
    running.await.unwrap();
}

#[tokio::test]
async fn static_request_exit_cancels_helper_scope_without_cancelling_peer() {
    request_scopes(false).await;
}

#[tokio::test]
async fn live_request_exit_cancels_helper_scope_without_cancelling_peer() {
    request_scopes(true).await;
}

async fn task_scopes(live: bool) {
    let tasks = TaskManager::new();
    let mut server = Server::new("task-scopes", "1").with_task_manager(tasks.clone());
    let (sender, mut sessions) = mpsc::unbounded_channel();
    for name in [
        "success",
        "error",
        "panic",
        "panic_sync",
        "invalid",
        "cancel",
        "expiry",
        "shutdown",
    ] {
        let sender = sender.clone();
        let options =
            TaskOptions::default().with_ttl_ms(if name == "expiry" { Some(500) } else { None });
        server
            .tool(name)
            .output_schema::<std::collections::BTreeMap<String, bool>>()
            .unwrap()
            .task_handler(options, move |ctx, _, _: Input| {
                sender
                    .send((Session::from_context(&ctx), ctx.ct.clone()))
                    .unwrap();
                assert_ne!(name, "panic_sync", "expected synchronous task panic");
                async move {
                    match name {
                        "success" => Ok(CallToolResult::structured(
                            serde_json::json!({"value":true}),
                        )),
                        "invalid" => Ok(CallToolResult::structured(
                            serde_json::json!({"value":"wrong type"}),
                        )),
                        "error" => Err(ErrorData::internal_error("expected failure", None).into()),
                        "panic" => panic!("expected task panic"),
                        _ => std::future::pending().await,
                    }
                }
            })
            .unwrap();
    }
    let (client, running) = if live {
        connect(server.into_live()).await
    } else {
        connect(server).await
    };
    // Keep one task alive while other request/task scopes end.
    let CallToolResponse::Task(background) = client.call_tool_once(call("shutdown")).await.unwrap()
    else {
        panic!("expected task");
    };
    let (background_session, background_token) = sessions.recv().await.unwrap();
    assert!(!background_token.is_cancelled());
    assert_eq!(
        client
            .get_task(GetTaskParams::new(&background.task.task_id))
            .await
            .unwrap()
            .task
            .status(),
        TaskStatus::Working
    );
    for name in [
        "success",
        "error",
        "panic",
        "panic_sync",
        "invalid",
        "cancel",
        "expiry",
    ] {
        let CallToolResponse::Task(created) = client.call_tool_once(call(name)).await.unwrap()
        else {
            panic!("expected task");
        };
        let (session, token): (Session, CancellationToken) = sessions.recv().await.unwrap();
        if matches!(name, "cancel" | "expiry") {
            // Returning a task handle must not end the task's independent scope.
            assert!(!token.is_cancelled());
            if name == "cancel" {
                client
                    .cancel_task(CancelTaskParams::new(&created.task.task_id))
                    .await
                    .unwrap();
            } else {
                tokio::time::sleep(Duration::from_millis(550)).await;
                let failed = client
                    .get_task(GetTaskParams::new(&created.task.task_id))
                    .await
                    .unwrap();
                assert_eq!(failed.task.status(), TaskStatus::Failed);
            }
        }
        stopped(session).await;
        assert!(!background_token.is_cancelled());
    }
    tasks.shutdown();
    stopped(background_session).await;
    assert!(background_token.is_cancelled());
    assert_eq!(tasks.running_task_count(), 0);
    assert!(!client.list_tools(None).await.unwrap().tools.is_empty());
    client.cancel().await.unwrap();
    running.await.unwrap();
}

#[tokio::test]
async fn static_task_scope_outlives_creation_and_ends_with_task() {
    task_scopes(false).await;
}

#[tokio::test]
async fn live_task_scope_outlives_creation_and_ends_with_task() {
    task_scopes(true).await;
}
