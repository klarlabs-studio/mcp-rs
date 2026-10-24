//! Request-bound progress and dynamic registry notification integration.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use mcp_rs::protocol::ClientServiceExt as _;
use mcp_rs::{
    Server, Session,
    model::*,
    protocol::{ClientHandler, RoleClient, ServiceExt, service::NotificationContext},
};
use schemars::JsonSchema;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::Mutex;
#[derive(Deserialize, JsonSchema)]
struct Input {}
#[derive(Clone)]
struct Recorder {
    events: Arc<Mutex<Vec<ProgressNotificationParam>>>,
    lists: Arc<Mutex<Vec<&'static str>>>,
}
impl ClientHandler for Recorder {
    async fn on_progress(
        &self,
        params: ProgressNotificationParam,
        _: NotificationContext<RoleClient>,
    ) {
        self.events.lock().await.push(params);
    }
    async fn on_tool_list_changed(&self, _: NotificationContext<RoleClient>) {
        self.lists.lock().await.push("tools");
    }
    async fn on_resource_list_changed(&self, _: NotificationContext<RoleClient>) {
        self.lists.lock().await.push("resources");
    }
    async fn on_prompt_list_changed(&self, _: NotificationContext<RoleClient>) {
        self.lists.lock().await.push("prompts");
    }
}
fn recorder() -> Recorder {
    Recorder {
        events: Arc::new(Mutex::new(vec![])),
        lists: Arc::new(Mutex::new(vec![])),
    }
}
#[tokio::test]
async fn progress_keeps_numeric_tokens_and_monotonic_order() {
    let mut server = Server::new("progress", "1");
    server
        .tool("work")
        .handler(|ctx, _: Input| async move {
            let session = Session::from_context(&ctx);
            session
                .progress(1.0, Some(3.0), Some("first".into()))
                .await
                .map_err(|e| mcp_rs::ToolError::new(e.to_string()))?;
            session
                .clone()
                .progress(0.5, Some(3.0), None)
                .await
                .map_err(|e| mcp_rs::ToolError::new(e.to_string()))?;
            Ok(ctx.meta.get_progress_token())
        })
        .unwrap();
    let rec = recorder();
    let events = rec.events.clone();
    let (client_io, server_io) = tokio::io::duplex(65536);
    let task = tokio::spawn(async move {
        let service = server.serve(server_io).await.unwrap();
        service.waiting().await.unwrap();
    });
    let client = rec
        .serve_with_lifecycle(
            client_io,
            mcp_rs::protocol::ClientLifecycleMode::Discover {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
            },
        )
        .await
        .unwrap();
    let mut request = CallToolRequestParams::new("work").with_arguments(Default::default());
    request.set_progress_token(ProgressToken(NumberOrString::Number(7)));
    let result = client.call_tool(request).await.unwrap();
    let incoming: ProgressToken =
        serde_json::from_str(&result.content[0].as_text().unwrap().text).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while events.lock().await.len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let events = events.lock().await;
    assert_eq!(events[0].progress_token, incoming);
    assert!(matches!(incoming.0, NumberOrString::Number(_)));
    assert_eq!(events[0].progress, 1.0);
    assert!(events[1].progress > 1.0);
    drop(events);
    client.cancel().await.unwrap();
    task.await.unwrap();
}
#[tokio::test]
async fn live_registry_change_notifications_arrive_at_the_selected_peer() {
    let mut server = Server::new("notifications", "1");
    // Notification emission is bound to this request's actual connection.
    server
        .tool("notify")
        .handler(|ctx, _: Input| async move {
            let registry = Server::new("unused", "1").into_live();
            registry
                .notify_changed(&ctx.peer)
                .await
                .map_err(|e| mcp_rs::ToolError::new(e.to_string()))?;
            Ok("done")
        })
        .unwrap();
    let rec = recorder();
    let lists = rec.lists.clone();
    let (client_io, server_io) = tokio::io::duplex(65536);
    let live = server.into_live();
    let task = tokio::spawn(async move {
        let service = live.serve(server_io).await.unwrap();
        service.waiting().await.unwrap();
    });
    let client = rec
        .serve_with_lifecycle(
            client_io,
            mcp_rs::protocol::ClientLifecycleMode::Discover {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
            },
        )
        .await
        .unwrap();
    client
        .call_tool(CallToolRequestParams::new("notify").with_arguments(Default::default()))
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while lists.lock().await.len() < 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(*lists.lock().await, vec!["tools", "resources", "prompts"]);
    client.cancel().await.unwrap();
    task.await.unwrap();
}

#[cfg(feature = "elicitation")]
#[derive(Clone)]
struct Interactive;
#[cfg(feature = "elicitation")]
impl ClientHandler for Interactive {
    fn get_info(&self) -> ClientConfig {
        ClientConfig::new(
            {
                let capabilities = ClientCapabilities::builder();
                #[cfg(feature = "elicitation")]
                let capabilities = capabilities.enable_elicitation_with(
                    ElicitationCapability::new().with_form(FormElicitationCapability::default()),
                );
                capabilities.build()
            },
            Implementation::new("interactive", "1"),
        )
    }
    #[cfg(feature = "elicitation")]
    async fn create_elicitation(
        &self,
        _: ElicitRequestParams,
        _: mcp_rs::protocol::service::RequestContext<RoleClient>,
    ) -> Result<ElicitResult, ErrorData> {
        Ok(ElicitResult::new(ElicitationAction::Accept)
            .with_content(serde_json::json!({"name":"Felix"})))
    }
}
#[cfg(feature = "elicitation")]
#[derive(Deserialize, JsonSchema)]
struct Answer {
    name: String,
}
#[cfg(feature = "elicitation")]
impl mcp_rs::protocol::service::ElicitationSafe for Answer {}
#[cfg(feature = "elicitation")]
#[tokio::test]
async fn typed_elicitation_round_trips_through_the_client_handler() {
    let mut server = Server::new("elicitation", "1");
    server
        .tool("ask")
        .handler(|ctx, _: Input| async move {
            let answer = Session::from_context(&ctx)
                .elicit::<Answer>("Your name?")
                .await
                .map_err(|e| mcp_rs::ToolError::new(e.to_string()))?;
            Ok(answer.map(|answer| answer.name).unwrap_or_default())
        })
        .unwrap();
    let (client_io, server_io) = tokio::io::duplex(65536);
    let task = tokio::spawn(async move {
        let service = server.serve(server_io).await.unwrap();
        service.waiting().await.unwrap();
    });
    let client = Interactive
        .serve_with_lifecycle(
            client_io,
            mcp_rs::protocol::ClientLifecycleMode::Discover {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
            },
        )
        .await
        .unwrap();
    let result = client
        .call_tool(CallToolRequestParams::new("ask").with_arguments(Default::default()))
        .await
        .unwrap();
    assert_eq!(result.content[0].as_text().unwrap().text, "Felix");
    client.cancel().await.unwrap();
    task.await.unwrap();
}

#[cfg(feature = "elicitation")]
#[tokio::test]
async fn modern_elicitation_requires_explicit_request_form_capability() {
    use mcp_rs::protocol::{ClientLifecycleMode, ClientServiceExt};
    for capabilities in [
        ClientCapabilities::default(),
        ClientCapabilities::builder().enable_elicitation().build(),
    ] {
        let mut server = Server::new("modern-form", "1");
        server
            .tool("ask")
            .handler(|ctx, _: Input| async move {
                Session::from_context(&ctx)
                    .elicit::<Answer>("Name?")
                    .await
                    .map(|answer| answer.map(|answer| answer.name).unwrap_or_default())
                    .map_err(|error| mcp_rs::ToolError::new(error.to_string()))
            })
            .unwrap();
        let (client_io, server_io) = tokio::io::duplex(65536);
        let job = tokio::spawn(async move {
            server
                .serve(server_io)
                .await
                .unwrap()
                .waiting()
                .await
                .unwrap();
        });
        let client = ClientConfig::new(capabilities, Implementation::new("no-form", "1"))
            .serve_with_lifecycle(
                client_io,
                ClientLifecycleMode::Discover {
                    preferred_versions: vec![ProtocolVersion::V_2026_07_28],
                },
            )
            .await
            .unwrap();
        let result = client
            .call_tool(CallToolRequestParams::new("ask").with_arguments(Default::default()))
            .await
            .unwrap();
        assert_eq!(result.is_error, Some(true));
        client.cancel().await.unwrap();
        job.await.unwrap();
    }
}

#[tokio::test]
async fn concurrent_progress_increases_at_large_values_and_rejects_overflow() {
    use mcp_rs::{ToolError, protocol::service::ServiceError};
    let mut server = Server::new("large-progress", "1");
    server
        .tool("work")
        .handler(|ctx, _: Input| async move {
            let session = Session::from_context(&ctx);
            let large = 2_f64.powi(53);
            session
                .progress(large, None, None)
                .await
                .map_err(|error| ToolError::new(error.to_string()))?;
            // Each clone shares the same notification ordering and last value.
            for result in futures_util::future::join_all((0..8).map(|_| {
                let session = session.clone();
                async move { session.progress(large, None, None).await }
            }))
            .await
            {
                result.map_err(|error| ToolError::new(error.to_string()))?;
            }
            session
                .progress(f64::MAX, None, None)
                .await
                .map_err(|error| ToolError::new(error.to_string()))?;
            // Neither a repeated maximum nor a decreasing value can be advanced
            // once the greatest finite float has been sent.
            for value in [f64::MAX, 0.0] {
                match session.progress(value, None, None).await {
                    Err(ServiceError::McpError(error))
                        if error.code == ErrorCode::INVALID_PARAMS => {}
                    other => {
                        return Err(ToolError::new(format!(
                            "expected progress overflow, got {other:?}"
                        )));
                    }
                }
            }
            Ok(ctx.meta.get_progress_token())
        })
        .unwrap();
    let rec = recorder();
    let events = rec.events.clone();
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
    let client = rec
        .serve_with_lifecycle(
            client_io,
            mcp_rs::protocol::ClientLifecycleMode::Discover {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
            },
        )
        .await
        .unwrap();
    let request = CallToolRequestParams::new("work").with_arguments(Default::default());
    let result = client.call_tool(request).await.unwrap();
    // A final response is ordered after all progress sends from this handler.
    assert_ne!(result.is_error, Some(true), "{result:?}");
    let token: ProgressToken =
        serde_json::from_str(&result.content[0].as_text().unwrap().text).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while events.lock().await.len() < 10 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let recorded = events.lock().await;
    assert_eq!(recorded.len(), 10, "overflow must not emit a notification");
    assert!(
        recorded
            .iter()
            .all(|event| event.progress_token == token && event.progress.is_finite()),
        "{recorded:?}"
    );
    assert!(
        recorded
            .windows(2)
            .all(|pair| pair[0].progress < pair[1].progress)
    );
    assert_eq!(recorded.first().unwrap().progress, 2_f64.powi(53));
    assert_eq!(recorded.last().unwrap().progress, f64::MAX);
    drop(recorded);
    assert!(!client.list_tools(None).await.unwrap().tools.is_empty());
    client.cancel().await.unwrap();
    running.await.unwrap();
}
