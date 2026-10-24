//! Request-aware discovery/access policy and bypass regression tests.
#![cfg(feature = "client")]
#![allow(clippy::unwrap_used)]
use futures_util::FutureExt;
use mcp_rs::{
    Client, Context, Server, VisibilityTarget,
    model::*,
    protocol::{
        ErrorData, RoleServer, ServerHandler, Service, ServiceExt,
        service::{NotificationContext, ServiceError},
    },
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
#[derive(Clone)]
struct Principal(&'static str);
// Test host boundary: identity is inserted by the service, never read from client metadata.
struct Authenticated<H> {
    inner: H,
    principal: Principal,
}
impl<H: ServerHandler> Service<RoleServer> for Authenticated<H> {
    async fn handle_request(
        &self,
        request: ClientRequest,
        mut context: Context,
    ) -> Result<ServerResult, ErrorData> {
        context.extensions.insert(self.principal.clone());
        self.inner.handle_request(request, context).await
    }
    async fn handle_notification(
        &self,
        notification: ClientNotification,
        context: NotificationContext<RoleServer>,
    ) -> Result<(), ErrorData> {
        self.inner.handle_notification(notification, context).await
    }
    fn get_info(&self) -> ServerConfig {
        ServerHandler::get_info(&self.inner)
    }
}
async fn connect<H: ServerHandler>(
    inner: H,
    principal: &'static str,
) -> (Client, tokio::task::JoinHandle<()>) {
    let (client_io, server_io) = tokio::io::duplex(65536);
    let task = tokio::spawn(async move {
        let service = Authenticated {
            inner,
            principal: Principal(principal),
        }
        .serve(server_io)
        .await
        .unwrap();
        service.waiting().await.unwrap();
    });
    (Client::connect(client_io).await.unwrap(), task)
}
async fn close(client: Client, task: tokio::task::JoinHandle<()>) {
    client.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
}
#[derive(Deserialize, JsonSchema)]
struct Input {}
fn policy(context: &Context, target: VisibilityTarget<'_>) -> bool {
    if context
        .extensions
        .get::<Principal>()
        .is_some_and(|principal| principal.0 == "staff")
    {
        return true;
    }
    match target {
        VisibilityTarget::Tool { name } | VisibilityTarget::Prompt { name } => name == "public",
        VisibilityTarget::Resource {
            uri,
            registered_uri,
            ..
        } => registered_uri != "data://private" && uri != "users://private",
        _ => false,
    }
}
fn app(counter: Arc<AtomicUsize>) -> Server {
    let mut server = Server::new("visibility", "1").with_visibility_policy(policy);
    for name in ["public", "private"] {
        let count = counter.clone();
        server
            .tool(name)
            .handler(move |_, _: Input| {
                count.fetch_add(1, Ordering::SeqCst);
                async { Ok("tool") }
            })
            .unwrap();
        let count = counter.clone();
        server
            .prompt(name)
            .argument("required", "value", true)
            .handler(move |_, _| {
                count.fetch_add(1, Ordering::SeqCst);
                async { Ok(GetPromptResult::new(vec![])) }
            })
            .unwrap();
        let count = counter.clone();
        server
            .resource(format!("data://{name}"))
            .handler(move |_, _| {
                count.fetch_add(1, Ordering::SeqCst);
                async { Ok("resource".into()) }
            })
            .unwrap();
    }
    let count = counter.clone();
    server
        .resource("users://{id}")
        .name("Users")
        .handler(move |_, _| {
            count.fetch_add(1, Ordering::SeqCst);
            async { Ok("user".into()) }
        })
        .unwrap();
    server
}
fn mcp(error: ServiceError) -> ErrorData {
    let ServiceError::McpError(error) = error else {
        panic!("expected protocol error")
    };
    error
}
#[tokio::test]
async fn per_connection_identity_filters_lists_and_blocks_hidden_objects_before_validation() {
    let counter = Arc::new(AtomicUsize::new(0));
    let server = app(counter.clone());
    let (guest, guest_task) = connect(server.clone(), "guest").await;
    let (staff, staff_task) = connect(server, "staff").await;
    assert_eq!(guest.peer().list_tools(None).await.unwrap().tools.len(), 1);
    assert_eq!(staff.peer().list_tools(None).await.unwrap().tools.len(), 2);
    assert_eq!(
        guest
            .peer()
            .list_resources(None)
            .await
            .unwrap()
            .resources
            .len(),
        1
    );
    assert_eq!(
        staff
            .peer()
            .list_resources(None)
            .await
            .unwrap()
            .resources
            .len(),
        2
    );
    assert_eq!(
        guest
            .peer()
            .list_resource_templates(None)
            .await
            .unwrap()
            .resource_templates
            .len(),
        1
    );
    assert_eq!(
        guest.peer().list_prompts(None).await.unwrap().prompts.len(),
        1
    );
    let hidden = mcp(guest
        .peer()
        .call_tool(CallToolRequestParams::new("private"))
        .await
        .unwrap_err());
    let missing = mcp(guest
        .peer()
        .call_tool(CallToolRequestParams::new("missing"))
        .await
        .unwrap_err());
    assert_eq!(hidden, missing);
    // Hidden prompts do not disclose their required argument names.
    let hidden = mcp(guest
        .peer()
        .get_prompt(GetPromptRequestParams::new("private"))
        .await
        .unwrap_err());
    let missing = mcp(guest
        .peer()
        .get_prompt(GetPromptRequestParams::new("missing"))
        .await
        .unwrap_err());
    assert_eq!(hidden, missing);
    let hidden = mcp(guest
        .peer()
        .read_resource(ReadResourceRequestParams::new("data://private"))
        .await
        .unwrap_err());
    let missing = mcp(guest
        .peer()
        .read_resource(ReadResourceRequestParams::new("missing://uri"))
        .await
        .unwrap_err());
    assert_eq!(hidden, missing);
    assert_eq!(counter.load(Ordering::SeqCst), 0);
    staff
        .peer()
        .call_tool(CallToolRequestParams::new("private").with_arguments(Default::default()))
        .await
        .unwrap();
    guest
        .peer()
        .read_resource(ReadResourceRequestParams::new("users://public"))
        .await
        .unwrap();
    assert!(
        guest
            .peer()
            .read_resource(ReadResourceRequestParams::new("users://private"))
            .await
            .is_err()
    );
    assert_eq!(counter.load(Ordering::SeqCst), 2);
    close(guest, guest_task).await;
    close(staff, staff_task).await;
}
#[tokio::test]
async fn filtering_precedes_pagination_and_hidden_cursors_are_not_accepted() {
    let mut server = Server::new("pages", "1").with_visibility_policy(|_, target| match target {
        VisibilityTarget::Tool { name } => name.starts_with("visible"),
        _ => true,
    });
    for i in 0..101 {
        for prefix in ["hidden", "visible"] {
            server
                .tool(format!("{prefix}_{i:03}"))
                .handler(|_, _: Input| async { Ok("ok") })
                .unwrap();
        }
    }
    let (client, task) = connect(server, "guest").await;
    let first = client.peer().list_tools(None).await.unwrap();
    assert_eq!(first.tools.len(), 100);
    assert_eq!(first.tools[0].name, "visible_000");
    let second = client
        .peer()
        .list_tools(Some(
            ListToolsRequestParams::default().with_cursor(first.next_cursor),
        ))
        .await
        .unwrap();
    assert_eq!(second.tools.len(), 1);
    assert!(second.next_cursor.is_none());
    assert!(
        client
            .peer()
            .list_tools(Some(
                ListToolsRequestParams::default().with_cursor(Some("hidden_099".into()))
            ))
            .await
            .is_err()
    );
    close(client, task).await;
}
#[tokio::test]
async fn denied_exact_resources_do_not_fall_back_to_templates_and_middleware_cannot_rewrite_to_hidden_uris()
 {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let mut server = Server::new("rewrite", "1")
        .with_visibility_policy(|_, target| {
            !matches!(
                target,
                VisibilityTarget::Resource {
                    uri: "users://private",
                    ..
                }
            )
        })
        .with_middleware(Arc::new(|ctx, mut args, next| {
            async move {
                if args.get("uri").is_some() {
                    args["uri"] = json!("users://private");
                }
                next(ctx, args).await
            }
            .boxed()
        }));
    server
        .resource("users://{id}")
        .handler(move |_, _| {
            counter.fetch_add(1, Ordering::SeqCst);
            async { Ok("template".into()) }
        })
        .unwrap();
    let counter = calls.clone();
    server
        .resource("users://private")
        .handler(move |_, _| {
            counter.fetch_add(1, Ordering::SeqCst);
            async { Ok("exact".into()) }
        })
        .unwrap();
    let (client, task) = connect(server, "guest").await;
    assert!(
        client
            .peer()
            .read_resource(ReadResourceRequestParams::new("users://private"))
            .await
            .is_err()
    );
    assert!(
        client
            .peer()
            .read_resource(ReadResourceRequestParams::new("users://public"))
            .await
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    close(client, task).await;
}
#[tokio::test]
async fn hidden_completion_references_and_middleware_rewrites_never_reach_callbacks() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let server = app(Arc::new(AtomicUsize::new(0)))
        .with_middleware(Arc::new(|ctx, mut args, next| {
            async move {
                if args.get("ref").is_some() {
                    args["ref"] = json!({"type":"ref/prompt","name":"private"});
                }
                next(ctx, args).await
            }
            .boxed()
        }))
        .with_completion_handler(move |_, _| {
            counter.fetch_add(1, Ordering::SeqCst);
            async {
                Ok(CompleteResult::new(
                    CompletionInfo::new(vec!["secret".into()]).unwrap(),
                ))
            }
        });
    let (client, task) = connect(server, "guest").await;
    for reference in [
        Reference::for_prompt("private"),
        Reference::for_resource("data://private"),
        Reference::for_prompt("public"),
    ] {
        assert!(
            client
                .peer()
                .complete(CompleteRequestParams::new(
                    reference,
                    ArgumentInfo::new("name", "a")
                ))
                .await
                .is_err()
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    close(client, task).await;
}
#[tokio::test]
async fn policy_panics_are_opaque_fail_closed_and_do_not_poison_subsequent_requests() {
    let server = app(Arc::new(AtomicUsize::new(0))).with_visibility_policy(|_, target| {
        if matches!(target, VisibilityTarget::Tool { name: "private" }) {
            panic!("private policy details");
        }
        true
    });
    let (client, task) = connect(server, "guest").await;
    let error = mcp(client.peer().list_tools(None).await.unwrap_err());
    assert!(!format!("{error:?}").contains("private policy details"));
    assert!(
        client
            .peer()
            .call_tool(CallToolRequestParams::new("private"))
            .await
            .is_err()
    );
    client
        .peer()
        .call_tool(CallToolRequestParams::new("public").with_arguments(Default::default()))
        .await
        .unwrap();
    close(client, task).await;
}
#[tokio::test]
async fn subscriptions_reject_hidden_concrete_uris_without_registering_streams() {
    let hub = mcp_rs::SubscriptionHub::default();
    let server = app(Arc::new(AtomicUsize::new(0))).with_subscription_hub(hub.clone());
    let (guest, task) = connect(server, "guest").await;
    let mut errors = Vec::new();
    for uri in ["users://private", "missing://uri"] {
        let mut stream = guest
            .listen(
                SubscriptionFilter::builder()
                    .resource_subscription(uri)
                    .build(),
            )
            .await
            .unwrap();
        // The preliminary context-free acknowledgement must not reveal existence.
        assert_eq!(
            stream.acknowledged().resource_subscriptions.as_deref(),
            Some([uri.to_owned()].as_slice())
        );
        let error = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap_err();
        errors.push(mcp(error));
        assert_eq!(hub.stats().active_streams, 0);
    }
    assert_eq!(errors[0], errors[1]);
    close(guest, task).await;
}
#[tokio::test]
async fn live_replacement_preserves_inflight_policy_and_applies_new_policy_to_later_requests() {
    let started = Arc::new(tokio::sync::Notify::new());
    let signal = started.clone();
    let release = Arc::new(tokio::sync::Notify::new());
    let wait = release.clone();
    let mut server = Server::new("live-policy", "1");
    server
        .tool("slow")
        .handler(move |_, _: Input| {
            let signal = signal.clone();
            let wait = wait.clone();
            async move {
                signal.notify_one();
                wait.notified().await;
                Ok("ok")
            }
        })
        .unwrap();
    let live = server.into_live();
    let (client, task) = connect(live.clone(), "guest").await;
    let peer = client.peer().clone();
    let running = tokio::spawn(async move {
        peer.call_tool(CallToolRequestParams::new("slow").with_arguments(Default::default()))
            .await
            .unwrap()
    });
    started.notified().await;
    live.replace(live.snapshot().with_visibility_policy(|_, _| false));
    assert!(
        client
            .peer()
            .list_tools(None)
            .await
            .unwrap()
            .tools
            .is_empty()
    );
    assert!(
        client
            .peer()
            .call_tool(CallToolRequestParams::new("slow").with_arguments(Default::default()))
            .await
            .is_err()
    );
    release.notify_one();
    running.await.unwrap();
    close(client, task).await;
}

#[tokio::test]
async fn hidden_task_tools_are_denied_before_admission() {
    let manager = mcp_rs::protocol::task_manager::TaskManager::new();
    let mut server = Server::new("hidden-tasks", "1")
        .with_task_manager(manager.clone())
        .with_visibility_policy(|_, _| false);
    server
        .tool("task")
        .task_handler(Default::default(), |_, _, _: Input| async {
            Ok(CallToolResult::success(vec![]))
        })
        .unwrap();
    let (client, task) = connect(server, "guest").await;
    let error = mcp(client
        .peer()
        .call_tool(CallToolRequestParams::new("task").with_arguments(Default::default()))
        .await
        .unwrap_err());
    assert_eq!(error.message, "Unknown tool");
    assert_eq!(manager.running_task_count(), 0);
    close(client, task).await;
    manager.shutdown();
}
#[cfg(all(feature = "http", feature = "client-http"))]
#[tokio::test]
async fn http_host_extensions_reach_visibility_before_discovery_and_execution() {
    let server = app(Arc::new(AtomicUsize::new(0))).with_visibility_policy(|context, target| {
        let principal = context
            .extensions
            .get::<axum::http::request::Parts>()
            .and_then(|parts| parts.extensions.get::<Principal>());
        principal.is_some_and(|principal| principal.0 == "guest")
            && matches!(target, VisibilityTarget::Tool { name: "public" })
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/mcp", listener.local_addr().unwrap());
    let stop = tokio_util::sync::CancellationToken::new();
    let done = stop.clone();
    let router = mcp_rs::transport::http_router(server, stop.child_token())
        .layer(axum::Extension(Principal("guest")));
    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(done.cancelled_owned())
            .await
            .unwrap()
    });
    let client = Client::http(reqwest::Client::new(),
        mcp_rs::protocol::transport::streamable_http_client::StreamableHttpClientTransportConfig::with_uri(endpoint)).await.unwrap();
    assert_eq!(
        client.peer().list_tools(None).await.unwrap().tools[0].name,
        "public"
    );
    client
        .peer()
        .call_tool(CallToolRequestParams::new("public").with_arguments(Default::default()))
        .await
        .unwrap();
    assert!(
        client
            .peer()
            .call_tool(CallToolRequestParams::new("private").with_arguments(Default::default()))
            .await
            .is_err()
    );
    client.close().await.unwrap();
    stop.cancel();
    task.await.unwrap();
}
