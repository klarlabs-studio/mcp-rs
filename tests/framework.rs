//! Protocol integration tests for the typed framework.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use futures_util::FutureExt;
use mcp_rs::protocol::ClientServiceExt as _;
use mcp_rs::protocol::{RoleClient, ServiceExt, service::RunningService};
use mcp_rs::{Middleware, Server, ToolError, middleware, model::*};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Hello {
    name: String,
}

async fn connect(server: Server) -> (RunningService<RoleClient, ()>, tokio::task::JoinHandle<()>) {
    let (client_io, server_io) = tokio::io::duplex(65536);
    let task = tokio::spawn(async move {
        let running = server.serve(server_io).await.unwrap();
        let _ = running.waiting().await;
    });
    let client = ()
        .serve_with_lifecycle(
            client_io,
            mcp_rs::protocol::ClientLifecycleMode::Discover {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
            },
        )
        .await
        .unwrap();
    (client, task)
}
fn app() -> Server {
    let mut server = Server::new("test-app", "0.1.0");
    server
        .tool("hello")
        .description("Say hello")
        .handler(|_, input: Hello| async move { Ok(format!("Hello {}", input.name)) })
        .unwrap();
    server
}
fn call(name: &str, arguments: Value) -> CallToolRequestParams {
    CallToolRequestParams::new(name.to_owned())
        .with_arguments(arguments.as_object().unwrap().clone())
}
fn wire(value: impl serde::Serialize) -> Value {
    serde_json::to_value(value).unwrap()
}

#[tokio::test]
async fn typed_tools_negotiate_list_schema_and_call() {
    let (client, task) = connect(app()).await;
    assert_eq!(
        client
            .peer_info()
            .unwrap()
            .server_info
            .as_ref()
            .unwrap()
            .name,
        "test-app"
    );
    let tools = client.list_tools(None).await.unwrap().tools;
    assert_eq!(tools[0].name, "hello");
    assert_eq!(
        tools[0].input_schema.as_ref().unwrap()["required"],
        json!(["name"])
    );
    assert_eq!(
        tools[0].input_schema.as_ref().unwrap()["additionalProperties"],
        false
    );
    let result = wire(
        client
            .call_tool(call("hello", json!({"name":"Felix"})))
            .await
            .unwrap(),
    );
    assert_eq!(result["content"][0]["text"], "Hello Felix");
    assert_eq!(result["isError"], false);
    client.cancel().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn invalid_inputs_never_reach_handler() {
    let called = Arc::new(AtomicUsize::new(0));
    let counter = called.clone();
    let mut server = Server::new("test", "1");
    server
        .tool("strict")
        .handler(move |_, _: Hello| {
            counter.fetch_add(1, Ordering::SeqCst);
            async { Ok("ok") }
        })
        .unwrap();
    let (client, task) = connect(server).await;
    for args in [
        json!({}),
        json!({"name":42}),
        json!({"name":"ok","unexpected":true}),
    ] {
        let result = wire(client.call_tool(call("strict", args)).await.unwrap());
        assert_eq!(result["isError"], true);
    }
    assert_eq!(called.load(Ordering::SeqCst), 0);
    assert!(client.call_tool(call("unknown", json!({}))).await.is_err());
    client.cancel().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn object_results_and_ui_metadata_survive_wire() {
    let mut server = Server::new("test", "1");
    server
        .tool("structured")
        .ui_resource("ui://app/view")
        .output_schema::<Hello>()
        .unwrap()
        .handler(|_, input: Hello| async move { Ok(json!({"name":input.name})) })
        .unwrap();
    let (client, task) = connect(server).await;
    let listed = wire(client.list_tools(None).await.unwrap());
    assert_eq!(
        listed["tools"][0]["_meta"]["ui"]["resourceUri"],
        "ui://app/view"
    );
    let result = wire(
        client
            .call_tool(call("structured", json!({"name":"Felix"})))
            .await
            .unwrap(),
    );
    assert_eq!(result["structuredContent"]["name"], "Felix");
    assert_eq!(result["_meta"]["ui"]["resourceUri"], "ui://app/view");
    client.cancel().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn tool_failure_is_a_caller_visible_execution_error() {
    let mut server = Server::new("test", "1");
    server
        .tool("fail")
        .handler(|_, _: Hello| async { Err::<String, _>(ToolError::new("No results found")) })
        .unwrap();
    let (client, task) = connect(server).await;
    let result = wire(
        client
            .call_tool(call("fail", json!({"name":"a"})))
            .await
            .unwrap(),
    );
    assert_eq!(result["isError"], true);
    assert_eq!(result["content"][0]["text"], "No results found");
    client.cancel().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn resources_and_required_prompts_round_trip() {
    let mut server = app();
    server
        .resource("about://server")
        .name("About")
        .mime_type("text/markdown")
        .handler(|_, _| async { Ok("# About".into()) })
        .unwrap();
    server
        .prompt("greet")
        .argument("name", "Person", true)
        .handler(|_, args| async move {
            Ok(GetPromptResult::new(vec![PromptMessage::new_text(
                Role::User,
                format!("Greet {}", args["name"]),
            )]))
        })
        .unwrap();
    let (client, task) = connect(server).await;
    assert_eq!(
        client.list_resources(None).await.unwrap().resources.len(),
        1
    );
    let result = wire(
        client
            .read_resource(ReadResourceRequestParams::new("about://server"))
            .await
            .unwrap(),
    );
    assert_eq!(result["contents"][0]["text"], "# About");
    assert_eq!(result["contents"][0]["mimeType"], "text/markdown");
    assert!(
        client
            .read_resource(ReadResourceRequestParams::new("about://unknown"))
            .await
            .is_err()
    );
    assert_eq!(client.list_prompts(None).await.unwrap().prompts.len(), 1);
    assert!(
        client
            .get_prompt(GetPromptRequestParams::new("greet"))
            .await
            .is_err()
    );
    let result = wire(
        client
            .get_prompt(
                GetPromptRequestParams::new("greet")
                    .with_arguments(json!({"name":"Felix"}).as_object().unwrap().clone()),
            )
            .await
            .unwrap(),
    );
    assert_eq!(result["messages"][0]["content"]["text"], "Greet Felix");
    client.cancel().await.unwrap();
    task.await.unwrap();
}

fn record(
    log: Arc<Mutex<Vec<&'static str>>>,
    before: &'static str,
    after: &'static str,
) -> Middleware {
    Arc::new(move |ctx, args, next| {
        let log = log.clone();
        async move {
            log.lock().unwrap().push(before);
            let result = next(ctx, args).await;
            log.lock().unwrap().push(after);
            result
        }
        .boxed()
    })
}
#[tokio::test]
async fn middleware_order_is_first_in_last_out() {
    let log = Arc::new(Mutex::new(vec![]));
    let server = app()
        .with_middleware(record(log.clone(), "a+", "a-"))
        .with_middleware(record(log.clone(), "b+", "b-"));
    let (client, task) = connect(server).await;
    client
        .call_tool(call("hello", json!({"name":"a"})))
        .await
        .unwrap();
    assert_eq!(*log.lock().unwrap(), vec!["a+", "b+", "b-", "a-"]);
    client.cancel().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn authorization_short_circuits() {
    let (client, task) = connect(app().with_middleware(middleware::authorize(|_| false))).await;
    assert!(
        client
            .call_tool(call("hello", json!({"name":"a"})))
            .await
            .is_err()
    );
    client.cancel().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn deadline_drops_async_handler() {
    let mut server = Server::new("test", "1").with_timeout(Duration::from_millis(20));
    server
        .tool("slow")
        .handler(|_, _: Hello| async {
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok("too late")
        })
        .unwrap();
    let (client, task) = connect(server).await;
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        client.call_tool(call("slow", json!({"name":"a"}))),
    )
    .await
    .unwrap();
    assert!(result.is_err());
    client.cancel().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn panic_is_contained_without_exposing_details() {
    let mut server = app();
    server
        .tool("panic")
        .handler(|_, _: Hello| async {
            panic!("private panic detail");
            #[allow(unreachable_code)]
            Ok::<_, ToolError>("unreachable")
        })
        .unwrap();
    let (client, task) = connect(server).await;
    let error = client
        .call_tool(call("panic", json!({"name":"a"})))
        .await
        .unwrap_err();
    assert!(!error.to_string().contains("private panic detail"));
    client
        .call_tool(call("hello", json!({"name":"still running"})))
        .await
        .unwrap();
    client.cancel().await.unwrap();
    task.await.unwrap();
}

#[test]
fn duplicate_registration_does_not_replace_existing_handler() {
    let mut server = app();
    assert!(
        server
            .tool("hello")
            .handler(|_, _: Hello| async { Ok("other") })
            .is_err()
    );
    assert!(
        server
            .tool("")
            .handler(|_, _: Hello| async { Ok("other") })
            .is_err()
    );
}

#[tokio::test]
async fn authorization_also_protects_resource_and_prompt_execution() {
    let mut server = app().with_middleware(middleware::authorize(|_| false));
    server
        .resource("private://data")
        .handler(|_, _| async { Ok("private".into()) })
        .unwrap();
    server
        .prompt("private")
        .handler(|_, _| async { Ok(GetPromptResult::new(vec![])) })
        .unwrap();
    let (client, task) = connect(server).await;
    assert!(
        client
            .read_resource(ReadResourceRequestParams::new("private://data"))
            .await
            .is_err()
    );
    assert!(
        client
            .get_prompt(GetPromptRequestParams::new("private"))
            .await
            .is_err()
    );
    client.cancel().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn cancelled_context_drops_an_uncooperative_async_handler() {
    let mut server = app();
    server
        .tool("cancel")
        .handler(|ctx, _: Hello| async move {
            ctx.ct.cancel();
            std::future::pending::<Result<String, ToolError>>().await
        })
        .unwrap();
    let (client, task) = connect(server).await;
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        client.call_tool(call("cancel", json!({"name":"a"}))),
    )
    .await
    .unwrap();
    assert!(result.is_err());
    client.cancel().await.unwrap();
    task.await.unwrap();
}

#[test]
fn registration_errors_are_typed_and_validate_prompt_arguments() {
    use mcp_rs::RegistrationError;
    let mut server = app();
    let duplicate = server
        .tool("hello")
        .handler(|_, _: Hello| async { Ok("other") });
    assert!(matches!(
        duplicate,
        Err(RegistrationError::Duplicate { kind: "tool", .. })
    ));
    let empty = server
        .resource(" ")
        .handler(|_, _| async { Ok(String::new()) });
    assert!(matches!(
        empty,
        Err(RegistrationError::EmptyIdentifier { kind: "resource" })
    ));
    let bad_schema = server
        .tool("scalar")
        .handler(|_, _: String| async { Ok("scalar") });
    assert!(matches!(
        bad_schema,
        Err(RegistrationError::InvalidInputSchema(_))
    ));
    let invalid_prompt = server
        .prompt("greet")
        .argument("name", "Person", true)
        .argument("name", "Duplicate", false)
        .handler(|_, _| async { Ok(GetPromptResult::new(vec![])) });
    assert!(
        matches!(invalid_prompt, Err(RegistrationError::InvalidPromptArgument(name)) if name == "name")
    );
}

#[tokio::test]
async fn cloned_configuration_is_isolated_from_later_registrations() {
    let mut original = app();
    let snapshot = original.clone();
    original
        .tool("second")
        .handler(|_, _: Hello| async { Ok("new") })
        .unwrap();
    let (first, task1) = connect(original).await;
    let (second, task2) = connect(snapshot).await;
    assert_eq!(first.list_tools(None).await.unwrap().tools.len(), 2);
    assert_eq!(second.list_tools(None).await.unwrap().tools.len(), 1);
    first.cancel().await.unwrap();
    second.cancel().await.unwrap();
    task1.await.unwrap();
    task2.await.unwrap();
}

#[tokio::test]
async fn typed_middleware_can_update_tool_content() {
    let middleware: Middleware = Arc::new(|ctx, args, next| {
        async move {
            let mut response = next(ctx, args).await?;
            if let mcp_rs::Response::Tool(result) = &mut response {
                result
                    .content
                    .push(ContentBlock::text("inspected by middleware"));
            }
            Ok(response)
        }
        .boxed()
    });
    let (client, task) = connect(app().with_middleware(middleware)).await;
    let result = wire(
        client
            .call_tool(call("hello", json!({"name":"Felix"})))
            .await
            .unwrap(),
    );
    assert_eq!(result["content"][1]["text"], "inspected by middleware");
    client.cancel().await.unwrap();
    task.await.unwrap();
}

#[cfg(feature = "stdio")]
#[tokio::test]
async fn pre_cancelled_stdio_shutdown_returns_without_waiting_for_initialize() {
    let shutdown = tokio_util::sync::CancellationToken::new();
    shutdown.cancel();
    tokio::time::timeout(
        Duration::from_secs(1),
        mcp_rs::transport::serve_stdio(app(), shutdown),
    )
    .await
    .unwrap()
    .unwrap();
}

#[test]
fn server_configuration_can_be_shared_between_runtime_threads() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Server>();
}
