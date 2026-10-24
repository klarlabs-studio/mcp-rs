//! Missing-core feature tests and typed client round trips.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use mcp_rs::protocol::ClientServiceExt as _;
use mcp_rs::{Server, model::*, protocol::ServiceExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Serialize, Deserialize, JsonSchema)]
struct Name {
    name: String,
}
#[derive(Serialize, Deserialize, JsonSchema)]
struct Count {
    #[schemars(range(min = 1, max = 5))]
    count: i64,
}
async fn connect(
    server: Server,
) -> (
    mcp_rs::protocol::service::RunningService<mcp_rs::protocol::RoleClient, ()>,
    tokio::task::JoinHandle<()>,
) {
    let (client_io, server_io) = tokio::io::duplex(65536);
    let task = tokio::spawn(async move {
        let service = server.serve(server_io).await.unwrap();
        service.waiting().await.unwrap();
    });
    (
        ().serve_with_lifecycle(
            client_io,
            mcp_rs::protocol::ClientLifecycleMode::Discover {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
            },
        )
        .await
        .unwrap(),
        task,
    )
}
fn call(name: &str, args: serde_json::Value) -> CallToolRequestParams {
    CallToolRequestParams::new(name.to_owned()).with_arguments(args.as_object().unwrap().clone())
}

#[tokio::test]
async fn schema_constraints_run_before_business_logic_and_can_be_opted_out() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let mut server = Server::new("validation", "1");
    server
        .tool("strict")
        .handler(move |_, input: Count| {
            observed.fetch_add(1, Ordering::SeqCst);
            async move { Ok(input.count) }
        })
        .unwrap();
    server
        .tool("skip")
        .skip_validation()
        .handler(|_, input: Count| async move { Ok(input.count) })
        .unwrap();
    let (client, task) = connect(server).await;
    assert_eq!(
        client
            .call_tool(call("strict", json!({"count":0})))
            .await
            .unwrap()
            .is_error,
        Some(true)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        client
            .call_tool(call("skip", json!({"count":0})))
            .await
            .unwrap()
            .is_error,
        Some(false)
    );
    client.cancel().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn output_schemas_are_advertised_and_enforced_without_losing_error_content() {
    let mut server = Server::new("outputs", "1");
    server
        .tool("valid")
        .output_schema::<Name>()
        .unwrap()
        .handler(|_, input: Name| async move { Ok(input) })
        .unwrap();
    server
        .tool("invalid")
        .output_schema::<Name>()
        .unwrap()
        .handler(|_, _: Name| async { Ok(json!({"wrong":1})) })
        .unwrap();
    server
        .tool("error")
        .output_schema::<Name>()
        .unwrap()
        .rich_handler(|_, _: Name| async {
            Ok(CallToolResult::error(vec![ContentBlock::text(
                "Could not complete",
            )]))
        })
        .unwrap();
    let (client, task) = connect(server).await;
    assert!(
        client
            .list_all_tools()
            .await
            .unwrap()
            .iter()
            .all(|tool| tool.output_schema.is_some())
    );
    let result = client
        .call_tool(call("valid", json!({"name":"Felix"})))
        .await
        .unwrap();
    assert_eq!(result.structured_content, Some(json!({"name":"Felix"})));
    assert!(
        client
            .call_tool(call("invalid", json!({"name":"Felix"})))
            .await
            .is_err()
    );
    let result = client
        .call_tool(call("error", json!({"name":"Felix"})))
        .await
        .unwrap();
    assert_eq!(result.is_error, Some(true));
    assert!(result.structured_content.is_none());
    assert_eq!(
        result.content[0].as_text().unwrap().text,
        "Could not complete"
    );
    client.cancel().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn rich_results_preserve_empty_objects_and_non_text_content() {
    let mut server = Server::new("rich", "1");
    server
        .tool("rich")
        .meta(MetaObject(
            serde_json::from_value(json!({"custom":1})).unwrap(),
        ))
        .ui_resource("ui://app/view")
        .rich_handler(|_, _: Name| async {
            let mut result = CallToolResult::success(vec![
                ContentBlock::image("AAEC", "image/png"),
                ContentBlock::text("Display"),
            ]);
            result.structured_content = Some(json!({}));
            Ok(result)
        })
        .unwrap();
    let (client, task) = connect(server).await;
    let result = client
        .call_tool(call("rich", json!({"name":"Felix"})))
        .await
        .unwrap();
    assert_eq!(result.structured_content, Some(json!({})));
    let wire = serde_json::to_value(result).unwrap();
    assert_eq!(wire["content"][0]["type"], "image");
    assert_eq!(wire["_meta"]["custom"], 1);
    assert_eq!(wire["_meta"]["ui"]["resourceUri"], "ui://app/view");
    client.cancel().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn uri_templates_and_binary_resources_round_trip_with_exact_match_priority() {
    let mut server = Server::new("resources", "1");
    server
        .resource("users://{id}")
        .template_handler(|_, _, params| async move { Ok(params["id"].clone()) })
        .unwrap();
    server
        .resource("users://current")
        .handler(|_, _| async { Ok("fixed".into()) })
        .unwrap();
    server
        .resource("blob://data")
        .content_handler(|_, uri, _| async move {
            Ok(vec![
                ResourceContents::blob("AAEC", uri).with_mime_type("application/octet-stream"),
            ])
        })
        .unwrap();
    assert!(
        server
            .resource("bad://{}")
            .handler(|_, _| async { Ok(String::new()) })
            .is_err()
    );
    let (client, task) = connect(server).await;
    assert_eq!(client.list_all_resources().await.unwrap().len(), 2);
    assert_eq!(client.list_all_resource_templates().await.unwrap().len(), 1);
    let read = client
        .read_resource(ReadResourceRequestParams::new("users://a%20b"))
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(read).unwrap()["contents"][0]["text"],
        "a%20b"
    );
    let read = client
        .read_resource(ReadResourceRequestParams::new("users://current"))
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(read).unwrap()["contents"][0]["text"],
        "fixed"
    );
    let read = client
        .read_resource(ReadResourceRequestParams::new("blob://data"))
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(read).unwrap()["contents"][0]["blob"],
        "AAEC"
    );
    assert!(
        client
            .read_resource(ReadResourceRequestParams::new("users://a/b"))
            .await
            .is_err()
    );
    client.cancel().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn pagination_is_stable_and_rejects_unknown_cursors() {
    let mut server = Server::new("pages", "1");
    for i in 0..101 {
        server
            .tool(format!("tool_{i:03}"))
            .handler(|_, _: Name| async { Ok("ok") })
            .unwrap();
    }
    let (client, task) = connect(server).await;
    let first = client.list_tools(None).await.unwrap();
    assert_eq!(first.tools.len(), 100);
    assert_eq!(first.next_cursor.as_deref(), Some("tool_099"));
    let second = client
        .list_tools(Some(
            ListToolsRequestParams::default().with_cursor(first.next_cursor),
        ))
        .await
        .unwrap();
    assert_eq!(second.tools.len(), 1);
    assert!(second.next_cursor.is_none());
    assert_eq!(client.list_all_tools().await.unwrap().len(), 101);
    assert!(
        client
            .list_tools(Some(
                ListToolsRequestParams::default().with_cursor(Some("absent".into()))
            ))
            .await
            .is_err()
    );
    client.cancel().await.unwrap();
    task.await.unwrap();
}

#[cfg(feature = "client")]
#[tokio::test]
async fn typed_client_handles_structured_values_raw_strings_and_execution_failures() {
    let mut server = Server::new("client", "1");
    server
        .tool("object")
        .output_schema::<Name>()
        .unwrap()
        .handler(|_, input: Name| async move { Ok(input) })
        .unwrap();
    server
        .tool("text")
        .rich_handler(|_, _: Name| async {
            Ok(CallToolResult::success(vec![
                ContentBlock::image("AAEC", "image/png"),
                ContentBlock::text("\"quoted\""),
            ]))
        })
        .unwrap();
    server
        .tool("fail")
        .rich_handler(|_, _: Name| async {
            Ok(CallToolResult::error(vec![ContentBlock::text(
                "Expected failure",
            )]))
        })
        .unwrap();
    let (client_io, server_io) = tokio::io::duplex(65536);
    let task = tokio::spawn(async move {
        let service = server.serve(server_io).await.unwrap();
        service.waiting().await.unwrap();
    });
    let client = mcp_rs::Client::connect(client_io).await.unwrap();
    let input = Name {
        name: "Felix".into(),
    };
    let output: Name = client
        .tool::<Name, Name>("object")
        .call(&input)
        .await
        .unwrap();
    assert_eq!(output.name, "Felix");
    let output: String = client.call("text", &input).await.unwrap();
    assert_eq!(output, "\"quoted\"");
    assert!(matches!(
        client.call::<_, String>("fail", &input).await,
        Err(mcp_rs::ClientError::ToolFailure(_))
    ));
    assert!(matches!(
        client.call::<_, String>("text", &42).await,
        Err(mcp_rs::ClientError::InvalidInput)
    ));
    client.close().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn live_registry_updates_reach_existing_connections() {
    let mut initial = Server::new("live", "1");
    initial
        .tool("old")
        .handler(|_, _: Name| async { Ok("old") })
        .unwrap();
    let live = initial.into_live();
    let handle = live.clone();
    let (client_io, server_io) = tokio::io::duplex(65536);
    let task = tokio::spawn(async move {
        let service = live.serve(server_io).await.unwrap();
        service.waiting().await.unwrap();
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
    assert_eq!(client.list_all_tools().await.unwrap()[0].name, "old");
    let mut updated = handle.snapshot();
    assert!(updated.remove_tool("old"));
    updated
        .tool("new")
        .handler(|_, _: Name| async { Ok("new") })
        .unwrap();
    handle.replace(updated);
    assert_eq!(client.list_all_tools().await.unwrap()[0].name, "new");
    assert!(
        client
            .call_tool(call("old", json!({"name":"a"})))
            .await
            .is_err()
    );
    assert_eq!(
        client
            .call_tool(call("new", json!({"name":"a"})))
            .await
            .unwrap()
            .content[0]
            .as_text()
            .unwrap()
            .text,
        "new"
    );
    client.cancel().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn replacing_a_live_registry_does_not_cancel_inflight_requests() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let mut initial = Server::new("live", "1");
    let began = entered.clone();
    let finish = release.clone();
    initial
        .tool("old")
        .handler(move |_, _: Name| {
            let began = began.clone();
            let finish = finish.clone();
            async move {
                began.notify_one();
                finish.notified().await;
                Ok("old completed")
            }
        })
        .unwrap();
    let live = initial.into_live();
    let handle = live.clone();
    let (client_io, server_io) = tokio::io::duplex(65536);
    let task = tokio::spawn(async move {
        let service = live.serve(server_io).await.unwrap();
        service.waiting().await.unwrap();
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
    let response = async {
        client
            .call_tool(call("old", json!({"name":"a"})))
            .await
            .unwrap()
    };
    let update = async {
        entered.notified().await;
        handle.replace(Server::new("live", "1"));
        release.notify_one();
    };
    let (response, _) = tokio::join!(response, update);
    assert_eq!(response.content[0].as_text().unwrap().text, "old completed");
    assert!(client.list_all_tools().await.unwrap().is_empty());
    client.cancel().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn size_limits_prevent_execution_and_observers_record_failures() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let events = Arc::new(std::sync::Mutex::new(vec![]));
    let audit = events.clone();
    let mut server = Server::new("limits", "1")
        .with_middleware(mcp_rs::middleware::observe(move |event| {
            audit.lock().unwrap().push(event)
        }))
        .with_middleware(mcp_rs::middleware::size_limit(20));
    server
        .tool("work")
        .handler(move |_, _: Name| {
            observed.fetch_add(1, Ordering::SeqCst);
            async { Ok("done") }
        })
        .unwrap();
    let (client, task) = connect(server).await;
    client
        .call_tool(call("work", json!({"name":"a"})))
        .await
        .unwrap();
    assert!(
        client
            .call_tool(call("work", json!({"name":"a much longer value"})))
            .await
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    {
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert!(events[0].success);
        assert!(!events[1].success);
    }
    client.cancel().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn shared_rate_limiter_rejects_excess_calls() {
    let limiter = mcp_rs::middleware::rate_limit(
        std::num::NonZeroU32::new(1).unwrap(),
        std::time::Duration::from_secs(60),
    )
    .unwrap();
    let mut server = Server::new("limits", "1").with_middleware(limiter);
    server
        .tool("work")
        .handler(|_, _: Name| async { Ok("done") })
        .unwrap();
    let (client, task) = connect(server).await;
    client
        .call_tool(call("work", json!({"name":"a"})))
        .await
        .unwrap();
    assert!(
        client
            .call_tool(call("work", json!({"name":"a"})))
            .await
            .is_err()
    );
    assert!(
        mcp_rs::middleware::rate_limit(
            std::num::NonZeroU32::new(1).unwrap(),
            std::time::Duration::ZERO
        )
        .is_err()
    );
    client.cancel().await.unwrap();
    task.await.unwrap();
}

#[cfg(all(feature = "client-http", feature = "http"))]
#[tokio::test]
async fn typed_client_connects_over_real_loopback_http_and_closes_cleanly() {
    let mut server = Server::new("http-client", "1");
    server
        .tool("hello")
        .handler(|_, input: Name| async move { Ok(format!("Hello {}", input.name)) })
        .unwrap();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let stop = shutdown.clone();
    let task = tokio::spawn(async move {
        mcp_rs::transport::serve_http(server, listener, stop)
            .await
            .unwrap();
    });
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap();
    let config=mcp_rs::protocol::transport::streamable_http_client::StreamableHttpClientTransportConfig::with_uri(format!("http://{address}/mcp"));
    let client = mcp_rs::Client::http(http, config).await.unwrap();
    let output: String = client
        .call(
            "hello",
            &Name {
                name: "Felix".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(output, "Hello Felix");
    client.close().await.unwrap();
    shutdown.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn parameter_completion_uses_typed_requests_and_shared_middleware() {
    let server = Server::new("completion", "1").with_completion_handler(|_, request| async move {
        Ok(CompleteResult::new(
            CompletionInfo::new(vec![format!("{}-complete", request.argument.value)]).unwrap(),
        ))
    });
    let (client, task) = connect(server).await;
    let request:CompleteRequestParams=serde_json::from_value(json!({"ref":{"type":"ref/resource","uri":"users://{id}"},"argument":{"name":"id","value":"fel"}})).unwrap();
    let result = client.complete(request).await.unwrap();
    assert_eq!(result.completion.values, vec!["fel-complete"]);
    client.cancel().await.unwrap();
    task.await.unwrap();
}
