//! Progressive discovery through ordinary MCP requests, including response caching.
#![cfg(feature = "client")]
#![allow(clippy::unwrap_used)]
use mcp_rs::{
    Client, LiveServer, Server, VisibilityTarget,
    model::*,
    protocol::{ServerHandler, ServiceExt, service::ServiceError},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[derive(Deserialize, Serialize, JsonSchema)]
struct Input {
    q: String,
}

fn app() -> Server {
    let mut server = Server::new("discovery", "1");
    server
        .tool("create")
        .group("billing")
        .tags(["write", "finance"])
        .defer_schema()
        .title("Create invoice")
        .meta(MetaObject(object(json!({"custom":true}))))
        .output_schema::<Input>()
        .unwrap()
        .handler(|_, input: Input| async move { Ok(input) })
        .unwrap();
    server
        .tool("list")
        .group("billing")
        .tags(["read", "finance"])
        .output_schema::<Input>()
        .unwrap()
        .handler(|_, input: Input| async move { Ok(input) })
        .unwrap();
    server
        .tool("search")
        .tags(["read"])
        .handler(|_, input: Input| async move { Ok(input) })
        .unwrap();
    server
}
async fn connect<H: ServerHandler>(server: H) -> (Client, tokio::task::JoinHandle<()>) {
    let (client_io, server_io) = tokio::io::duplex(65536);
    let task = tokio::spawn(async move {
        server
            .serve(server_io)
            .await
            .unwrap()
            .waiting()
            .await
            .unwrap();
    });
    let client = Client::connect(client_io).await.unwrap();
    (client, task)
}
async fn close(client: Client, task: tokio::task::JoinHandle<()>) {
    client.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
}
fn params(value: Value) -> ListToolsRequestParams {
    serde_json::from_value(value).unwrap()
}
fn names(page: &ListToolsResult) -> Vec<&str> {
    page.tools.iter().map(|tool| tool.name.as_ref()).collect()
}

#[tokio::test]
async fn standard_list_filters_defers_and_restores_schemas_without_weakening_calls() {
    let (client, task) = connect(app()).await;
    let default = client.peer().list_tools(None).await.unwrap();
    assert_eq!(names(&default), ["create", "list", "search"]);
    let value = serde_json::to_value(&default).unwrap();
    assert!(value["tools"][0].get("inputSchema").is_none());
    assert!(value["tools"][0].get("outputSchema").is_none());
    assert!(default.tools[1].input_schema.is_some());
    assert_eq!(default.tools[0].group, "billing");
    assert_eq!(default.tools[0].tags, ["write", "finance"]);
    assert_eq!(value["tools"][0]["_meta"], json!({"custom":true}));
    assert_eq!(value["tools"][0]["title"], "Create invoice");
    for (query, expected) in [
        (json!({"group":"billing"}), vec!["create", "list"]),
        (json!({"tags":["finance", "read", "read"]}), vec!["list"]),
        (json!({"group":"billing", "tags":["Read"]}), vec![]),
        (json!({"group":"missing"}), vec![]),
        (json!({"tags":["read"]}), vec!["list", "search"]),
    ] {
        let page = client.peer().list_tools(Some(params(query))).await.unwrap();
        assert_eq!(names(&page), expected);
    }
    for detail in ["names", "full"] {
        let page = client
            .peer()
            .list_tools(Some(params(json!({"detail":detail}))))
            .await
            .unwrap();
        for tool in &page.tools {
            assert_eq!(tool.input_schema.is_some(), detail == "full");
        }
        for tool in &page.tools[..2] {
            assert_eq!(tool.output_schema.is_some(), detail == "full");
        }
    }
    assert!(client.call::<_, Value>("create", &json!({})).await.is_err());
    let result: Value = client.call("create", &json!({"q":"ok"})).await.unwrap();
    assert_eq!(result, json!({"q":"ok"}));
    close(client, task).await;
}

#[tokio::test]
async fn filtered_visible_pagination_and_collection_preserve_query() {
    let mut server = Server::new("pages", "1").with_visibility_policy(
        |_, target| !matches!(target, VisibilityTarget::Tool {name} if name.starts_with("hidden")),
    );
    for i in 0..103 {
        for prefix in ["hidden", "visible", "other"] {
            server
                .tool(format!("{prefix}_{i:03}"))
                .group(if prefix == "other" {
                    "other"
                } else {
                    "research"
                })
                .tags(["read"])
                .handler(|_, _: Input| async { Ok("ok") })
                .unwrap();
        }
    }
    let (client, task) = connect(server).await;
    let mut query = params(json!({"group":"research","tags":["read"],"detail":"names"}));
    let first = client.peer().list_tools(Some(query.clone())).await.unwrap();
    assert_eq!(first.tools.len(), 100);
    assert_eq!(first.next_cursor.as_deref(), Some("visible_099"));
    assert!(first.tools.iter().all(|tool| tool.input_schema.is_none()));
    query.cursor = first.next_cursor;
    let second = client.peer().list_tools(Some(query.clone())).await.unwrap();
    assert_eq!(
        names(&second),
        ["visible_100", "visible_101", "visible_102"]
    );
    assert!(second.next_cursor.is_none());
    assert!(second.tools.iter().all(|tool| tool.input_schema.is_none()));
    for cursor in ["hidden_099", "other_099", "unknown"] {
        query.cursor = Some(cursor.into());
        assert!(client.peer().list_tools(Some(query.clone())).await.is_err());
    }
    query.cursor = None;
    let all = client.peer().list_all_tools_with(query).await.unwrap();
    assert_eq!(all.len(), 103);
    assert!(
        all.iter()
            .all(|tool| tool.group == "research" && tool.input_schema.is_none())
    );
    close(client, task).await;
}

#[tokio::test]
async fn live_list_uses_current_registry_and_visibility_and_fails_closed() {
    let live = LiveServer::new(app());
    let (client, task) = connect(live.clone()).await;
    // Exercise server failures directly; the default client permits stale-on-error.
    client
        .peer()
        .set_response_cache_config(mcp_rs::protocol::service::ClientCacheConfig::disabled())
        .await;
    let query = params(json!({"group":"billing","detail":"full"}));
    assert_eq!(
        names(&client.peer().list_tools(Some(query.clone())).await.unwrap()),
        ["create", "list"]
    );
    live.replace(app().with_visibility_policy(
        |_, target| !matches!(target, VisibilityTarget::Tool {name} if name == "create"),
    ));
    assert_eq!(
        names(&client.peer().list_tools(Some(query.clone())).await.unwrap()),
        ["list"]
    );
    let mut invalid = query.clone();
    invalid.cursor = Some("create".into());
    assert!(client.peer().list_tools(Some(invalid)).await.is_err());
    let mut replacement = Server::new("replacement", "2");
    replacement
        .tool("new")
        .group("billing")
        .defer_schema()
        .handler(|_, _: Input| async { Ok("new") })
        .unwrap();
    live.replace(replacement);
    let page = client.peer().list_tools(Some(query.clone())).await.unwrap();
    assert_eq!(names(&page), ["new"]);
    assert!(page.tools[0].input_schema.is_some());
    live.replace(app().with_visibility_policy(|_, _| panic!("policy failed")));
    assert!(client.peer().list_tools(Some(query)).await.is_err());
    close(client, task).await;
}

#[tokio::test]
async fn malformed_wire_parameters_are_invalid_params_not_custom_requests() {
    let (client, task) = connect(app()).await;
    for value in [
        json!({"detail":"unknown"}),
        json!({"detail":""}),
        json!({"tags":"read"}),
        json!({"group":42}),
        json!({"typo":true}),
    ] {
        let error = client
            .peer()
            .send_request(ClientRequest::CustomRequest(CustomRequest::new(
                "tools/list",
                Some(value),
            )))
            .await
            .unwrap_err();
        let ServiceError::McpError(error) = error else {
            panic!("unexpected error: {error:?}")
        };
        assert_eq!(error.code, ErrorCode::INVALID_PARAMS);
    }
    close(client, task).await;
}

#[tokio::test]
async fn cache_keys_isolate_filters_detail_and_pages_and_identical_queries_hit() {
    let evaluations = Arc::new(AtomicUsize::new(0));
    let count = evaluations.clone();
    let server = app()
        .with_result_cache(60_000, CacheScope::Public)
        .with_visibility_policy(move |_, _| {
            count.fetch_add(1, Ordering::SeqCst);
            true
        });
    let (client, task) = connect(server).await;
    let peer = client.peer();
    let default = peer.list_tools(None).await.unwrap();
    assert_eq!(evaluations.load(Ordering::SeqCst), 3);
    assert_eq!(
        peer.list_tools(Some(ListToolsRequestParams::default()))
            .await
            .unwrap(),
        default
    );
    assert_eq!(evaluations.load(Ordering::SeqCst), 3);
    let query = params(json!({"group":"billing","tags":["write"],"detail":"full"}));
    let full = peer.list_tools(Some(query.clone())).await.unwrap();
    assert_eq!(names(&full), ["create"]);
    assert!(full.tools[0].input_schema.is_some());
    assert_eq!(peer.list_tools(Some(query)).await.unwrap(), full);
    assert_eq!(evaluations.load(Ordering::SeqCst), 6);
    let lean = peer
        .list_tools(Some(params(
            json!({"group":"billing","tags":["write"],"detail":"names"}),
        )))
        .await
        .unwrap();
    assert!(lean.tools[0].input_schema.is_none());
    let read = peer
        .list_tools(Some(params(
            json!({"group":"billing","tags":["read"],"detail":"full"}),
        )))
        .await
        .unwrap();
    assert_eq!(names(&read), ["list"]);
    let other = peer
        .list_tools(Some(params(
            json!({"group":"missing","tags":["read"],"detail":"full"}),
        )))
        .await
        .unwrap();
    assert!(other.tools.is_empty());
    let tail = peer
        .list_tools(Some(params(
            json!({"group":"billing","tags":["read"],"detail":"full","cursor":"list"}),
        )))
        .await
        .unwrap();
    assert!(tail.tools.is_empty());
    assert_eq!(evaluations.load(Ordering::SeqCst), 18);
    close(client, task).await;
}
