//! Wire-level cache-hint policy, resource overrides, and live configuration.
#![cfg(feature = "client")]
#![allow(clippy::unwrap_used)]

use futures_util::FutureExt;
use mcp_rs::{
    Client, Response, Server,
    model::*,
    protocol::{ServerHandler, ServiceExt},
};
use schemars::JsonSchema;
use serde::Deserialize;
use std::{sync::Arc, time::Duration};

async fn connect<H: ServerHandler>(server: H) -> (Client, tokio::task::JoinHandle<()>) {
    let (client_io, server_io) = tokio::io::duplex(65536);
    let task = tokio::spawn(async move {
        let running = server.serve(server_io).await.unwrap();
        running.waiting().await.unwrap();
    });
    let client = tokio::time::timeout(Duration::from_secs(3), Client::connect(client_io))
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), client.peer().list_tools(None))
        .await
        .unwrap()
        .unwrap();
    (client, task)
}

async fn close(client: Client, task: tokio::task::JoinHandle<()>) {
    client.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
}

fn configured(ttl_ms: u64, scope: CacheScope) -> Server {
    let mut server = Server::new("cache-policy", "1").with_result_cache(ttl_ms, scope);
    server
        .tool("hello")
        .handler(|_, _: EmptyInput| async { Ok("hello") })
        .unwrap();
    server
        .resource("about://server")
        .handler(|_, _| async { Ok("about".into()) })
        .unwrap();
    server
        .resource("about://ttl")
        .handler(|_, _| async { Ok("ttl".into()) })
        .unwrap();
    server
        .resource("about://scope")
        .handler(|_, _| async { Ok("scope".into()) })
        .unwrap();
    server
        .resource("users://{id}")
        .handler(|_, _| async { Ok("user".into()) })
        .unwrap();
    server
        .prompt("greet")
        .handler(|_, _| async { Ok(GetPromptResult::new(vec![])) })
        .unwrap();
    server
}

#[derive(Deserialize, JsonSchema)]
struct EmptyInput {}

#[tokio::test]
async fn configured_hints_cover_discovery_list_and_complete_resource_read_results() {
    let (client, task) = connect(configured(12_345, CacheScope::Public)).await;
    let peer = client.peer();
    let discovery = peer.discover(RequestMetaObject::new()).await.unwrap();
    assert_eq!(discovery.ttl_ms, 12_345);
    assert_eq!(discovery.cache_scope, CacheScope::Public);

    let tools = peer.list_tools(None).await.unwrap();
    assert_eq!(tools.ttl_ms, Some(12_345));
    assert_eq!(tools.cache_scope, Some(CacheScope::Public));
    let prompts = peer.list_prompts(None).await.unwrap();
    assert_eq!(prompts.ttl_ms, Some(12_345));
    assert_eq!(prompts.cache_scope, Some(CacheScope::Public));
    let resources = peer.list_resources(None).await.unwrap();
    assert_eq!(resources.ttl_ms, Some(12_345));
    assert_eq!(resources.cache_scope, Some(CacheScope::Public));
    let templates = peer.list_resource_templates(None).await.unwrap();
    assert_eq!(templates.ttl_ms, Some(12_345));
    assert_eq!(templates.cache_scope, Some(CacheScope::Public));

    let read = peer
        .read_resource(ReadResourceRequestParams::new("about://server"))
        .await
        .unwrap();
    assert_eq!(read.ttl_ms, Some(12_345));
    assert_eq!(read.cache_scope, Some(CacheScope::Public));
    close(client, task).await;
}

#[tokio::test]
async fn builder_policy_is_copy_on_write_and_live_replacement_is_visible() {
    let original = configured(10, CacheScope::Private);
    let clone = original.clone().with_result_cache(20, CacheScope::Public);
    let (client, task) = connect(original.clone()).await;
    let discovery = client
        .peer()
        .discover(RequestMetaObject::new())
        .await
        .unwrap();
    assert_eq!(discovery.ttl_ms, 10);
    assert_eq!(discovery.cache_scope, CacheScope::Private);
    close(client, task).await;
    let (client, task) = connect(clone).await;
    let discovery = client
        .peer()
        .discover(RequestMetaObject::new())
        .await
        .unwrap();
    assert_eq!(discovery.ttl_ms, 20);
    assert_eq!(discovery.cache_scope, CacheScope::Public);
    close(client, task).await;

    let live = original.into_live();
    let (client, task) = connect(live.clone()).await;
    // Observe server changes immediately rather than a still-fresh client snapshot.
    client
        .peer()
        .set_response_cache_config(mcp_rs::protocol::service::ClientCacheConfig::disabled())
        .await;
    let before = client
        .peer()
        .discover(RequestMetaObject::new())
        .await
        .unwrap();
    assert_eq!(before.ttl_ms, 10);
    assert_eq!(before.cache_scope, CacheScope::Private);
    let replacement = live.snapshot().with_result_cache(30, CacheScope::Public);
    live.replace(replacement);
    let discovery = client
        .peer()
        .discover(RequestMetaObject::new())
        .await
        .unwrap();
    assert_eq!(discovery.ttl_ms, 30);
    assert_eq!(discovery.cache_scope, CacheScope::Public);
    assert_eq!(
        client.peer().list_tools(None).await.unwrap().ttl_ms,
        Some(30)
    );
    close(client, task).await;
}

#[tokio::test]
async fn middleware_resource_hints_are_preserved_and_errors_and_other_results_stay_uncached() {
    let middleware: mcp_rs::Middleware = Arc::new(|ctx, args, next| {
        let uri = args["uri"].as_str().unwrap_or_default().to_owned();
        async move {
            match next(ctx, args).await? {
                Response::Resource(mut result) => {
                    match uri.as_str() {
                        "about://ttl" => result.ttl_ms = Some(77),
                        "about://scope" => result.cache_scope = Some(CacheScope::Private),
                        _ => {}
                    }
                    Ok(Response::Resource(result))
                }
                response => Ok(response),
            }
        }
        .boxed()
    });
    let (client, task) =
        connect(configured(12_345, CacheScope::Public).with_middleware(middleware)).await;
    let peer = client.peer();
    let read = peer
        .read_resource(ReadResourceRequestParams::new("about://ttl"))
        .await
        .unwrap();
    assert_eq!(read.ttl_ms, Some(77));
    assert_eq!(read.cache_scope, Some(CacheScope::Public));
    let read = peer
        .read_resource(ReadResourceRequestParams::new("about://scope"))
        .await
        .unwrap();
    assert_eq!(read.ttl_ms, Some(12_345));
    assert_eq!(read.cache_scope, Some(CacheScope::Private));

    let tool = peer
        .call_tool(CallToolRequestParams::new("hello").with_arguments(Default::default()))
        .await
        .unwrap();
    let tool_value = serde_json::to_value(tool).unwrap();
    assert!(tool_value.get("ttlMs").is_none());
    assert!(tool_value.get("cacheScope").is_none());
    let prompt = peer
        .get_prompt(GetPromptRequestParams::new("greet"))
        .await
        .unwrap();
    let prompt_value = serde_json::to_value(prompt).unwrap();
    assert!(prompt_value.get("ttlMs").is_none());
    assert!(prompt_value.get("cacheScope").is_none());
    assert!(
        peer.read_resource(ReadResourceRequestParams::new("missing://resource"))
            .await
            .is_err()
    );
    close(client, task).await;
}

#[tokio::test]
async fn sdk_client_reuses_private_results_and_partition_changes_refetch() {
    use mcp_rs::protocol::service::ClientCacheConfig;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let calls = Arc::new(AtomicUsize::new(0));
    let invoked = calls.clone();
    let mut server =
        Server::new("cache-isolation", "1").with_result_cache(60_000, CacheScope::Private);
    server
        .resource("counter://value")
        .handler(move |_, _| {
            let value = invoked.fetch_add(1, Ordering::SeqCst) + 1;
            async move { Ok(value.to_string()) }
        })
        .unwrap();
    let (client, task) = connect(server).await;
    let peer = client.peer();
    peer.set_response_cache_config(ClientCacheConfig::default().with_private_partition("alice"))
        .await;
    let params = ReadResourceRequestParams::new("counter://value");
    let first = peer.read_resource(params.clone()).await.unwrap();
    let second = peer.read_resource(params.clone()).await.unwrap();
    assert_eq!(first, second);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    peer.set_response_cache_config(ClientCacheConfig::default().with_private_partition("bob"))
        .await;
    let third = peer.read_resource(params.clone()).await.unwrap();
    assert_ne!(first.contents, third.contents);
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    peer.set_response_cache_config(ClientCacheConfig::disabled())
        .await;
    let _ = peer.read_resource(params).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    close(client, task).await;
}
