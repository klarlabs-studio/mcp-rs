//! Client cache refresh, notification invalidation and in-flight write isolation.
#![cfg(feature = "client")]
#![allow(clippy::unwrap_used)]

use mcp_rs::{
    Client, Server,
    model::*,
    protocol::{
        ClientHandler, ClientLifecycleMode, ClientServiceExt, RoleClient, RoleServer,
        ServerHandler, ServiceExt,
        service::{ClientCacheConfig, NotificationContext, Peer},
    },
};
use schemars::JsonSchema;
use serde::Deserialize;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Semaphore, mpsc, oneshot};

const DEADLINE: Duration = Duration::from_secs(3);

struct Recorder(mpsc::UnboundedSender<&'static str>);
impl ClientHandler for Recorder {
    async fn on_resource_updated(
        &self,
        _: ResourceUpdatedNotificationParam,
        _: NotificationContext<RoleClient>,
    ) {
        let _ = self.0.send("updated");
    }
    async fn on_tool_list_changed(&self, _: NotificationContext<RoleClient>) {
        let _ = self.0.send("tools");
    }
    async fn on_resource_list_changed(&self, _: NotificationContext<RoleClient>) {
        let _ = self.0.send("resources");
    }
    async fn on_prompt_list_changed(&self, _: NotificationContext<RoleClient>) {
        let _ = self.0.send("prompts");
    }
}

struct Connection {
    client: Client<Recorder>,
    server_peer: Peer<RoleServer>,
    events: mpsc::UnboundedReceiver<&'static str>,
    task: tokio::task::JoinHandle<()>,
}
impl Connection {
    async fn new(server: impl ServerHandler) -> Self {
        tokio::time::timeout(DEADLINE, async {
            let (client_io, server_io) = tokio::io::duplex(65536);
            let (peer_tx, peer_rx) = oneshot::channel();
            let task = tokio::spawn(async move {
                let running = server.serve(server_io).await.unwrap();
                peer_tx.send(running.peer().clone()).unwrap();
                running.waiting().await.unwrap();
            });
            let (tx, events) = mpsc::unbounded_channel();
            let running = Recorder(tx)
                .serve_with_lifecycle(
                    client_io,
                    ClientLifecycleMode::Discover {
                        preferred_versions: vec![ProtocolVersion::V_2026_07_28],
                    },
                )
                .await
                .unwrap();
            // Server::serve completes admission when the first ordinary request arrives.
            running.list_tools(None).await.unwrap();
            running.clear_response_cache().await;
            Self {
                client: Client::from_service(running).unwrap(),
                server_peer: peer_rx.await.unwrap(),
                events,
                task,
            }
        })
        .await
        .unwrap()
    }

    async fn event(&mut self, expected: &str) {
        // Cache invalidation precedes the application notification callback.
        assert_eq!(
            tokio::time::timeout(DEADLINE, self.events.recv())
                .await
                .unwrap(),
            Some(expected)
        );
    }

    async fn close(self) {
        self.client.close().await.unwrap();
        tokio::time::timeout(DEADLINE, self.task)
            .await
            .unwrap()
            .unwrap();
    }
}

struct ResourceState {
    calls: AtomicUsize,
    payload_bytes: AtomicUsize,
    ttl: AtomicU64,
    fail: AtomicBool,
    block_next: AtomicBool,
    entered: Semaphore,
    release: Semaphore,
}
impl ResourceState {
    fn new(ttl: u64) -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            payload_bytes: AtomicUsize::new(0),
            ttl: AtomicU64::new(ttl),
            fail: AtomicBool::new(false),
            block_next: AtomicBool::new(false),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        })
    }
}

fn resource_server(state: Arc<ResourceState>) -> Server {
    let mut server =
        Server::new("cache-lifecycle", "1").with_result_cache(60_000, CacheScope::Private);
    server
        .resource("counter://{id}")
        .protocol_handler(move |_, uri, _| {
            let state = state.clone();
            async move {
                let value = state.calls.fetch_add(1, Ordering::SeqCst) + 1;
                let ttl = state.ttl.load(Ordering::SeqCst);
                if state.fail.load(Ordering::SeqCst) {
                    return Err(ErrorData::internal_error("refresh failed", None));
                }
                if state.block_next.swap(false, Ordering::SeqCst) {
                    state.entered.add_permits(1);
                    state.release.acquire().await.unwrap().forget();
                }
                let mut result = ReadResourceResult::new(vec![ResourceContents::text(
                    format!(
                        "{value}{}",
                        "x".repeat(state.payload_bytes.load(Ordering::SeqCst))
                    ),
                    uri,
                )]);
                result.ttl_ms = Some(ttl);
                result.cache_scope = Some(CacheScope::Private);
                Ok(result.into())
            }
        })
        .unwrap();
    server
}

async fn read(peer: &Peer<RoleClient>, uri: &str) -> ReadResourceResult {
    tokio::time::timeout(
        DEADLINE,
        peer.read_resource(ReadResourceRequestParams::new(uri)),
    )
    .await
    .unwrap()
    .unwrap()
}

#[tokio::test]
async fn successful_zero_ttl_refresh_retires_the_old_stale_fallback() {
    let state = ResourceState::new(1);
    let connection = Connection::new(resource_server(state.clone())).await;
    let peer = connection.client.peer();
    let first = read(peer, "counter://one").await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    state.ttl.store(0, Ordering::SeqCst);
    let refreshed = read(peer, "counter://one").await;
    assert_ne!(first.contents, refreshed.contents);
    state.fail.store(true, Ordering::SeqCst);
    let result = peer
        .read_resource(ReadResourceRequestParams::new("counter://one"))
        .await;
    assert!(
        result.is_err(),
        "a successful uncacheable refresh must retire the earlier value"
    );
    assert_eq!(state.calls.load(Ordering::SeqCst), 3);
    connection.close().await;
}

#[tokio::test]
async fn expired_results_fall_back_on_error_only_when_enabled_and_recover_on_success() {
    let state = ResourceState::new(1);
    let connection = Connection::new(resource_server(state.clone())).await;
    let peer = connection.client.peer();
    let first = read(peer, "counter://one").await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    state.fail.store(true, Ordering::SeqCst);
    assert_eq!(read(peer, "counter://one").await, first);
    assert_eq!(
        state.calls.load(Ordering::SeqCst),
        2,
        "stale data requires an attempted refresh"
    );
    state.fail.store(false, Ordering::SeqCst);
    state.ttl.store(60_000, Ordering::SeqCst);
    let fresh = read(peer, "counter://one").await;
    assert_ne!(fresh.contents, first.contents);
    assert_eq!(read(peer, "counter://one").await, fresh);
    assert_eq!(state.calls.load(Ordering::SeqCst), 3);

    peer.clear_response_cache().await;
    state.ttl.store(1, Ordering::SeqCst);
    read(peer, "counter://one").await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    peer.set_response_cache_config(ClientCacheConfig::default().with_serve_stale_on_error(false))
        .await;
    state.fail.store(true, Ordering::SeqCst);
    assert!(
        peer.read_resource(ReadResourceRequestParams::new("counter://one"))
            .await
            .is_err()
    );
    assert_eq!(state.calls.load(Ordering::SeqCst), 5);
    connection.close().await;
}

#[tokio::test]
async fn resource_notifications_invalidate_only_the_exact_uri_and_remove_stale_fallbacks() {
    let state = ResourceState::new(60_000);
    let mut connection = Connection::new(resource_server(state.clone())).await;
    let first = read(connection.client.peer(), "counter://one").await;
    let other = read(connection.client.peer(), "counter://one-more").await;
    connection
        .server_peer
        .notify_resource_updated(ResourceUpdatedNotificationParam::new("counter://one"))
        .await
        .unwrap();
    connection.event("updated").await;
    assert_eq!(
        read(connection.client.peer(), "counter://one-more").await,
        other
    );
    let refreshed = read(connection.client.peer(), "counter://one").await;
    assert_ne!(refreshed.contents, first.contents);
    assert_eq!(state.calls.load(Ordering::SeqCst), 3);
    connection
        .server_peer
        .notify_resource_updated(ResourceUpdatedNotificationParam::new("counter://one"))
        .await
        .unwrap();
    connection.event("updated").await;
    state.fail.store(true, Ordering::SeqCst);
    assert!(
        connection
            .client
            .peer()
            .read_resource(ReadResourceRequestParams::new("counter://one"))
            .await
            .is_err()
    );
    assert_eq!(
        read(connection.client.peer(), "counter://one-more").await,
        other
    );
    connection.close().await;
}

#[tokio::test]
async fn bounded_cache_evicts_oldest_entries_and_honors_a_reduced_limit() {
    let state = ResourceState::new(60_000);
    let connection = Connection::new(resource_server(state.clone())).await;
    let peer = connection.client.peer();
    peer.set_response_cache_config(ClientCacheConfig::default().with_max_entries(2))
        .await;
    peer.clear_response_cache().await;
    let first = read(peer, "counter://one").await;
    let second = read(peer, "counter://two").await;
    let third = read(peer, "counter://three").await;
    assert_eq!(read(peer, "counter://two").await, second);
    assert_eq!(read(peer, "counter://three").await, third);
    assert_eq!(state.calls.load(Ordering::SeqCst), 3);
    assert_ne!(read(peer, "counter://one").await.contents, first.contents);
    peer.set_response_cache_config(ClientCacheConfig::default().with_max_entries(1))
        .await;
    let current = read(peer, "counter://one").await;
    assert_eq!(state.calls.load(Ordering::SeqCst), 4);
    assert_eq!(read(peer, "counter://one").await, current);
    read(peer, "counter://three").await;
    assert_eq!(state.calls.load(Ordering::SeqCst), 5);
    connection.close().await;
}

#[tokio::test]
async fn invalidation_and_identity_changes_prevent_in_flight_responses_repopulating_cache() {
    for action in [
        "notification",
        "partition",
        "clear",
        "disable",
        "byte-budget",
    ] {
        let state = ResourceState::new(60_000);
        state.block_next.store(true, Ordering::SeqCst);
        let mut connection = Connection::new(resource_server(state.clone())).await;
        let peer = connection.client.peer().clone();
        peer.set_response_cache_config(
            ClientCacheConfig::default().with_private_partition("alice"),
        )
        .await;
        let request_peer = peer.clone();
        let pending = tokio::spawn(async move { read(&request_peer, "counter://one").await });
        tokio::time::timeout(DEADLINE, state.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        match action {
            "notification" => {
                connection
                    .server_peer
                    .notify_resource_updated(ResourceUpdatedNotificationParam::new("counter://one"))
                    .await
                    .unwrap();
                connection.event("updated").await;
            }
            "partition" => {
                peer.set_response_cache_config(
                    ClientCacheConfig::default().with_private_partition("bob"),
                )
                .await
            }
            "byte-budget" => {
                peer.set_response_cache_config(
                    ClientCacheConfig::default().with_max_total_bytes(4096),
                )
                .await;
            }
            "clear" => peer.clear_response_cache().await,
            "disable" => {
                peer.set_response_cache_config(ClientCacheConfig::disabled())
                    .await
            }
            _ => unreachable!(),
        }
        state.release.add_permits(1);
        let old = tokio::time::timeout(DEADLINE, pending)
            .await
            .unwrap()
            .unwrap();
        if action == "disable" {
            peer.set_response_cache_config(ClientCacheConfig::default())
                .await;
        }
        let fresh = read(&peer, "counter://one").await;
        assert_ne!(old.contents, fresh.contents, "{action}");
        assert_eq!(read(&peer, "counter://one").await, fresh, "{action}");
        assert_eq!(state.calls.load(Ordering::SeqCst), 2, "{action}");
        connection.close().await;
    }
}

#[derive(Deserialize, JsonSchema)]
struct Empty {}

#[tokio::test]
async fn list_notifications_invalidate_all_tool_queries_and_pages_but_preserve_other_families() {
    let state = ResourceState::new(60_000);
    let mut server = resource_server(state.clone());
    for i in 0..102 {
        server
            .tool(format!("item_{i:03}"))
            .group("inventory")
            .tags(["read"])
            .handler(|_, _: Empty| async { Ok("ok") })
            .unwrap();
    }
    server
        .resource("static://one")
        .handler(|_, _| async { Ok("one".into()) })
        .unwrap();
    server
        .prompt("greet")
        .handler(|_, _| async { Ok(GetPromptResult::new(vec![])) })
        .unwrap();
    let live = server.into_live();
    let mut connection = Connection::new(live.clone()).await;
    let peer = connection.client.peer().clone();
    let params = |value| serde_json::from_value::<ListToolsRequestParams>(value).unwrap();
    let queries = [
        None,
        Some(params(
            serde_json::json!({"group":"inventory", "tags":["read"], "detail":"names"}),
        )),
        Some(params(
            serde_json::json!({"group":"inventory", "detail":"full"}),
        )),
    ];
    for query in &queries {
        assert_eq!(
            peer.list_tools(query.clone()).await.unwrap().tools[0].name,
            "item_000"
        );
    }
    let page = Some(params(
        serde_json::json!({"cursor":"item_099", "detail":"names"}),
    ));
    assert_eq!(peer.list_tools(page.clone()).await.unwrap().tools.len(), 2);
    let old_resource = read(&peer, "counter://one").await;
    assert_eq!(peer.list_prompts(None).await.unwrap().prompts.len(), 1);
    assert_eq!(peer.list_resources(None).await.unwrap().resources.len(), 1);
    assert_eq!(
        peer.list_resource_templates(None)
            .await
            .unwrap()
            .resource_templates
            .len(),
        1
    );

    let mut replacement = live.snapshot();
    assert!(replacement.remove_tool("item_000"));
    assert!(replacement.remove_tool("item_101"));
    assert!(replacement.remove_prompt("greet"));
    assert!(replacement.remove_resource("static://one"));
    assert!(replacement.remove_resource("counter://{id}"));
    live.replace(replacement);
    connection
        .server_peer
        .notify_tool_list_changed()
        .await
        .unwrap();
    connection.event("tools").await;
    for query in queries {
        assert_eq!(
            peer.list_tools(query).await.unwrap().tools[0].name,
            "item_001"
        );
    }
    assert_eq!(peer.list_tools(page).await.unwrap().tools.len(), 1);
    assert_eq!(peer.list_prompts(None).await.unwrap().prompts.len(), 1);
    assert_eq!(peer.list_resources(None).await.unwrap().resources.len(), 1);
    assert_eq!(
        peer.list_resource_templates(None)
            .await
            .unwrap()
            .resource_templates
            .len(),
        1
    );
    assert_eq!(read(&peer, "counter://one").await, old_resource);
    connection
        .server_peer
        .notify_prompt_list_changed()
        .await
        .unwrap();
    connection.event("prompts").await;
    assert!(peer.list_prompts(None).await.unwrap().prompts.is_empty());
    connection
        .server_peer
        .notify_resource_list_changed()
        .await
        .unwrap();
    connection.event("resources").await;
    assert!(
        peer.list_resources(None)
            .await
            .unwrap()
            .resources
            .is_empty()
    );
    assert!(
        peer.list_resource_templates(None)
            .await
            .unwrap()
            .resource_templates
            .is_empty()
    );
    // List changes do not revoke independent resource-read cache entries.
    assert_eq!(read(&peer, "counter://one").await, old_resource);
    assert_eq!(state.calls.load(Ordering::SeqCst), 1);
    connection.close().await;
}

#[tokio::test]
async fn cache_byte_budget_evicts_oldest_shrinks_and_zero_disables_retention() {
    let state = ResourceState::new(60_000);
    let connection = Connection::new(resource_server(state.clone())).await;
    let peer = connection.client.peer();
    state.payload_bytes.store(256, Ordering::SeqCst);
    // Size from an actual decoded protocol result, including the logical key.
    let first = read(peer, "counter://one").await;
    let encoded = serde_json::to_vec(&ServerResult::ReadResourceResult(first.clone()))
        .unwrap()
        .len();
    let bytes = r#"resources/read:"counter://one":"#.len() + encoded;
    peer.clear_response_cache().await;
    peer.set_response_cache_config(
        ClientCacheConfig::default()
            .with_max_entries(0)
            .with_max_total_bytes(bytes * 2),
    )
    .await;
    let one = read(peer, "counter://one").await;
    tokio::time::sleep(Duration::from_millis(2)).await;
    let two = read(peer, "counter://two").await;
    assert_eq!(read(peer, "counter://one").await, one);
    tokio::time::sleep(Duration::from_millis(2)).await;
    let six = read(peer, "counter://six").await;
    assert_eq!(read(peer, "counter://two").await, two);
    assert_eq!(read(peer, "counter://six").await, six);
    assert_eq!(state.calls.load(Ordering::SeqCst), 4);
    assert_ne!(read(peer, "counter://one").await, one);
    assert_eq!(state.calls.load(Ordering::SeqCst), 5);

    peer.set_response_cache_config(ClientCacheConfig::default().with_max_total_bytes(bytes))
        .await;
    let current = read(peer, "counter://one").await;
    assert_eq!(read(peer, "counter://one").await, current);
    assert_eq!(state.calls.load(Ordering::SeqCst), 5, "exact budget fits");
    peer.set_response_cache_config(ClientCacheConfig::default().with_max_total_bytes(bytes - 1))
        .await;
    read(peer, "counter://one").await;
    read(peer, "counter://one").await;
    assert_eq!(
        state.calls.load(Ordering::SeqCst),
        7,
        "one byte over is not retained"
    );
    peer.set_response_cache_config(ClientCacheConfig::default().with_max_total_bytes(0))
        .await;
    assert_eq!(peer.response_cache_config().await.max_total_bytes, 0);
    read(peer, "counter://one").await;
    read(peer, "counter://one").await;
    assert_eq!(state.calls.load(Ordering::SeqCst), 9);
    connection.close().await;
}

#[tokio::test]
async fn oversized_refresh_retires_stale_value_and_preserves_unrelated_entries() {
    let state = ResourceState::new(60_000);
    let connection = Connection::new(resource_server(state.clone())).await;
    let peer = connection.client.peer();
    peer.set_response_cache_config(ClientCacheConfig::default().with_max_total_bytes(1024))
        .await;
    let other = read(peer, "counter://other").await;
    state.ttl.store(1, Ordering::SeqCst);
    read(peer, "counter://one").await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    state.payload_bytes.store(2048, Ordering::SeqCst);
    let oversized = read(peer, "counter://one").await;
    assert_ne!(
        oversized.contents, other.contents,
        "oversized result is still delivered"
    );
    assert_eq!(read(peer, "counter://other").await, other);
    assert_eq!(state.calls.load(Ordering::SeqCst), 3);
    state.fail.store(true, Ordering::SeqCst);
    assert!(
        peer.read_resource(ReadResourceRequestParams::new("counter://one"))
            .await
            .is_err(),
        "superseded stale data must not reappear after an uncacheable success"
    );
    assert_eq!(read(peer, "counter://other").await, other);
    connection.close().await;
}
