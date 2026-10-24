//! List-all helpers bound untrusted pagination across every native catalog.
#![cfg(feature = "client")]
#![allow(clippy::unwrap_used)]

use mcp_rs::{
    Client,
    model::*,
    protocol::{
        RoleClient, RoleServer, ServerHandler, ServiceExt,
        service::{ClientCacheConfig, PaginationLimits, Peer, RequestContext, ServiceError},
    },
};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Clone, Copy)]
enum Mode {
    Cycle,
    Finite,
    Empty,
}
type CatalogCalls = Arc<Mutex<Vec<(String, Option<String>)>>>;

#[derive(Clone)]
struct Catalog {
    mode: Mode,
    calls: CatalogCalls,
}
impl Catalog {
    fn page(&self, kind: &str, cursor: Option<String>) -> Value {
        self.calls
            .lock()
            .unwrap()
            .push((kind.to_owned(), cursor.clone()));
        let item = match kind {
            "tools" => json!({"name":"tool","inputSchema":{"type":"object"}}),
            "prompts" => json!({"name":"prompt"}),
            "resources" => json!({"name":"resource","uri":"data:value"}),
            _ => json!({"name":"template","uriTemplate":"data:{id}"}),
        };
        let next = match self.mode {
            Mode::Cycle => Some("loop".to_owned()),
            Mode::Finite => {
                if cursor.is_none() {
                    Some("second".to_owned())
                } else {
                    None
                }
            }
            Mode::Empty => Some(self.calls.lock().unwrap().len().to_string()),
        };
        let mut result = json!({"nextCursor":next,"ttlMs":60000,"cacheScope":"public"});
        result[kind] = if matches!(self.mode, Mode::Empty) {
            json!([])
        } else {
            json!([item])
        };
        result
    }
}
impl ServerHandler for Catalog {
    async fn list_tools(
        &self,
        params: Option<ListToolsRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, mcp_rs::protocol::ErrorData> {
        let params = params.unwrap_or_default();
        if !params.group.is_empty() {
            assert_eq!(params.group, "research");
            assert_eq!(params.detail, Some(ToolListDetail::Names));
            assert_eq!(params.tags, vec!["read"]);
        }
        Ok(serde_json::from_value(self.page("tools", params.cursor)).unwrap())
    }
    async fn list_prompts(
        &self,
        params: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListPromptsResult, mcp_rs::protocol::ErrorData> {
        Ok(serde_json::from_value(self.page("prompts", params.and_then(|p| p.cursor))).unwrap())
    }
    async fn list_resources(
        &self,
        params: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, mcp_rs::protocol::ErrorData> {
        Ok(serde_json::from_value(self.page("resources", params.and_then(|p| p.cursor))).unwrap())
    }
    async fn list_resource_templates(
        &self,
        params: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, mcp_rs::protocol::ErrorData> {
        Ok(
            serde_json::from_value(self.page("resourceTemplates", params.and_then(|p| p.cursor)))
                .unwrap(),
        )
    }
}
async fn connect(mode: Mode) -> (Client, Catalog, tokio::task::JoinHandle<()>) {
    let catalog = Catalog {
        mode,
        calls: Arc::default(),
    };
    let server = catalog.clone();
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
    (Client::connect(client_io).await.unwrap(), catalog, task)
}
async fn close(client: Client, task: tokio::task::JoinHandle<()>) {
    client.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
}
async fn collect(
    peer: &Peer<RoleClient>,
    kind: usize,
    limits: Option<PaginationLimits>,
) -> Result<usize, ServiceError> {
    match (kind, limits) {
        (0, None) => peer.list_all_tools().await.map(|v| v.len()),
        (0, Some(limits)) => peer
            .list_all_tools_with_limits(ListToolsRequestParams::default(), limits)
            .await
            .map(|v| v.len()),
        (1, None) => peer.list_all_prompts().await.map(|v| v.len()),
        (1, Some(limits)) => peer
            .list_all_prompts_with_limits(limits)
            .await
            .map(|v| v.len()),
        (2, None) => peer.list_all_resources().await.map(|v| v.len()),
        (2, Some(limits)) => peer
            .list_all_resources_with_limits(limits)
            .await
            .map(|v| v.len()),
        (_, None) => peer.list_all_resource_templates().await.map(|v| v.len()),
        (_, Some(limits)) => peer
            .list_all_resource_templates_with_limits(limits)
            .await
            .map(|v| v.len()),
    }
}

#[tokio::test]
async fn default_collectors_reject_cycles_and_leave_client_usable() {
    let (client, catalog, task) = connect(Mode::Cycle).await;
    for kind in 0..4 {
        let error =
            tokio::time::timeout(Duration::from_secs(1), collect(client.peer(), kind, None))
                .await
                .unwrap()
                .unwrap_err();
        assert!(matches!(error, ServiceError::PaginationCursorCycle));
    }
    assert_eq!(catalog.calls.lock().unwrap().len(), 8);
    assert_eq!(client.peer().list_tools(None).await.unwrap().tools.len(), 1);
    close(client, task).await;
}

#[tokio::test]
async fn all_catalogs_enforce_page_and_item_limits_with_exact_boundary_success() {
    let (client, catalog, task) = connect(Mode::Finite).await;
    client
        .peer()
        .set_response_cache_config(ClientCacheConfig::disabled())
        .await;
    for kind in 0..4 {
        let before = catalog.calls.lock().unwrap().len();
        let error = collect(
            client.peer(),
            kind,
            Some(PaginationLimits::default().with_max_pages(1)),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            ServiceError::PaginationPageLimitExceeded { limit: 1 }
        ));
        assert_eq!(catalog.calls.lock().unwrap().len(), before + 1);
        let error = collect(
            client.peer(),
            kind,
            Some(PaginationLimits::default().with_max_items(1)),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            ServiceError::PaginationItemLimitExceeded { limit: 1 }
        ));
        assert_eq!(
            collect(
                client.peer(),
                kind,
                Some(
                    PaginationLimits::default()
                        .with_max_pages(2)
                        .with_max_items(2)
                )
            )
            .await
            .unwrap(),
            2
        );
    }
    close(client, task).await;
}

#[tokio::test]
async fn empty_unique_cursors_are_bounded_and_zero_sends_no_requests() {
    let (client, catalog, task) = connect(Mode::Empty).await;
    for kind in 0..4 {
        let error = collect(
            client.peer(),
            kind,
            Some(PaginationLimits::default().with_max_pages(0)),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            ServiceError::PaginationPageLimitExceeded { limit: 0 }
        ));
    }
    assert!(catalog.calls.lock().unwrap().is_empty());
    for kind in 0..4 {
        let error = collect(
            client.peer(),
            kind,
            Some(
                PaginationLimits::default()
                    .with_max_pages(3)
                    .with_max_items(0),
            ),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            ServiceError::PaginationPageLimitExceeded { limit: 3 }
        ));
    }
    assert_eq!(catalog.calls.lock().unwrap().len(), 12);
    close(client, task).await;
}

#[tokio::test]
async fn tool_filters_and_resume_cursor_survive_explicit_limits() {
    let (client, catalog, task) = connect(Mode::Finite).await;
    let params =
        serde_json::from_value(json!({"group":"research","tags":["read"],"detail":"names"}))
            .unwrap();
    assert_eq!(
        client
            .peer()
            .list_all_tools_with_limits(params, PaginationLimits::default().with_max_pages(2))
            .await
            .unwrap()
            .len(),
        2
    );
    let params = serde_json::from_value(
        json!({"group":"research","tags":["read"],"detail":"names","cursor":"second"}),
    )
    .unwrap();
    assert_eq!(
        client
            .peer()
            .list_all_tools_with_limits(
                params,
                PaginationLimits::default()
                    .with_max_pages(1)
                    .with_max_items(1)
            )
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        catalog
            .calls
            .lock()
            .unwrap()
            .iter()
            .map(|(_, c)| c.as_deref())
            .collect::<Vec<_>>(),
        vec![None, Some("second")]
    );
    close(client, task).await;
}
