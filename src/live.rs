//! Atomic runtime registry replacement, with explicit per-session notifications.
use crate::{Context, Server, model::*};
use arc_swap::ArcSwap;
use mcp_protocol::{ErrorData, RoleServer, ServerHandler, service::ServiceError};
use std::sync::Arc;

/// A live server shared by all transport sessions.
///
/// Build changes on a snapshot, then atomically replace the configuration. In-flight
/// requests retain their old snapshot. Replacement is last-writer-wins; callers
/// coordinating concurrent edits must serialize their configuration changes.
/// Stateless HTTP captures one registry snapshot before routing admission and
/// retains it through dispatch. Application state and task/event managers inside
/// the configuration remain shared; snapshotting does not roll back their effects.
#[derive(Clone)]
pub struct LiveServer {
    state: Arc<ArcSwap<Server>>,
}
impl std::fmt::Debug for LiveServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveServer")
            .field("configuration", &self.state.load())
            .finish()
    }
}
impl LiveServer {
    /// Create a shared runtime registry from a configured server.
    pub fn new(server: Server) -> Self {
        Self {
            state: Arc::new(ArcSwap::from_pointee(server)),
        }
    }
    /// Obtain a cheap configuration snapshot for editing or inspection.
    pub fn snapshot(&self) -> Server {
        self.state.load().as_ref().clone()
    }
    /// Replace the configuration atomically; in-flight requests retain their snapshot.
    pub fn replace(&self, server: Server) {
        self.state.store(Arc::new(server));
    }
    /// Notify one connected peer that all three registry lists may have changed.
    ///
    /// Changes remain committed if notification delivery fails. Call this for each
    /// affected session; this registry does not retain connections or spawn watchers.
    /// # Errors
    /// Returns an SDK notification delivery failure.
    pub async fn notify_changed(
        &self,
        peer: &mcp_protocol::Peer<RoleServer>,
    ) -> Result<(), ServiceError> {
        peer.notify_tool_list_changed().await?;
        peer.notify_resource_list_changed().await?;
        peer.notify_prompt_list_changed().await?;
        Ok(())
    }
}
impl ServerHandler for LiveServer {
    fn request_snapshot(&self) -> Option<Self> {
        // The HTTP transport retains this isolated registry through routing
        // admission and dispatch. Application state and managers remain shared.
        Some(Self {
            state: Arc::new(ArcSwap::from(self.state.load_full())),
        })
    }
    fn get_info(&self) -> ServerConfig {
        let mut info = self.state.load().get_info();
        mark_lists_changed(&mut info.capabilities);
        info
    }
    async fn discover(&self, _: Context) -> Result<DiscoverResult, ErrorData> {
        let mut result = self.state.load_full().discovery()?;
        mark_lists_changed(&mut result.capabilities);
        Ok(result)
    }
    fn supported_protocol_versions(&self) -> std::borrow::Cow<'static, [ProtocolVersion]> {
        std::borrow::Cow::Borrowed(&[ProtocolVersion::V_2026_07_28])
    }
    async fn initialize(
        &self,
        _: InitializeRequestParams,
        _: Context,
    ) -> Result<InitializeResult, ErrorData> {
        Err(ErrorData::invalid_request(
            "Use server/discover with MCP 2026-07-28",
            None,
        ))
    }
    fn accepted_subscription_filter(
        &self,
        requested: &SubscriptionFilter,
    ) -> Option<SubscriptionFilter> {
        self.state.load().accepted_subscription_filter(requested)
    }
    async fn listen(
        &self,
        context: mcp_protocol::service::SubscriptionContext,
    ) -> Result<(), ErrorData> {
        self.state.load_full().listen(context).await
    }
    async fn on_custom_request(
        &self,
        request: CustomRequest,
        context: Context,
    ) -> Result<CustomResult, ErrorData> {
        self.state
            .load_full()
            .on_custom_request(request, context)
            .await
    }
    async fn get_task(
        &self,
        request: GetTaskParams,
        context: Context,
    ) -> Result<GetTaskResult, ErrorData> {
        self.state.load_full().get_task(request, context).await
    }
    async fn update_task(
        &self,
        request: UpdateTaskParams,
        context: Context,
    ) -> Result<(), ErrorData> {
        self.state.load_full().update_task(request, context).await
    }
    async fn cancel_task(
        &self,
        request: CancelTaskParams,
        context: Context,
    ) -> Result<(), ErrorData> {
        self.state.load_full().cancel_task(request, context).await
    }
    async fn complete(
        &self,
        request: CompleteRequestParams,
        context: Context,
    ) -> Result<CompleteResult, ErrorData> {
        self.state.load_full().complete(request, context).await
    }
    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.state.load().get_tool(name)
    }
    async fn list_tools(
        &self,
        request: Option<ListToolsRequestParams>,
        context: Context,
    ) -> Result<ListToolsResult, ErrorData> {
        self.state.load_full().list_tools(request, context).await
    }
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: Context,
    ) -> Result<CallToolResponse, ErrorData> {
        self.state.load_full().call_tool(request, context).await
    }
    async fn list_resources(
        &self,
        request: Option<PaginatedRequestParams>,
        context: Context,
    ) -> Result<ListResourcesResult, ErrorData> {
        self.state
            .load_full()
            .list_resources(request, context)
            .await
    }
    async fn list_resource_templates(
        &self,
        request: Option<PaginatedRequestParams>,
        context: Context,
    ) -> Result<ListResourceTemplatesResult, ErrorData> {
        self.state
            .load_full()
            .list_resource_templates(request, context)
            .await
    }
    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: Context,
    ) -> Result<ReadResourceResponse, ErrorData> {
        self.state.load_full().read_resource(request, context).await
    }
    async fn list_prompts(
        &self,
        request: Option<PaginatedRequestParams>,
        context: Context,
    ) -> Result<ListPromptsResult, ErrorData> {
        self.state.load_full().list_prompts(request, context).await
    }
    async fn get_prompt(
        &self,
        request: GetPromptRequestParams,
        context: Context,
    ) -> Result<GetPromptResponse, ErrorData> {
        self.state.load_full().get_prompt(request, context).await
    }
}

fn mark_lists_changed(capabilities: &mut ServerCapabilities) {
    capabilities
        .tools
        .get_or_insert_with(Default::default)
        .list_changed = Some(true);
    capabilities
        .resources
        .get_or_insert_with(Default::default)
        .list_changed = Some(true);
    capabilities
        .prompts
        .get_or_insert_with(Default::default)
        .list_changed = Some(true);
}
