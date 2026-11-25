//! Enforce the framework protocol policy for caller-provided SDK handlers too.
use crate::{Context, model::*};
use mcp_protocol::{
    ErrorData, RoleServer, ServerHandler,
    service::{NotificationContext, SubscriptionContext},
};

#[derive(Clone)]
pub(super) struct Modern<H>(pub H);

macro_rules! forward_requests {
    ($($method:ident($parameter:ident: $input:ty) -> $output:ty;)*) => {
        $(async fn $method(&self, $parameter: $input, context: Context) -> Result<$output, ErrorData> {
            self.0.$method($parameter, context).await
        })*
    };
}
impl<H: ServerHandler> ServerHandler for Modern<H> {
    fn request_snapshot(&self) -> Option<Self> {
        self.0.request_snapshot().map(Self)
    }
    fn get_info(&self) -> ServerConfig {
        let mut info = self.0.get_info();
        info.protocol_version = ProtocolVersion::V_2026_07_28;
        info
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
    async fn discover(&self, context: Context) -> Result<DiscoverResult, ErrorData> {
        let mut discovery = self.0.discover(context).await?;
        discovery.supported_versions = vec![ProtocolVersion::V_2026_07_28];
        Ok(discovery)
    }
    forward_requests! {
        complete(request: CompleteRequestParams) -> CompleteResult;
        get_prompt(request: GetPromptRequestParams) -> GetPromptResponse;
        list_prompts(request: Option<PaginatedRequestParams>) -> ListPromptsResult;
        list_resources(request: Option<PaginatedRequestParams>) -> ListResourcesResult;
        list_resource_templates(request: Option<PaginatedRequestParams>) -> ListResourceTemplatesResult;
        read_resource(request: ReadResourceRequestParams) -> ReadResourceResponse;
        call_tool(request: CallToolRequestParams) -> CallToolResponse;
        list_tools(request: Option<ListToolsRequestParams>) -> ListToolsResult;
        get_task(request: GetTaskParams) -> GetTaskResult;
        update_task(request: UpdateTaskParams) -> ();
        cancel_task(request: CancelTaskParams) -> ();
    }
    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.0.get_tool(name)
    }
    fn accepted_subscription_filter(
        &self,
        requested: &SubscriptionFilter,
    ) -> Option<SubscriptionFilter> {
        self.0.accepted_subscription_filter(requested)
    }
    async fn listen(&self, context: SubscriptionContext) -> Result<(), ErrorData> {
        self.0.listen(context).await
    }
    async fn on_custom_request(
        &self,
        request: CustomRequest,
        context: Context,
    ) -> Result<CustomResult, ErrorData> {
        if matches!(
            request.method.as_str(),
            "tasks/list" | "tasks/result" | "elicitation/complete"
        ) {
            return Err(ErrorData::new(
                ErrorCode::METHOD_NOT_FOUND,
                "Retired method is not supported",
                None,
            ));
        }
        self.0.on_custom_request(request, context).await
    }
    async fn on_cancelled(
        &self,
        notification: CancelledNotificationParam,
        context: NotificationContext<RoleServer>,
    ) {
        self.0.on_cancelled(notification, context).await;
    }
    async fn on_progress(
        &self,
        notification: ProgressNotificationParam,
        context: NotificationContext<RoleServer>,
    ) {
        self.0.on_progress(notification, context).await;
    }
    async fn on_custom_notification(
        &self,
        notification: CustomNotification,
        context: NotificationContext<RoleServer>,
    ) {
        self.0.on_custom_notification(notification, context).await;
    }
}

/// Opt-in compatibility with protocol revisions before 2026-07-28, which
/// negotiate with `initialize`. Everything except negotiation behaves exactly
/// as [`Modern`]: retired methods stay rejected. Only
/// `serve_stdio_compatible` wraps a server in it; every default entry point
/// stays modern-only.
#[derive(Clone)]
pub(super) struct Compatible<H>(pub H);

impl<H: ServerHandler> ServerHandler for Compatible<H> {
    fn request_snapshot(&self) -> Option<Self> {
        self.0.request_snapshot().map(Self)
    }
    fn get_info(&self) -> ServerConfig {
        let mut info = self.0.get_info();
        info.protocol_version = ProtocolVersion::V_2026_07_28;
        info
    }
    fn supported_protocol_versions(&self) -> std::borrow::Cow<'static, [ProtocolVersion]> {
        std::borrow::Cow::Borrowed(ProtocolVersion::KNOWN_VERSIONS)
    }
    /// Negotiate a legacy revision: accept any known version that still has
    /// `initialize`, record the client, and answer with the inner server's
    /// info at the requested version.
    async fn initialize(
        &self,
        request: InitializeRequestParams,
        context: Context,
    ) -> Result<InitializeResult, ErrorData> {
        let version = request.protocol_version.clone();
        if !version.has_initialize() || !ProtocolVersion::KNOWN_VERSIONS.contains(&version) {
            return Err(ErrorData::unsupported_protocol_version(
                version,
                ProtocolVersion::known_up_to(&ProtocolVersion::LATEST_WITH_INITIALIZE),
            ));
        }
        context.peer.set_peer_info(request);
        let mut info = self.0.get_info();
        info.protocol_version = version;
        Ok(info)
    }
    async fn discover(&self, context: Context) -> Result<DiscoverResult, ErrorData> {
        let mut discovery = self.0.discover(context).await?;
        discovery.supported_versions = ProtocolVersion::KNOWN_VERSIONS.to_vec();
        Ok(discovery)
    }
    forward_requests! {
        complete(request: CompleteRequestParams) -> CompleteResult;
        get_prompt(request: GetPromptRequestParams) -> GetPromptResponse;
        list_prompts(request: Option<PaginatedRequestParams>) -> ListPromptsResult;
        list_resources(request: Option<PaginatedRequestParams>) -> ListResourcesResult;
        list_resource_templates(request: Option<PaginatedRequestParams>) -> ListResourceTemplatesResult;
        read_resource(request: ReadResourceRequestParams) -> ReadResourceResponse;
        call_tool(request: CallToolRequestParams) -> CallToolResponse;
        list_tools(request: Option<ListToolsRequestParams>) -> ListToolsResult;
        get_task(request: GetTaskParams) -> GetTaskResult;
        update_task(request: UpdateTaskParams) -> ();
        cancel_task(request: CancelTaskParams) -> ();
    }
    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.0.get_tool(name)
    }
    fn accepted_subscription_filter(
        &self,
        requested: &SubscriptionFilter,
    ) -> Option<SubscriptionFilter> {
        self.0.accepted_subscription_filter(requested)
    }
    async fn listen(&self, context: SubscriptionContext) -> Result<(), ErrorData> {
        self.0.listen(context).await
    }
    async fn on_custom_request(
        &self,
        request: CustomRequest,
        context: Context,
    ) -> Result<CustomResult, ErrorData> {
        if matches!(
            request.method.as_str(),
            "tasks/list" | "tasks/result" | "elicitation/complete"
        ) {
            return Err(ErrorData::new(
                ErrorCode::METHOD_NOT_FOUND,
                "Retired method is not supported",
                None,
            ));
        }
        self.0.on_custom_request(request, context).await
    }
    async fn on_cancelled(
        &self,
        notification: CancelledNotificationParam,
        context: NotificationContext<RoleServer>,
    ) {
        self.0.on_cancelled(notification, context).await;
    }
    async fn on_progress(
        &self,
        notification: ProgressNotificationParam,
        context: NotificationContext<RoleServer>,
    ) {
        self.0.on_progress(notification, context).await;
    }
    async fn on_custom_notification(
        &self,
        notification: CustomNotification,
        context: NotificationContext<RoleServer>,
    ) {
        self.0.on_custom_notification(notification, context).await;
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use mcp_protocol::{ClientLifecycleMode, ClientServiceExt, ServiceExt};
    struct SdkHandler;
    impl ServerHandler for SdkHandler {
        fn get_info(&self) -> ServerConfig {
            ServerConfig::default().with_protocol_version(ProtocolVersion::V_2025_11_25)
        }
    }
    #[tokio::test]
    async fn custom_handlers_cannot_reenable_initialize_or_advertise_older_revisions() {
        assert_eq!(
            Modern(SdkHandler).get_info().protocol_version,
            ProtocolVersion::V_2026_07_28
        );
        let (client_io, server_io) = tokio::io::duplex(65536);
        let job = tokio::spawn(async move {
            assert!(Modern(SdkHandler).serve(server_io).await.is_err());
        });
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        let (reader, mut writer) = tokio::io::split(client_io);
        let request = serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","clientInfo":{"name":"old","version":"1"},"capabilities":{}}});
        writer
            .write_all(format!("{request}\n").as_bytes())
            .await
            .unwrap();
        let line = BufReader::new(reader)
            .lines()
            .next_line()
            .await
            .unwrap()
            .unwrap();
        let response: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert!(response.get("error").is_some());
        job.await.unwrap();
        let (client_io, server_io) = tokio::io::duplex(65536);
        let job = tokio::spawn(async move {
            Modern(SdkHandler)
                .serve(server_io)
                .await
                .unwrap()
                .waiting()
                .await
                .unwrap();
        });
        let client = ClientConfig::default()
            .serve_with_lifecycle(
                client_io,
                ClientLifecycleMode::Discover {
                    preferred_versions: vec![ProtocolVersion::V_2026_07_28],
                },
            )
            .await
            .unwrap();
        let discovery = client.discover(RequestMetaObject::new()).await.unwrap();
        assert_eq!(
            discovery.supported_versions,
            vec![ProtocolVersion::V_2026_07_28]
        );
        let _ = client.list_tools(None).await;
        client.cancel().await.unwrap();
        job.await.unwrap();
    }

    async fn exchange(
        writer: &mut (impl tokio::io::AsyncWrite + Unpin),
        lines: &mut tokio::io::Lines<tokio::io::BufReader<impl tokio::io::AsyncRead + Unpin>>,
        message: serde_json::Value,
    ) -> serde_json::Value {
        use tokio::io::AsyncWriteExt;
        writer
            .write_all(format!("{message}\n").as_bytes())
            .await
            .unwrap();
        serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap()
    }

    #[tokio::test]
    async fn compatible_negotiates_legacy_initialize_and_keeps_retired_methods_rejected() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        let (client_io, server_io) = tokio::io::duplex(65536);
        let job = tokio::spawn(async move {
            let running = Compatible(SdkHandler).serve(server_io).await.unwrap();
            let _ = running.waiting().await;
        });
        let (reader, mut writer) = tokio::io::split(client_io);
        let mut lines = BufReader::new(reader).lines();
        let init = exchange(&mut writer, &mut lines, serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","clientInfo":{"name":"old","version":"1"},"capabilities":{}}})).await;
        assert_eq!(init["result"]["protocolVersion"], "2025-06-18", "{init}");
        writer
            .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
            .await
            .unwrap();
        let tools = exchange(
            &mut writer,
            &mut lines,
            serde_json::json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
        )
        .await;
        assert!(tools.get("result").is_some(), "{tools}");
        let retired = exchange(
            &mut writer,
            &mut lines,
            serde_json::json!({"jsonrpc":"2.0","id":3,"method":"tasks/list"}),
        )
        .await;
        assert!(retired.get("error").is_some(), "{retired}");
        drop((writer, lines));
        tokio::time::timeout(std::time::Duration::from_secs(5), job)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn compatible_refuses_an_unknown_revision() {
        use tokio::io::{AsyncBufReadExt, BufReader};
        let (client_io, server_io) = tokio::io::duplex(65536);
        let job = tokio::spawn(async move {
            assert!(Compatible(SdkHandler).serve(server_io).await.is_err());
        });
        let (reader, mut writer) = tokio::io::split(client_io);
        let mut lines = BufReader::new(reader).lines();
        let reply = exchange(&mut writer, &mut lines, serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2023-01-01","clientInfo":{"name":"old","version":"1"},"capabilities":{}}})).await;
        assert!(reply.get("error").is_some(), "{reply}");
        tokio::time::timeout(std::time::Duration::from_secs(5), job)
            .await
            .unwrap()
            .unwrap();
    }

    #[test]
    fn compatible_advertises_every_known_revision_and_modern_only_the_current_one() {
        assert_eq!(
            Compatible(SdkHandler)
                .supported_protocol_versions()
                .as_ref(),
            ProtocolVersion::KNOWN_VERSIONS
        );
        assert_eq!(
            Modern(SdkHandler).supported_protocol_versions().as_ref(),
            &[ProtocolVersion::V_2026_07_28]
        );
    }
}
