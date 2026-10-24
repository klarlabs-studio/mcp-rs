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
}
