//! Typed MCP client and reusable tool handles on the owned protocol engine.

use crate::model::{CallToolRequestParams, CallToolResult, ClientConfig};
pub use mcp_protocol::service::MrtrLimits;
use mcp_protocol::{
    ClientHandler, ClientLifecycleMode, ClientServiceExt, RoleClient,
    service::{ClientInitializeError, RunningService, ServiceError},
    transport::IntoTransport,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{marker::PhantomData, time::Duration};

/// A client lifecycle, protocol, encoding, or execution failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ClientError {
    /// The initial MCP handshake failed.
    #[error(transparent)]
    Initialize(Box<ClientInitializeError>),
    /// The peer did not finish modern discovery before the deadline.
    #[error("modern MCP discovery timed out")]
    DiscoveryTimeout,
    /// A protocol request failed.
    #[error(transparent)]
    Service(Box<ServiceError>),
    /// A value could not be encoded or decoded.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// The skills index has an unsupported schema or resource content shape.
    #[error("invalid skills discovery index")]
    InvalidSkillIndex,
    /// Tool inputs must serialize to an object.
    #[error("tool input must serialize to an object")]
    InvalidInput,
    /// The tool returned an intentional execution error.
    #[error("tool execution failed: {0}")]
    ToolFailure(String),
    /// The tool did not return a decodable value.
    #[error("tool result has neither structured content nor a single text value")]
    MissingOutput,
    /// A child process could not be started.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// WebSocket framing or connection failure.
    #[cfg(feature = "websocket")]
    #[error(transparent)]
    WebSocket(Box<crate::transport::websocket::WebSocketError>),
    /// The connection did not negotiate MCP 2026-07-28.
    #[error("MCP 2026-07-28 discovery is required")]
    ProtocolVersion,
    /// A subscription buffer size exceeded the runtime limit.
    #[error("subscription buffer capacity exceeds runtime limits")]
    SubscriptionCapacity,
    /// Client background cleanup failed.
    #[error(transparent)]
    Task(#[from] tokio::task::JoinError),
}
impl From<ClientInitializeError> for ClientError {
    fn from(error: ClientInitializeError) -> Self {
        Self::Initialize(Box::new(error))
    }
}
impl From<ServiceError> for ClientError {
    fn from(error: ServiceError) -> Self {
        Self::Service(Box::new(error))
    }
}

/// An owned SDK client connection. Call `close` to await connection cleanup.
///
/// Custom SDK client handlers can support elicitation and notifications.
#[must_use]
pub struct Client<H: ClientHandler = ClientConfig> {
    service: RunningService<RoleClient, H>,
}
impl<H: ClientHandler> std::fmt::Debug for Client<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("closed", &self.service.is_closed())
            .finish_non_exhaustive()
    }
}
impl Client {
    /// Connect using MCP 2026-07-28 discovery with a ten-second deadline.
    ///
    /// Never falls back to initialize. Use `connect_with_timeout` to override the deadline.
    ///
    /// # Errors
    /// Returns a handshake error or `ClientError::DiscoveryTimeout`.
    pub async fn connect<T, E, A>(transport: T) -> Result<Self, ClientError>
    where
        T: IntoTransport<RoleClient, E, A>,
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::connect_with_timeout(transport, Duration::from_secs(10)).await
    }
    /// Connect using modern discovery with a caller-selected deadline.
    ///
    /// The deadline covers discovery sends and responses, not earlier transport
    /// construction or requests after connecting. Zero rejects immediately without
    /// sending discovery. Expiry drops the owned discovery future and transport;
    /// transport-specific background cleanup may finish asynchronously.
    /// Never retries or falls back to initialize.
    ///
    /// # Errors
    /// Returns a handshake error or `ClientError::DiscoveryTimeout`.
    pub async fn connect_with_timeout<T, E, A>(
        transport: T,
        timeout: Duration,
    ) -> Result<Self, ClientError>
    where
        T: IntoTransport<RoleClient, E, A>,
        E: std::error::Error + Send + Sync + 'static,
    {
        if timeout.is_zero() {
            return Err(ClientError::DiscoveryTimeout);
        }
        Self::from_service(
            tokio::time::timeout(
                timeout,
                ClientConfig::default().serve_with_lifecycle(
                    transport,
                    ClientLifecycleMode::Discover {
                        preferred_versions: vec![crate::model::ProtocolVersion::V_2026_07_28],
                    },
                ),
            )
            .await
            .map_err(|_| ClientError::DiscoveryTimeout)??,
        )
    }
    /// Connect to a WebSocket endpoint with caller-supplied request headers.
    /// Uses WebPKI roots for wss, a 4 MiB inbound limit, a ten-second TCP/TLS/upgrade
    /// deadline, then a separate ten-second MCP discovery deadline.
    /// For custom TLS, use `websocket_tls` with `websocket-tls`, or wrap a
    /// preconfigured socket in `WebSocketTransport`.
    /// # Errors
    /// Returns a WebSocket or MCP lifecycle error.
    #[cfg(feature = "websocket")]
    pub async fn websocket(
        request: impl tokio_tungstenite::tungstenite::client::IntoClientRequest + Unpin,
    ) -> Result<Self, ClientError> {
        Self::connect_websocket(request, None).await
    }
    /// Connect to a wss endpoint with explicit Rustls trust and optional client identity.
    ///
    /// Requires `websocket-tls`. Retains caller-supplied request headers and the
    /// standard WebSocket message limits/connect deadline. Rejects non-wss URLs
    /// before opening a socket. The host owns trust roots and client certificates.
    /// # Errors
    /// Returns scheme, TLS, WebSocket or modern MCP lifecycle errors.
    #[cfg(feature = "websocket-tls")]
    pub async fn websocket_tls(
        request: impl tokio_tungstenite::tungstenite::client::IntoClientRequest + Unpin,
        tls: std::sync::Arc<tokio_rustls::rustls::ClientConfig>,
    ) -> Result<Self, ClientError> {
        use crate::transport::websocket::WebSocketError;
        let request = request
            .into_client_request()
            .map_err(|error| ClientError::WebSocket(Box::new(error.into())))?;
        if request.uri().scheme_str() != Some("wss") {
            return Err(ClientError::WebSocket(Box::new(
                WebSocketError::TlsRequired,
            )));
        }
        Self::connect_websocket(request, Some(tokio_tungstenite::Connector::Rustls(tls))).await
    }
    #[cfg(feature = "websocket")]
    async fn connect_websocket(
        request: impl tokio_tungstenite::tungstenite::client::IntoClientRequest + Unpin,
        connector: Option<tokio_tungstenite::Connector>,
    ) -> Result<Self, ClientError> {
        use crate::transport::websocket::{WebSocketError, WebSocketTransport};
        let config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
            .max_message_size(Some(4 << 20))
            .max_frame_size(Some(4 << 20));
        let socket = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            tokio_tungstenite::connect_async_tls_with_config(
                request,
                Some(config),
                false,
                connector,
            ),
        )
        .await
        .map_err(|_| ClientError::WebSocket(Box::new(WebSocketError::Timeout)))?
        .map_err(|error| ClientError::WebSocket(Box::new(error.into())))?
        .0;
        Self::connect(WebSocketTransport::<RoleClient, _>::new(socket)).await
    }
    /// Open a gRPC connection using a host-configured generated client and metadata.
    /// # Errors
    /// Returns a Connect RPC or MCP lifecycle failure.
    #[cfg(feature = "grpc")]
    pub async fn grpc(
        client: crate::transport::grpc::proto::mcp_client::McpClient<tonic::transport::Channel>,
        metadata: tonic::metadata::MetadataMap,
    ) -> Result<Self, ClientError> {
        let transport = crate::transport::grpc::connect(client, metadata);
        Self::connect(transport).await
    }
    /// Launch a caller-configured program directly, without a shell.
    ///
    /// # Errors
    /// Returns an I/O or handshake error. Command arguments and environment are caller-owned.
    #[cfg(feature = "client-stdio")]
    pub async fn stdio(command: tokio::process::Command) -> Result<Self, ClientError> {
        Self::stdio_with_max_message_size(
            command,
            mcp_protocol::transport::async_rw::DEFAULT_MAX_MESSAGE_SIZE,
        )
        .await
    }
    /// Launch a program with a maximum incoming line payload from stdout.
    ///
    /// The limit excludes LF and includes an optional CR. Zero rejects nonempty
    /// lines. Oversized messages end reception before JSON parsing.
    /// # Errors
    /// Returns an I/O or discovery error. Command and authentication are host-owned.
    #[cfg(feature = "client-stdio")]
    pub async fn stdio_with_max_message_size(
        command: tokio::process::Command,
        max_message_size: usize,
    ) -> Result<Self, ClientError> {
        let (transport, _) = mcp_protocol::transport::TokioChildProcess::builder(command)
            .max_message_size(max_message_size)
            .spawn()?;
        Self::connect(transport).await
    }
    /// Connect using a caller-provided HTTP client and complete `/mcp` endpoint.
    ///
    /// Transport credentials, TLS, timeouts, and headers remain caller-controlled.
    /// # Errors
    /// Returns a handshake or transport error.
    #[cfg(feature = "client-http")]
    pub async fn http<C>(
        client: C,
        config: mcp_protocol::transport::streamable_http_client::StreamableHttpClientTransportConfig,
    ) -> Result<Self, ClientError>
    where
        C: mcp_protocol::transport::streamable_http_client::StreamableHttpClient,
    {
        Self::connect(
            mcp_protocol::transport::StreamableHttpClientTransport::with_client(client, config),
        )
        .await
    }
}
impl<H: ClientHandler> Client<H> {
    /// Discover skills through the ordinary `skill://index.json` resource.
    /// # Errors
    /// Propagates missing-resource/protocol errors and rejects malformed indexes.
    pub async fn list_skills(&self) -> Result<Vec<crate::skills::SkillIndexEntry>, ClientError> {
        use crate::skills::*;
        let result = self.read_skill_uri(SKILL_INDEX_URI).await?;
        let [crate::model::ResourceContents::TextResourceContents { uri, text, .. }] =
            result.contents.as_slice()
        else {
            return Err(ClientError::InvalidSkillIndex);
        };
        if uri != SKILL_INDEX_URI {
            return Err(ClientError::InvalidSkillIndex);
        }
        let index: SkillIndex = serde_json::from_str(text)?;
        if index.schema != SKILL_INDEX_SCHEMA {
            return Err(ClientError::InvalidSkillIndex);
        }
        Ok(index.skills)
    }
    /// Read a skill entrypoint or supporting file through ordinary resources/read.
    /// The URI is passed unchanged to the server, which controls access. MRTR inputs
    /// are answered through the client handler with default limits.
    /// # Errors
    /// Propagates protocol and transport errors.
    pub async fn read_skill_uri(
        &self,
        uri: impl Into<String>,
    ) -> Result<crate::model::ReadResourceResult, ClientError> {
        self.read_resource(crate::model::ReadResourceRequestParams::new(uri))
            .await
    }
    /// Wrap a modern SDK connection, including one with custom callbacks.
    /// Rejects and cancels connections negotiated at any other revision.
    /// # Errors
    /// Returns a protocol-version error for a non-modern connection.
    pub fn from_service(service: RunningService<RoleClient, H>) -> Result<Self, ClientError> {
        if service
            .peer_info()
            .is_none_or(|info| info.protocol_version != crate::model::ProtocolVersion::V_2026_07_28)
        {
            service.cancellation_token().cancel();
            return Err(ClientError::ProtocolVersion);
        }
        Ok(Self { service })
    }
    /// Access the protocol peer for listing and advanced requests.
    ///
    /// Use this client's prompt/resource helpers to drive automatic MRTR callbacks;
    /// the peer's convenience methods only accept completed single-round responses.
    pub fn peer(&self) -> &mcp_protocol::Peer<RoleClient> {
        self.service.peer()
    }
    /// Open a modern, scoped subscription with a ten-second establishment deadline.
    /// Notifications are consumed through
    /// `Subscription::next`, separate from general client notification callbacks.
    /// The buffer covers initial resource/task snapshots plus 64 ordinary updates.
    /// The SDK acknowledgment precedes application admission; wait for an initial
    /// refresh hint before treating the subscription as live.
    /// # Errors
    /// Returns an SDK transport or subscription establishment error. Admission
    /// rejection after acknowledgment appears on the subscription's next operation.
    pub async fn listen(
        &self,
        filter: crate::model::SubscriptionFilter,
    ) -> Result<mcp_protocol::service::Subscription, ClientError> {
        self.listen_with_timeout(filter, Duration::from_secs(10))
            .await
    }
    /// Open a scoped subscription with a caller-selected establishment deadline.
    ///
    /// Covers outgoing queue admission, sending and acknowledgment. Established
    /// streams have no lifetime deadline. Zero sends nothing. On expiry local
    /// notification routing is removed and peer cancellation is best effort.
    /// Application rejection after acknowledgment is delivered by `Subscription::next`.
    ///
    /// # Errors
    /// Returns `ClientError::Service` containing `ServiceError::Timeout` on expiry,
    /// a buffer-capacity error, or an underlying transport/protocol error.
    pub async fn listen_with_timeout(
        &self,
        filter: crate::model::SubscriptionFilter,
        timeout: Duration,
    ) -> Result<mcp_protocol::service::Subscription, ClientError> {
        // Allow initial resource/task snapshots and category hints plus the ordinary buffer.
        let capacity = filter
            .resource_subscriptions
            .as_ref()
            .map_or(0, Vec::len)
            .checked_add(filter.task_ids.as_ref().map_or(0, Vec::len))
            .and_then(|count| count.checked_add(67))
            .and_then(std::num::NonZeroUsize::new)
            .filter(|capacity| capacity.get() <= tokio::sync::Semaphore::MAX_PERMITS)
            .ok_or(ClientError::SubscriptionCapacity)?;
        Ok(self
            .service
            .peer()
            .listen_with_capacity_and_timeout(filter, capacity, timeout)
            .await?)
    }
    /// Read handshake identity, including build information and modern Go icon sizes.
    /// This uses retained discovery metadata without another network request.
    /// Call `ServerMetadata::from_discovery` on a fresh discovery result after updates.
    /// # Errors
    /// Returns a decoding error if the advertised metadata is malformed.
    pub fn server_metadata(&self) -> Result<Option<crate::ServerMetadata>, ClientError> {
        self.service
            .peer()
            .peer_info()
            .as_deref()
            .map(crate::ServerMetadata::from_peer_info)
            .transpose()
            .map(Option::flatten)
            .map_err(Into::into)
    }
    /// Bind a reusable typed handle to a tool name.
    pub fn tool<I, O>(&self, name: impl Into<String>) -> TypedTool<'_, I, O, H> {
        TypedTool {
            client: self,
            name: name.into(),
            types: PhantomData,
        }
    }
    /// Invoke a tool and decode its typed output.
    ///
    /// Structured content takes precedence. Otherwise the first text block is
    /// decoded as JSON; a String output receives the exact unmodified text.
    /// # Errors
    /// Returns input, protocol, execution, or output decoding failures.
    pub async fn call<I: Serialize, O: DeserializeOwned + 'static>(
        &self,
        name: &str,
        input: &I,
    ) -> Result<O, ClientError> {
        self.call_with_mrtr_limits(name, input, MrtrLimits::default())
            .await
    }
    /// Invoke a tool with per-call MRTR budgets and decode its typed output.
    ///
    /// Limits apply only to this operation. Output decoding follows [`Self::call`].
    /// Zero rounds sends nothing after input validation. These count limits do not
    /// impose a callback deadline or a transport byte limit.
    ///
    /// # Errors
    /// Returns input, protocol, execution, or output decoding failures. MRTR budget
    /// failures are preserved in [`ClientError::Service`].
    pub async fn call_with_mrtr_limits<I: Serialize, O: DeserializeOwned + 'static>(
        &self,
        name: &str,
        input: &I,
        limits: MrtrLimits,
    ) -> Result<O, ClientError> {
        let result = self.call_raw_with_mrtr_limits(name, input, limits).await?;
        if result.is_error == Some(true) {
            let message = result
                .content
                .iter()
                .filter_map(|block| block.as_text())
                .map(|text| text.text.as_str())
                .collect::<Vec<_>>()
                .join("\n");
            return Err(ClientError::ToolFailure(message));
        }
        if let Some(value) = result.structured_content {
            return Ok(serde_json::from_value(value)?);
        }
        let text = result
            .content
            .iter()
            .find_map(|block| block.as_text())
            .map(|text| text.text.as_str())
            .ok_or(ClientError::MissingOutput)?;
        if std::any::TypeId::of::<O>() == std::any::TypeId::of::<String>() {
            return Ok(serde_json::from_value(Value::String(text.to_owned()))?);
        }
        Ok(serde_json::from_str(text)?)
    }
    /// Invoke once, preserving final results, MRTR input requests, or task handles.
    ///
    /// Use the peer for subsequent task polling or explicit MRTR control.
    /// # Errors
    /// Returns input encoding or SDK request errors.
    pub async fn call_once<I: Serialize>(
        &self,
        name: &str,
        input: &I,
    ) -> Result<crate::model::CallToolResponse, ClientError> {
        let Value::Object(arguments) = serde_json::to_value(input)? else {
            return Err(ClientError::InvalidInput);
        };
        Ok(self
            .service
            .call_tool_once(CallToolRequestParams::new(name.to_owned()).with_arguments(arguments))
            .await?)
    }
    /// Invoke a tool while preserving its native content and execution status.
    ///
    /// # Errors
    /// Returns input encoding errors or SDK request failures.
    pub async fn call_raw<I: Serialize>(
        &self,
        name: &str,
        input: &I,
    ) -> Result<CallToolResult, ClientError> {
        self.call_raw_with_mrtr_limits(name, input, MrtrLimits::default())
            .await
    }
    /// Invoke with per-call MRTR budgets, retaining native content and execution status.
    ///
    /// Limits apply only to this operation; [`Self::call_once`] remains single-round.
    /// Zero rounds sends nothing after input validation. These count limits do not
    /// impose a callback deadline or a transport byte limit.
    ///
    /// # Errors
    /// Returns input encoding errors or protocol failures, including MRTR budget
    /// errors inside [`ClientError::Service`].
    pub async fn call_raw_with_mrtr_limits<I: Serialize>(
        &self,
        name: &str,
        input: &I,
        limits: MrtrLimits,
    ) -> Result<CallToolResult, ClientError> {
        let input = serde_json::to_value(input)?;
        let Value::Object(arguments) = input else {
            return Err(ClientError::InvalidInput);
        };
        Ok(self
            .service
            .call_tool_with_mrtr_limits(
                CallToolRequestParams::new(name.to_owned()).with_arguments(arguments),
                limits,
            )
            .await?)
    }
    /// Get a prompt, automatically answering MRTR inputs through the client handler.
    ///
    /// Uses [`MrtrLimits::default`]. The result retains native messages and metadata.
    /// # Errors
    /// Returns protocol, input-handler, or MRTR budget errors in [`ClientError::Service`].
    pub async fn get_prompt(
        &self,
        params: crate::model::GetPromptRequestParams,
    ) -> Result<crate::model::GetPromptResult, ClientError> {
        self.get_prompt_with_mrtr_limits(params, MrtrLimits::default())
            .await
    }
    /// Get a prompt with per-operation round, input and callback concurrency budgets.
    ///
    /// Zero rounds sends nothing. Count limits do not impose callback deadlines
    /// or transport byte limits. Overrides do not change later calls' defaults.
    /// # Errors
    /// Returns protocol, input-handler, or MRTR budget errors in [`ClientError::Service`].
    pub async fn get_prompt_with_mrtr_limits(
        &self,
        params: crate::model::GetPromptRequestParams,
        limits: MrtrLimits,
    ) -> Result<crate::model::GetPromptResult, ClientError> {
        Ok(self
            .service
            .get_prompt_with_mrtr_limits(params, limits)
            .await?)
    }
    /// Send one prompt request, preserving completion or an MRTR input request.
    ///
    /// Does not invoke input callbacks. To resume, pass the returned state and
    /// answers in the next request, or use [`Self::get_prompt`] for automatic handling.
    /// # Errors
    /// Returns protocol errors in [`ClientError::Service`].
    pub async fn get_prompt_once(
        &self,
        params: crate::model::GetPromptRequestParams,
    ) -> Result<crate::model::GetPromptResponse, ClientError> {
        Ok(self.service.get_prompt_once(params).await?)
    }
    /// Read a resource, automatically answering MRTR inputs through the client handler.
    ///
    /// Uses [`MrtrLimits::default`]. Retains native content, metadata and cache hints.
    /// # Errors
    /// Returns protocol, input-handler, or MRTR budget errors in [`ClientError::Service`].
    pub async fn read_resource(
        &self,
        params: crate::model::ReadResourceRequestParams,
    ) -> Result<crate::model::ReadResourceResult, ClientError> {
        self.read_resource_with_mrtr_limits(params, MrtrLimits::default())
            .await
    }
    /// Read a resource with per-operation round, input and callback concurrency budgets.
    ///
    /// Zero rounds sends nothing. Count limits do not impose callback deadlines
    /// or transport byte limits. Overrides do not change later calls' defaults.
    /// # Errors
    /// Returns protocol, input-handler, or MRTR budget errors in [`ClientError::Service`].
    pub async fn read_resource_with_mrtr_limits(
        &self,
        params: crate::model::ReadResourceRequestParams,
        limits: MrtrLimits,
    ) -> Result<crate::model::ReadResourceResult, ClientError> {
        Ok(self
            .service
            .read_resource_with_mrtr_limits(params, limits)
            .await?)
    }
    /// Send one resource request, preserving completion or an MRTR input request.
    ///
    /// Does not invoke input callbacks. To resume, pass the returned state and
    /// answers in the next request, or use [`Self::read_resource`] for automatic handling.
    /// # Errors
    /// Returns protocol errors in [`ClientError::Service`].
    pub async fn read_resource_once(
        &self,
        params: crate::model::ReadResourceRequestParams,
    ) -> Result<crate::model::ReadResourceResponse, ClientError> {
        Ok(self.service.read_resource_once(params).await?)
    }
    /// Close the connection and wait for SDK cleanup.
    ///
    /// # Errors
    /// Returns an SDK background task failure.
    pub async fn close(mut self) -> Result<(), ClientError> {
        if let mcp_protocol::service::QuitReason::JoinError(error) = self.service.close().await? {
            return Err(error.into());
        }
        Ok(())
    }
}

/// A borrowed reusable tool handle with compile-time input and output types.
pub struct TypedTool<'a, I, O, H: ClientHandler = ClientConfig> {
    client: &'a Client<H>,
    name: String,
    types: PhantomData<fn(I) -> O>,
}
impl<I, O, H: ClientHandler> std::fmt::Debug for TypedTool<'_, I, O, H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedTool")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}
impl<I: Serialize, O: DeserializeOwned + 'static, H: ClientHandler> TypedTool<'_, I, O, H> {
    /// Invoke this tool with a typed input.
    ///
    /// # Errors
    /// Returns the same errors as `Client::call`.
    pub async fn call(&self, input: &I) -> Result<O, ClientError> {
        self.client.call(&self.name, input).await
    }
    /// Invoke with typed input and per-call MRTR budgets.
    ///
    /// The handle remains reusable with different limits or the defaults.
    /// # Errors
    /// Returns the same errors as [`Client::call_with_mrtr_limits`].
    pub async fn call_with_mrtr_limits(
        &self,
        input: &I,
        limits: MrtrLimits,
    ) -> Result<O, ClientError> {
        self.client
            .call_with_mrtr_limits(&self.name, input, limits)
            .await
    }
}

/// Explicit HTTP subscription compatibility with the pinned klarlabs-studio/mcp-go wire dialect.
#[cfg(feature = "client-go-subscriptions")]
pub mod go_subscriptions;

/// Explicit modern HTTP MRTR adapter for the pinned Go wire dialect.
#[cfg(feature = "client-go-mrtr")]
pub mod go_mrtr;
