//! Optional SDK transport adapters with explicit graceful shutdown.
#[cfg(any(
    feature = "stdio",
    feature = "http",
    feature = "websocket",
    feature = "grpc"
))]
mod modern;
#[cfg(feature = "webhooks")]
pub mod webhook;
#[cfg(any(feature = "stdio", feature = "http"))]
use mcp_protocol::ServerHandler;
#[cfg(feature = "stdio")]
use mcp_protocol::ServiceExt;
#[cfg(feature = "http")]
use mcp_protocol::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::never::NeverSessionManager,
};
#[cfg(any(
    feature = "stdio",
    feature = "http",
    feature = "websocket",
    feature = "grpc"
))]
use modern::Modern;
#[cfg(any(feature = "stdio", feature = "http"))]
use tokio_util::sync::CancellationToken;

/// A transport or SDK service lifecycle failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TransportError {
    /// The socket or transport failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// The SDK could not initialize the stdio connection.
    #[cfg(feature = "stdio")]
    #[error(transparent)]
    Initialize(Box<mcp_protocol::service::ServerInitializeError>),
    /// An SDK background task failed.
    #[error(transparent)]
    Task(#[from] tokio::task::JoinError),
}

/// Serve MCP on stdin/stdout until EOF or cancellation, then await SDK cleanup.
///
/// Tokio stdin may retain a blocking reader until EOF. A caller shutting down an
/// owned runtime should bound its teardown with `Runtime::shutdown_timeout` if
/// stdin can remain open; see the example application.
///
/// # Errors
/// Returns an initialization error or a failure from the SDK background task.
#[cfg(feature = "stdio")]
pub async fn serve_stdio<H: ServerHandler>(
    server: H,
    shutdown: CancellationToken,
) -> Result<(), TransportError> {
    serve_stdio_with_max_message_size(
        server,
        shutdown,
        mcp_protocol::transport::async_rw::DEFAULT_MAX_MESSAGE_SIZE,
    )
    .await
}

/// Serve stdio with a maximum incoming line payload before JSON parsing.
///
/// The limit excludes LF and includes an optional CR. Zero permits only empty
/// lines. Oversized messages end reception without draining the remaining input.
/// The stdin runtime teardown note on [`serve_stdio`] also applies.
/// # Errors
/// Returns a discovery or background-task failure.
#[cfg(feature = "stdio")]
pub async fn serve_stdio_with_max_message_size<H: ServerHandler>(
    server: H,
    shutdown: CancellationToken,
    max_message_size: usize,
) -> Result<(), TransportError> {
    let (read, write) = mcp_protocol::transport::stdio();
    let transport = mcp_protocol::transport::async_rw::AsyncRwTransport::new_with_max_message_size(
        read,
        write,
        max_message_size,
    );
    let running = tokio::select! {
        biased;
        _ = shutdown.cancelled() => return Ok(()),
        result = Modern(server).serve(transport) => result?,
    };
    let cancel = running.cancellation_token();
    let completion = running.waiting();
    tokio::pin!(completion);
    let reason = tokio::select! {
        result = &mut completion => result?,
        _ = shutdown.cancelled() => {
            cancel.cancel();
            completion.await?
        },
    };
    if let mcp_protocol::service::QuitReason::JoinError(error) = reason {
        return Err(error.into());
    }
    Ok(())
}

/// Host policy for modern, stateless Streamable HTTP.
///
/// Defaults retain SDK loopback admission, a 4 MiB request body limit and SSE
/// responses. TLS and endpoint authentication belong to the hosting application.
/// This type exposes no legacy session or session-store settings.
#[cfg(feature = "http")]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct HttpServerConfig {
    /// Allowed hostnames or `host:port` authorities. Empty disables host checks.
    pub allowed_hosts: Vec<String>,
    /// Browser origin policy. `None` disables Origin validation; `Some([])`
    /// rejects every present Origin. Requests without Origin remain permitted.
    /// Use explicit ports, e.g. `https://app.example.com:443`, or `:*` for any port.
    pub allowed_origins: Option<Vec<String>>,
    /// Maximum POST body size, enforced while streaming, even without a length.
    /// Zero rejects every nonempty body. Oversized bodies receive HTTP 413.
    pub max_request_body_bytes: usize,
    /// Prefer JSON responses; fall back to SSE when a handler emits a message
    /// before its response, preserving server-to-client traffic.
    pub json_response: bool,
}

#[cfg(feature = "http")]
impl Default for HttpServerConfig {
    fn default() -> Self {
        let defaults = StreamableHttpServerConfig::default();
        Self {
            allowed_hosts: defaults.allowed_hosts,
            allowed_origins: None,
            max_request_body_bytes: defaults.max_request_body_bytes,
            json_response: defaults.json_response,
        }
    }
}

/// Streamable HTTP at `/mcp`. SDK origin/host validation and request limits apply.
/// Requests are stateless: no transport session IDs or legacy session endpoints.
/// TLS and endpoint-wide authentication belong to the host's transport layer.
/// Each request offers the handler a `ServerHandler::request_snapshot` hook;
/// [`crate::LiveServer`] uses it to bind routing admission and dispatch to one
/// registry configuration, even if another task replaces the registry.
#[cfg(feature = "http")]
pub fn http_router<H: ServerHandler + Clone>(
    server: H,
    shutdown: CancellationToken,
) -> axum::Router {
    http_router_with_config(server, shutdown, HttpServerConfig::default())
}

/// Streamable HTTP at `/mcp` with explicit host policy.
///
/// Host/Origin validation and streaming body limits run before handler dispatch.
/// The shutdown token cancels active requests. Legacy sessions and session stores
/// remain disabled regardless of the chosen response or admission policy.
#[cfg(feature = "http")]
pub fn http_router_with_config<H: ServerHandler + Clone>(
    server: H,
    shutdown: CancellationToken,
    policy: HttpServerConfig,
) -> axum::Router {
    let mut config = StreamableHttpServerConfig::default()
        .with_cancellation_token(shutdown)
        .with_allowed_hosts(policy.allowed_hosts)
        .with_max_request_body_bytes(policy.max_request_body_bytes);
    config.json_response = policy.json_response;
    if let Some(origins) = policy.allowed_origins {
        config = config
            .with_allowed_origins(origins)
            .enforce_origin_validation();
    }
    let service = StreamableHttpService::new(
        move || Ok(Modern(server.clone())),
        NeverSessionManager::default().into(),
        config,
    );
    axum::Router::new().nest_service("/mcp", service)
}

/// Serve Streamable HTTP using a caller-owned listener and shutdown token.
///
/// # Errors
/// Returns an I/O error if the HTTP server fails.
#[cfg(feature = "http")]
pub async fn serve_http<H: ServerHandler + Clone>(
    server: H,
    listener: tokio::net::TcpListener,
    shutdown: CancellationToken,
) -> Result<(), TransportError> {
    serve_http_with_config(server, listener, shutdown, HttpServerConfig::default()).await
}

/// Serve modern Streamable HTTP with explicit host policy and awaited shutdown.
///
/// # Errors
/// Returns an I/O error if the HTTP server fails.
#[cfg(feature = "http")]
pub async fn serve_http_with_config<H: ServerHandler + Clone>(
    server: H,
    listener: tokio::net::TcpListener,
    shutdown: CancellationToken,
    policy: HttpServerConfig,
) -> Result<(), TransportError> {
    axum::serve(
        listener,
        http_router_with_config(server, shutdown.child_token(), policy),
    )
    .with_graceful_shutdown(shutdown.cancelled_owned())
    .await?;
    Ok(())
}

#[cfg(feature = "stdio")]
impl From<mcp_protocol::service::ServerInitializeError> for TransportError {
    fn from(error: mcp_protocol::service::ServerInitializeError) -> Self {
        Self::Initialize(Box::new(error))
    }
}

#[cfg(feature = "http")]
pub mod server_card;

#[cfg(feature = "grpc")]
pub mod grpc;
#[cfg(feature = "websocket")]
pub mod websocket;

#[cfg(any(feature = "websocket", feature = "grpc"))]
async fn wait_session<H: mcp_protocol::Service<mcp_protocol::RoleServer>>(
    running: mcp_protocol::service::RunningService<mcp_protocol::RoleServer, H>,
    shutdown: tokio_util::sync::CancellationToken,
) -> Result<(), tokio::task::JoinError> {
    let cancel = running.cancellation_token();
    let waiting = running.waiting();
    tokio::pin!(waiting);
    let reason = tokio::select! {
        result = &mut waiting => result?,
        _ = shutdown.cancelled() => { cancel.cancel(); waiting.await? }
    };
    if let mcp_protocol::service::QuitReason::JoinError(error) = reason {
        return Err(error);
    }
    Ok(())
}

#[cfg(any(feature = "client-go-subscriptions", feature = "client-go-mrtr"))]
pub(crate) mod sse;
