//! WebSocket JSON-RPC transport, compatible with the pinned Go transport.
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use mcp_protocol::{
    ServerHandler, ServiceExt,
    service::{RxJsonRpcMessage, ServiceRole, TxJsonRpcMessage},
    transport::Transport,
};
use std::{marker::PhantomData, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::Mutex,
};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        self, Message,
        handshake::server::{Request, Response},
        protocol::WebSocketConfig,
    },
};
use tokio_util::sync::CancellationToken;

/// Rustls configuration and TLS stream types for caller-owned trust and identities.
#[cfg(feature = "websocket-tls")]
pub use tokio_rustls;
/// Re-export for caller-controlled handshake headers and TLS connectors.
pub use tokio_tungstenite;

/// WebSocket framing, encoding, or deadline failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WebSocketError {
    /// WebSocket I/O or protocol failure.
    #[error(transparent)]
    Socket(Box<tungstenite::Error>),
    /// Invalid JSON-RPC message.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// A write or closing handshake exceeded its deadline.
    #[error("WebSocket operation timed out")]
    Timeout,
    /// A TLS-only client was given a URL without the wss scheme.
    #[cfg(feature = "websocket-tls")]
    #[error("TLS WebSocket client requires a wss URL")]
    TlsRequired,
}

impl From<tungstenite::Error> for WebSocketError {
    fn from(error: tungstenite::Error) -> Self {
        Self::Socket(Box::new(error))
    }
}

/// Owns both halves of a WebSocket; closing awaits the bounded close-frame write.
/// Pass a preconfigured TLS socket here when terminating TLS inside the process.
#[must_use]
pub struct WebSocketTransport<R: ServiceRole, S> {
    sink: Arc<Mutex<SplitSink<WebSocketStream<S>, Message>>>,
    stream: SplitStream<WebSocketStream<S>>,
    role: PhantomData<R>,
    write_timeout: Duration,
}
impl<R: ServiceRole, S> std::fmt::Debug for WebSocketTransport<R, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebSocketTransport").finish_non_exhaustive()
    }
}
impl<R: ServiceRole, S: AsyncRead + AsyncWrite + Unpin> WebSocketTransport<R, S> {
    /// Wrap an already upgraded socket with a ten-second write deadline.
    pub fn new(socket: WebSocketStream<S>) -> Self {
        let (sink, stream) = socket.split();
        Self {
            sink: Arc::new(Mutex::new(sink)),
            stream,
            role: PhantomData,
            write_timeout: Duration::from_secs(10),
        }
    }
}
impl<R: ServiceRole, S: AsyncRead + AsyncWrite + Unpin + Send + 'static> Transport<R>
    for WebSocketTransport<R, S>
{
    type Error = WebSocketError;
    fn send(
        &mut self,
        item: TxJsonRpcMessage<R>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        let encoded = serde_json::to_string(&item);
        let sink = self.sink.clone();
        let deadline = self.write_timeout;
        async move {
            let message = Message::Text(encoded?.into());
            tokio::time::timeout(deadline, async { sink.lock().await.send(message).await })
                .await
                .map_err(|_| WebSocketError::Timeout)??;
            Ok(())
        }
    }
    async fn receive(&mut self) -> Option<RxJsonRpcMessage<R>> {
        loop {
            let message = match self.stream.next().await? {
                Ok(message) => message,
                Err(error) => {
                    tracing::debug!(%error, "WebSocket receive failed");
                    return None;
                }
            };
            let decoded = match message {
                Message::Text(text) => serde_json::from_slice(text.as_bytes()),
                Message::Binary(bytes) => serde_json::from_slice(&bytes),
                Message::Close(_) => return None,
                Message::Ping(_) => {
                    // Tungstenite queues the pong while reading; flush it even on an idle connection.
                    if tokio::time::timeout(self.write_timeout, async {
                        self.sink.lock().await.flush().await
                    })
                    .await
                    .ok()?
                    .is_err()
                    {
                        return None;
                    }
                    continue;
                }
                _ => continue,
            };
            match decoded {
                Ok(value) => return Some(value),
                Err(error) => {
                    tracing::debug!(%error, "Invalid WebSocket JSON-RPC message");
                    return None;
                }
            }
        }
    }
    async fn close(&mut self) -> Result<(), Self::Error> {
        let result = tokio::time::timeout(self.write_timeout, async {
            self.sink.lock().await.close().await
        })
        .await
        .map_err(|_| WebSocketError::Timeout)?;
        match result {
            Ok(())
            | Err(tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed) => {
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }
}

/// Connection and handshake limits. Browser origins must be explicitly allowed.
/// Requests without an Origin header are accepted for non-browser clients.
#[derive(Clone, Debug)]
pub struct WebSocketServerConfig {
    /// Exact browser Origin values permitted to upgrade; empty denies all browser origins.
    pub allowed_origins: Vec<String>,
    /// Maximum concurrent connections, including pending handshakes.
    pub max_connections: std::num::NonZeroUsize,
    /// Maximum inbound message/frame bytes.
    pub max_message_bytes: std::num::NonZeroUsize,
    /// Deadline for TLS (when enabled), HTTP upgrade and MCP discovery.
    pub handshake_timeout: Duration,
}
impl Default for WebSocketServerConfig {
    fn default() -> Self {
        Self {
            allowed_origins: Vec::new(),
            max_connections: std::num::NonZeroUsize::new(1024)
                .unwrap_or(std::num::NonZeroUsize::MIN),
            max_message_bytes: std::num::NonZeroUsize::new(4 << 20)
                .unwrap_or(std::num::NonZeroUsize::MIN),
            handshake_timeout: Duration::from_secs(10),
        }
    }
}

/// Serve raw JSON-RPC WebSockets at any path on a caller-owned listener.
///
/// Cancellation stops handshakes and awaits every SDK session. For TLS termination
/// use `serve_websocket_tls` with `websocket-tls`, or wrap authenticated upgraded
/// sockets in [`WebSocketTransport`] directly.
/// # Errors
/// Returns a listener I/O error or a failed session task. Individual peer errors
/// are logged without terminating the listener.
pub async fn serve_websocket<H: ServerHandler + Clone>(
    server: H,
    listener: tokio::net::TcpListener,
    shutdown: CancellationToken,
    config: WebSocketServerConfig,
) -> Result<(), super::TransportError> {
    serve_with_acceptor(server, listener, shutdown, config, |socket| {
        std::future::ready(Ok(socket))
    })
    .await
}

/// Serve modern MCP over TLS WebSockets on a caller-owned TCP listener.
///
/// Requires `websocket-tls`. The host supplies a Rustls server configuration with
/// a certificate identity and optional client-certificate verifier for mutual TLS.
/// The connection limit includes TLS handshakes; one deadline covers TLS, HTTP
/// upgrade and MCP discovery. Origin admission and message limits match
/// [`serve_websocket`]. Cancellation stops handshakes and awaits active sessions.
/// # Errors
/// Returns listener I/O errors or failed session tasks. Rejected peers do not
/// terminate the listener. Certificate loading and trust policy are host-owned.
#[cfg(feature = "websocket-tls")]
pub async fn serve_websocket_tls<H: ServerHandler + Clone>(
    server: H,
    listener: tokio::net::TcpListener,
    shutdown: CancellationToken,
    config: WebSocketServerConfig,
    tls: Arc<tokio_rustls::rustls::ServerConfig>,
) -> Result<(), super::TransportError> {
    let acceptor = tokio_rustls::TlsAcceptor::from(tls);
    serve_with_acceptor(server, listener, shutdown, config, move |socket| {
        let acceptor = acceptor.clone();
        async move { acceptor.accept(socket).await }
    })
    .await
}

async fn serve_with_acceptor<H, F, Fut, S>(
    server: H,
    listener: tokio::net::TcpListener,
    shutdown: CancellationToken,
    config: WebSocketServerConfig,
    accept: F,
) -> Result<(), super::TransportError>
where
    H: ServerHandler + Clone,
    F: Fn(tokio::net::TcpStream) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = std::io::Result<S>> + Send + 'static,
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let permits = Arc::new(tokio::sync::Semaphore::new(config.max_connections.get()));
    let mut sessions = tokio::task::JoinSet::new();
    let child = shutdown.child_token();
    let mut result = loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break Ok(()),
            completed = sessions.join_next(), if !sessions.is_empty() => {
                if let Some(Err(error)) = completed { break Err(error.into()); }
            }
            accepted = listener.accept() => {
                let (socket, _) = match accepted { Ok(value) => value, Err(error) => break Err(error.into()) };
                let Ok(permit) = permits.clone().try_acquire_owned() else { drop(socket); continue; };
                let config = config.clone();
                let server = server.clone();
                let stop = child.child_token();
                let accept = accept.clone();
                sessions.spawn(async move {
                    let _permit = permit;
                    serve_connection(server, accept(socket), stop, config).await;
                });
            }
        }
    };
    child.cancel();
    while let Some(completed) = sessions.join_next().await {
        if let Err(error) = completed
            && result.is_ok()
        {
            result = Err(error.into());
        }
    }
    result
}

struct OriginPolicy<'a>(&'a [String]);
impl tungstenite::handshake::server::Callback for OriginPolicy<'_> {
    // The unboxed HTTP error response is mandated by Tungstenite's callback trait.
    #[allow(clippy::result_large_err)]
    fn on_request(
        self,
        request: &Request,
        response: Response,
    ) -> Result<Response, tungstenite::handshake::server::ErrorResponse> {
        let origins = request.headers().get_all("origin");
        let mut origins = origins.iter();
        let allowed = match origins.next() {
            None => true,
            Some(value) => {
                origins.next().is_none()
                    && value
                        .to_str()
                        .is_ok_and(|origin| self.0.iter().any(|allowed| allowed == origin))
            }
        };
        if allowed {
            return Ok(response);
        }
        let mut rejection = tungstenite::http::Response::new(Some("Origin denied".to_owned()));
        *rejection.status_mut() = tungstenite::http::StatusCode::FORBIDDEN;
        Err(rejection)
    }
}

async fn serve_connection<H, Fut, S>(
    server: H,
    socket: Fut,
    stop: CancellationToken,
    config: WebSocketServerConfig,
) where
    H: ServerHandler,
    Fut: Future<Output = std::io::Result<S>> + Send,
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let limits = WebSocketConfig::default()
        .max_message_size(Some(config.max_message_bytes.get()))
        .max_frame_size(Some(config.max_message_bytes.get()));
    let initialize = async {
        let socket = socket.await?;
        let socket = tokio_tungstenite::accept_hdr_async_with_config(
            socket,
            OriginPolicy(&config.allowed_origins),
            Some(limits),
        )
        .await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
            super::Modern(server)
                .serve(WebSocketTransport::<mcp_protocol::RoleServer, _>::new(
                    socket,
                ))
                .await?,
        )
    };
    let running = tokio::select! {
        _ = stop.cancelled() => return,
        result = tokio::time::timeout(config.handshake_timeout, initialize) => match result {
            Ok(Ok(running)) => running,
            Ok(Err(error)) => { tracing::debug!(%error, "WebSocket handshake failed"); return; }
            Err(_) => { tracing::debug!("WebSocket handshake timed out"); return; }
        }
    };
    if let Err(error) = super::wait_session(running, stop).await {
        tracing::debug!(%error, "WebSocket session failed");
    }
}
