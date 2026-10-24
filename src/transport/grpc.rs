//! MCP over the pinned upstream `mcp.v1.MCP/Connect` protobuf service.
#[cfg(feature = "client")]
use futures_util::TryStreamExt;
use futures_util::{Stream, StreamExt};
use mcp_protocol::{
    ServerHandler, ServiceExt,
    service::{RxJsonRpcMessage, ServiceRole, TxJsonRpcMessage},
    transport::Transport,
};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    marker::PhantomData,
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

/// Generated from the pinned upstream schema; normal builds do not invoke protoc.
#[allow(missing_docs, clippy::all)]
pub mod proto {
    include!("grpc_proto.rs");
}
/// Tonic types for host-controlled channels, TLS, interceptors, and metadata.
pub use tonic;

/// Framing, request correlation, or network error.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum GrpcError {
    /// A protobuf payload did not contain valid JSON-RPC.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// The peer rejected or closed the stream.
    #[error(transparent)]
    Status(Box<tonic::Status>),
    /// The outbound stream was closed.
    #[error("gRPC stream closed")]
    Closed,
    /// A write exceeded its deadline.
    #[error("gRPC write timed out")]
    Timeout,
    /// Too many unanswered requests, an unsolicited response, or malformed envelope.
    #[error("invalid gRPC envelope: {0}")]
    Envelope(&'static str),
}

impl From<tonic::Status> for GrpcError {
    fn from(error: tonic::Status) -> Self {
        Self::Status(Box::new(error))
    }
}

struct Codec {
    pending: HashMap<String, Value>,
    subscriptions: HashSet<String>,
    retired: VecDeque<(String, Value)>,
    is_client: bool,
}
impl Default for Codec {
    fn default() -> Self {
        Self {
            pending: HashMap::new(),
            subscriptions: HashSet::new(),
            retired: VecDeque::new(),
            is_client: true,
        }
    }
}
impl Codec {
    fn retire(&mut self, wire: &str) {
        if let Some(id) = self.pending.remove(wire) {
            self.subscriptions.remove(wire);
            if self.retired.len() == 1024 {
                self.retired.pop_front();
            }
            self.retired.push_back((wire.to_owned(), id));
        }
    }
    fn encode(&mut self, mut value: Value) -> Result<proto::Message, GrpcError> {
        let mut message = proto::Message::default();
        if let Some(method) = value.get("method").and_then(Value::as_str) {
            message.method = method.to_owned();
            message.r#type = if let Some(id) = value.get("id") {
                if self.pending.len() >= 1024 {
                    return Err(GrpcError::Envelope("1024 pending requests exceeded"));
                }
                // Tag the type: JSON number 1 and JSON string "1" must never alias.
                let key = match id {
                    Value::Number(n) => format!("n:{n}"),
                    Value::String(s) => format!("s:{s}"),
                    _ => return Err(GrpcError::Envelope("invalid request ID")),
                };
                if self.pending.contains_key(&key) {
                    return Err(GrpcError::Envelope("duplicate request ID"));
                }
                // Reusing an ID supersedes any old cancelled incarnation.
                self.retired.retain(|(wire, _)| wire != &key);
                self.pending.insert(key.clone(), id.clone());
                if method == "subscriptions/listen" {
                    self.subscriptions.insert(key.clone());
                }
                message.request_id = key;
                proto::MessageType::Request as i32
            } else {
                proto::MessageType::Notification as i32
            };
            if let Some(params) = value.get("params") {
                let mut params = params.clone();
                if message.method == "notifications/cancelled"
                    && let Some(id) = params.get_mut("requestId")
                {
                    *id = Value::String(match &*id {
                        Value::Number(n) => format!("n:{n}"),
                        // Server cancellations refer to the peer's opaque subscription ID.
                        Value::String(s) if !self.is_client => s.clone(),
                        Value::String(s) => format!("s:{s}"),
                        _ => return Err(GrpcError::Envelope("invalid cancellation ID")),
                    });
                }
                message.params = serde_json::to_vec(&params)?;
                if message.method == "notifications/cancelled"
                    && let Some(wire) = params.get("requestId").and_then(Value::as_str)
                    && (self.is_client
                        || value
                            .pointer("/params/requestId")
                            .is_some_and(Value::is_number))
                {
                    self.retire(wire);
                }
                if let Some(token) = params.pointer("/_meta/progressToken") {
                    message.meta = Some(proto::RequestMeta {
                        progress_token: token
                            .as_str()
                            .map(str::to_owned)
                            .unwrap_or_else(|| token.to_string()),
                    });
                }
            }
        } else {
            message.r#type = proto::MessageType::Response as i32;
            message.request_id = value
                .get("id")
                .and_then(Value::as_str)
                .ok_or(GrpcError::Envelope(
                    "response ID must be the peer's opaque string",
                ))?
                .to_owned();
            if let Some(error) = value.get_mut("error") {
                message.error = Some(proto::Error {
                    code: error
                        .get("code")
                        .and_then(Value::as_i64)
                        .and_then(|n| i32::try_from(n).ok())
                        .ok_or(GrpcError::Envelope("invalid error code"))?,
                    message: error
                        .get("message")
                        .and_then(Value::as_str)
                        .ok_or(GrpcError::Envelope("missing error message"))?
                        .to_owned(),
                    data: error
                        .get("data")
                        .map(serde_json::to_vec)
                        .transpose()?
                        .unwrap_or_default(),
                });
            } else {
                message.result =
                    serde_json::to_vec(&value.get("result").cloned().unwrap_or(json!({})))?;
            }
        }
        Ok(message)
    }
    fn decode(&mut self, message: proto::Message) -> Result<Value, GrpcError> {
        let mut value = json!({"jsonrpc":"2.0"});
        match proto::MessageType::try_from(message.r#type) {
            Ok(proto::MessageType::Request | proto::MessageType::Notification) => {
                if message.method.is_empty() {
                    return Err(GrpcError::Envelope("missing method"));
                }
                if !message.result.is_empty() || message.error.is_some() {
                    return Err(GrpcError::Envelope(
                        "request or notification contains response fields",
                    ));
                }
                if message.r#type == proto::MessageType::Notification as i32
                    && !message.request_id.is_empty()
                {
                    return Err(GrpcError::Envelope("notification contains request ID"));
                }
                if message.r#type == proto::MessageType::Request as i32 {
                    if message.request_id.is_empty() {
                        return Err(GrpcError::Envelope("missing request ID"));
                    }
                    value["id"] = Value::String(message.request_id);
                }
                value["method"] = Value::String(message.method);
                if !message.params.is_empty() {
                    value["params"] = serde_json::from_slice(&message.params)?;
                }
                let cancelled = if self.is_client && value["method"] == "notifications/cancelled" {
                    value
                        .pointer("/params/requestId")
                        .and_then(Value::as_str)
                        .filter(|wire| self.subscriptions.contains(*wire))
                        .map(str::to_owned)
                } else {
                    None
                };
                if self.is_client {
                    // The protobuf envelope has string IDs, including embedded
                    // references. Restore only known local subscriptions; an arbitrary
                    // peer string resembling our tags must remain opaque.
                    for pointer in [
                        "/params/_meta/io.modelcontextprotocol~1subscriptionId",
                        "/params/requestId",
                    ] {
                        if pointer == "/params/requestId"
                            && value["method"] != "notifications/cancelled"
                        {
                            continue;
                        }
                        if let Some(id) = value.pointer_mut(pointer)
                            && let Some(wire) = id.as_str()
                            && self.subscriptions.contains(wire)
                            && let Some(original) = self.pending.get(wire)
                        {
                            *id = original.clone();
                        }
                    }
                }
                if let Some(wire) = cancelled {
                    self.retire(&wire);
                }
                // The Go transport keeps params JSON intact. Do not coerce progress
                // token types from the string-only envelope meta field.
            }
            Ok(proto::MessageType::Response) => {
                if !message.method.is_empty() || !message.params.is_empty() {
                    return Err(GrpcError::Envelope("response contains request fields"));
                }
                if message.error.is_some() && !message.result.is_empty() {
                    return Err(GrpcError::Envelope(
                        "response contains both result and error",
                    ));
                }
                value["id"] = self
                    .pending
                    .get(&message.request_id)
                    .or_else(|| {
                        self.retired
                            .iter()
                            .find(|(wire, _)| wire == &message.request_id)
                            .map(|(_, id)| id)
                    })
                    .cloned()
                    .ok_or(GrpcError::Envelope("unsolicited response ID"))?;
                if let Some(error) = message.error {
                    let mut data = json!({"code":error.code,"message":error.message});
                    if !error.data.is_empty() {
                        data["data"] = serde_json::from_slice(&error.data)?;
                    }
                    value["error"] = data;
                } else {
                    value["result"] = if message.result.is_empty() {
                        json!({})
                    } else {
                        serde_json::from_slice(&message.result)?
                    };
                }
                // Commit correlation only after the whole envelope and JSON payload
                // have validated. A malformed response must not consume its ID.
                if self.subscriptions.contains(&message.request_id)
                    && let Some(id) =
                        value.pointer_mut("/result/_meta/io.modelcontextprotocol~1subscriptionId")
                    && id.as_str() == Some(message.request_id.as_str())
                    && let Some(original) = self.pending.get(&message.request_id)
                {
                    *id = original.clone();
                }
                self.pending.remove(&message.request_id);
                self.subscriptions.remove(&message.request_id);
                self.retired.retain(|(wire, _)| wire != &message.request_id);
            }
            _ => return Err(GrpcError::Envelope("unknown message type")),
        }
        Ok(value)
    }
}

type IncomingMessages = Pin<Box<dyn Stream<Item = Result<proto::Message, tonic::Status>> + Send>>;

/// Owns a bounded outbound stream and the peer's incoming stream.
/// Numeric and string JSON-RPC IDs retain their types across protobuf replies.
#[must_use]
pub struct GrpcTransport<R: ServiceRole> {
    outgoing: Arc<Mutex<Option<mpsc::Sender<proto::Message>>>>,
    incoming: Option<IncomingMessages>,
    codec: Arc<Mutex<Codec>>,
    role: PhantomData<R>,
}
impl<R: ServiceRole> std::fmt::Debug for GrpcTransport<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcTransport").finish_non_exhaustive()
    }
}
impl<R: ServiceRole> GrpcTransport<R> {
    fn new(
        incoming: impl Stream<Item = Result<proto::Message, tonic::Status>> + Send + 'static,
        outgoing: mpsc::Sender<proto::Message>,
    ) -> Self {
        Self {
            outgoing: Arc::new(Mutex::new(Some(outgoing))),
            incoming: Some(Box::pin(incoming)),
            codec: Arc::new(Mutex::new(Codec {
                is_client: R::IS_CLIENT,
                ..Default::default()
            })),
            role: PhantomData,
        }
    }
}
impl<R: ServiceRole> Transport<R> for GrpcTransport<R> {
    type Error = GrpcError;
    fn send(
        &mut self,
        item: TxJsonRpcMessage<R>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        let value = serde_json::to_value(item);
        let codec = self.codec.clone();
        let outgoing = self.outgoing.clone();
        async move {
            let message = codec.lock().await.encode(value?)?;
            tokio::time::timeout(Duration::from_secs(10), async {
                let sender = outgoing
                    .lock()
                    .await
                    .as_ref()
                    .cloned()
                    .ok_or(GrpcError::Closed)?;
                sender.send(message).await.map_err(|_| GrpcError::Closed)
            })
            .await
            .map_err(|_| GrpcError::Timeout)?
        }
    }
    async fn receive(&mut self) -> Option<RxJsonRpcMessage<R>> {
        let result = match self.incoming.as_mut()?.next().await? {
            Ok(message) => self
                .codec
                .lock()
                .await
                .decode(message)
                .and_then(|value| serde_json::from_value(value).map_err(Into::into)),
            Err(error) => Err(error.into()),
        };
        match result {
            Ok(value) => Some(value),
            Err(error) => {
                tracing::debug!(%error, "gRPC receive failed");
                None
            }
        }
    }
    async fn close(&mut self) -> Result<(), Self::Error> {
        self.outgoing.lock().await.take();
        self.incoming.take();
        let mut codec = self.codec.lock().await;
        codec.pending.clear();
        codec.subscriptions.clear();
        codec.retired.clear();
        Ok(())
    }
}

/// Prepare the upstream Connect stream using a caller-owned channel.
/// Supply gRPC authentication metadata in `metadata`; message limits and TLS are
/// configured on the caller's generated client or channel instead.
/// Stream opening is driven by SDK receive polling, concurrently with the first
/// send. RPC failures terminate receive and surface during MCP initialization.
#[cfg(feature = "client")]
pub fn connect(
    mut client: proto::mcp_client::McpClient<tonic::transport::Channel>,
    metadata: tonic::metadata::MetadataMap,
) -> GrpcTransport<mcp_protocol::RoleClient> {
    let (sender, receiver) = mpsc::channel(32);
    let mut request = tonic::Request::new(ReceiverStream::new(receiver));
    *request.metadata_mut() = metadata;
    // Go sends response headers only with its first response. Poll the RPC while
    // the SDK queues its first request, rather than awaiting headers up front.
    // This owns no detached connection-opening task.
    let incoming = futures_util::stream::once(async move {
        let response = tokio::time::timeout(Duration::from_secs(10), client.connect(request))
            .await
            .map_err(|_| {
                tonic::Status::deadline_exceeded("Connect response headers timed out")
            })??;
        Ok::<_, tonic::Status>(response.into_inner())
    })
    .try_flatten();
    GrpcTransport::new(incoming, sender)
}

/// Per-adapter limits shared by all clones of a gRPC service.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct GrpcServerConfig {
    /// Maximum active Connect streams, including peers awaiting discovery.
    pub max_streams: NonZeroUsize,
    /// Outbound protobuf messages queued per stream.
    pub queue_capacity: NonZeroUsize,
    /// Maximum encoded/decoded protobuf message bytes in the generated service.
    pub max_message_bytes: NonZeroUsize,
    /// Deadline for modern discovery and the first ordinary MCP request after admission.
    pub handshake_timeout: Duration,
}
impl Default for GrpcServerConfig {
    fn default() -> Self {
        Self {
            max_streams: NonZeroUsize::new(1024).unwrap_or(NonZeroUsize::MIN),
            queue_capacity: NonZeroUsize::new(32).unwrap_or(NonZeroUsize::MIN),
            max_message_bytes: NonZeroUsize::new(4 << 20).unwrap_or(NonZeroUsize::MIN),
            handshake_timeout: Duration::from_secs(10),
        }
    }
}
/// A server limit cannot be represented safely by the runtime.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum GrpcConfigError {
    /// Stream admission would exceed Tokio's semaphore limit.
    #[error("gRPC stream limit exceeds Tokio capacity")]
    StreamLimit,
    /// A stream queue would exceed Tokio's channel limit.
    #[error("gRPC queue capacity exceeds Tokio capacity")]
    QueueCapacity,
    /// The handshake must have a nonzero deadline.
    #[error("gRPC handshake timeout must be nonzero")]
    HandshakeTimeout,
}

/// Host-composable Tonic service with tracked MCP sessions.
/// Call [`Self::shutdown`] when stopping a host-composed server.
#[derive(Clone)]
#[must_use]
pub struct GrpcService<H> {
    server: H,
    shutdown: CancellationToken,
    tasks: TaskTracker,
    admission: Arc<Mutex<()>>,
    permits: Arc<tokio::sync::Semaphore>,
    config: GrpcServerConfig,
}
impl<H> std::fmt::Debug for GrpcService<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcService").finish_non_exhaustive()
    }
}
impl<H: ServerHandler + Clone> GrpcService<H> {
    /// Construct an adapter. RPC authentication belongs to a Tonic interceptor.
    pub fn new(server: H, shutdown: CancellationToken) -> Self {
        Self::configured(server, shutdown, GrpcServerConfig::default())
    }
    /// Construct an adapter with explicit memory, admission and handshake limits.
    /// Limits apply across adapter clones. TLS and authentication remain host-owned.
    /// # Errors
    /// Rejects capacities above Tokio's limit and a zero handshake deadline.
    pub fn with_config(
        server: H,
        shutdown: CancellationToken,
        config: GrpcServerConfig,
    ) -> Result<Self, GrpcConfigError> {
        if config.max_streams.get() > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(GrpcConfigError::StreamLimit);
        }
        if config.queue_capacity.get() > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(GrpcConfigError::QueueCapacity);
        }
        if config.handshake_timeout.is_zero() {
            return Err(GrpcConfigError::HandshakeTimeout);
        }
        Ok(Self::configured(server, shutdown, config))
    }
    fn configured(server: H, shutdown: CancellationToken, config: GrpcServerConfig) -> Self {
        Self {
            server,
            shutdown,
            tasks: TaskTracker::new(),
            admission: Arc::new(Mutex::new(())),
            permits: Arc::new(tokio::sync::Semaphore::new(config.max_streams.get())),
            config,
        }
    }
    /// Clone the adapter into a generated service for `Server::add_service`.
    pub fn service(&self) -> proto::mcp_server::McpServer<Self> {
        proto::mcp_server::McpServer::new(self.clone())
            .max_decoding_message_size(self.config.max_message_bytes.get())
            .max_encoding_message_size(self.config.max_message_bytes.get())
    }
    /// Stop admission, cancel every session, and await its SDK cleanup.
    pub async fn shutdown(&self) {
        {
            let _guard = self.admission.lock().await;
            self.shutdown.cancel();
            self.tasks.close();
        }
        self.tasks.wait().await;
    }
}
#[tonic::async_trait]
impl<H: ServerHandler + Clone> proto::mcp_server::Mcp for GrpcService<H> {
    type ConnectStream = IncomingMessages;
    async fn connect(
        &self,
        request: tonic::Request<tonic::Streaming<proto::Message>>,
    ) -> Result<tonic::Response<Self::ConnectStream>, tonic::Status> {
        let _guard = self.admission.lock().await;
        if self.shutdown.is_cancelled() {
            return Err(tonic::Status::unavailable("server shutting down"));
        }
        let permit =
            self.permits.clone().try_acquire_owned().map_err(|_| {
                tonic::Status::resource_exhausted("gRPC active stream limit exceeded")
            })?;
        let (sender, receiver) = mpsc::channel(self.config.queue_capacity.get());
        let transport =
            GrpcTransport::<mcp_protocol::RoleServer>::new(request.into_inner(), sender);
        let server = self.server.clone();
        let shutdown = self.shutdown.child_token();
        let handshake_timeout = self.config.handshake_timeout;
        self.tasks.spawn(async move {
            let _permit = permit;
            let running = tokio::select! {
                _ = shutdown.cancelled() => return,
                result = tokio::time::timeout(handshake_timeout, super::Modern(server).serve(transport)) => match result {
                    Ok(Ok(running)) => running,
                    _ => { tracing::debug!("gRPC MCP initialization failed or timed out"); return; }
                }
            };
            if let Err(error) = super::wait_session(running, shutdown).await {
                tracing::debug!(%error, "gRPC session failed");
            }
        });
        Ok(tonic::Response::new(Box::pin(
            ReceiverStream::new(receiver).map(Ok),
        )))
    }
}

/// Serve the upstream gRPC service on a caller-owned TCP listener.
/// Uses default limits. Use [`GrpcService::with_config`] directly for custom limits
/// and compose its generated service with host-owned TLS and interceptors.
/// # Errors
/// Returns a Tonic server error; peer failures do not stop other sessions.
pub async fn serve_grpc<H: ServerHandler + Clone>(
    server: H,
    listener: tokio::net::TcpListener,
    shutdown: CancellationToken,
) -> Result<(), tonic::transport::Error> {
    serve_with_builder(
        tonic::transport::Server::builder(),
        server,
        listener,
        shutdown,
    )
    .await
}

/// Serve modern MCP over TLS on a caller-owned TCP listener.
///
/// Requires `grpc-tls`. The server identity is required explicitly; configure
/// `ServerTlsConfig::client_ca_root` to require client certificates for mutual TLS.
/// Certificate loading, trust policy and rotation belong to the host. Cancellation
/// stops admission and awaits tracked MCP sessions, as with [`serve_grpc`].
/// # Errors
/// Returns invalid TLS configuration or a Tonic server error.
#[cfg(feature = "grpc-tls")]
pub async fn serve_grpc_tls<H: ServerHandler + Clone>(
    server: H,
    listener: tokio::net::TcpListener,
    shutdown: CancellationToken,
    identity: tonic::transport::Identity,
    tls: tonic::transport::ServerTlsConfig,
) -> Result<(), tonic::transport::Error> {
    let builder = tonic::transport::Server::builder().tls_config(tls.identity(identity))?;
    serve_with_builder(builder, server, listener, shutdown).await
}

async fn serve_with_builder<H: ServerHandler + Clone>(
    mut builder: tonic::transport::Server,
    server: H,
    listener: tokio::net::TcpListener,
    shutdown: CancellationToken,
) -> Result<(), tonic::transport::Error> {
    let adapter = GrpcService::new(server, shutdown.child_token());
    let stop = adapter.shutdown.clone();
    let result = builder
        .add_service(adapter.service())
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            shutdown.cancelled().await;
            stop.cancel();
        })
        .await;
    adapter.shutdown().await;
    result
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    #[test]
    fn contradictory_request_and_notification_fields_are_rejected() {
        for kind in [
            proto::MessageType::Request,
            proto::MessageType::Notification,
        ] {
            let valid = proto::Message {
                r#type: kind as i32,
                request_id: if kind == proto::MessageType::Request {
                    "peer-id".into()
                } else {
                    String::new()
                },
                method: "ping".into(),
                ..Default::default()
            };
            let mut codec = Codec::default();
            assert!(codec.decode(valid.clone()).is_ok());
            for invalid in [
                proto::Message {
                    method: String::new(),
                    ..valid.clone()
                },
                proto::Message {
                    result: b"{}".to_vec(),
                    ..valid.clone()
                },
                proto::Message {
                    error: Some(proto::Error::default()),
                    ..valid.clone()
                },
                proto::Message {
                    params: b"{".to_vec(),
                    ..valid.clone()
                },
            ] {
                assert!(codec.decode(invalid).is_err());
            }
        }
        let mut codec = Codec::default();
        assert!(
            codec
                .decode(proto::Message {
                    r#type: proto::MessageType::Notification as i32,
                    request_id: "ambiguous".into(),
                    method: "notifications/progress".into(),
                    ..Default::default()
                })
                .is_err()
        );
    }

    #[test]
    fn malformed_responses_preserve_correlation_until_valid_response_commits() {
        let mut codec = Codec::default();
        let request = codec
            .encode(json!({"jsonrpc":"2.0","id":7,"method":"ping"}))
            .unwrap();
        let valid = proto::Message {
            r#type: proto::MessageType::Response as i32,
            request_id: request.request_id,
            result: b"{}".to_vec(),
            ..Default::default()
        };
        for invalid in [
            proto::Message {
                method: "ping".into(),
                ..valid.clone()
            },
            proto::Message {
                params: b"{}".to_vec(),
                ..valid.clone()
            },
            proto::Message {
                error: Some(proto::Error::default()),
                ..valid.clone()
            },
            proto::Message {
                result: b"{".to_vec(),
                ..valid.clone()
            },
            proto::Message {
                result: vec![],
                error: Some(proto::Error {
                    code: -32602,
                    message: "invalid".into(),
                    data: b"{".to_vec(),
                }),
                ..valid.clone()
            },
        ] {
            assert!(codec.decode(invalid).is_err());
            assert_eq!(codec.pending.get(&valid.request_id), Some(&json!(7)));
        }
        assert_eq!(codec.decode(valid.clone()).unwrap()["id"], 7);
        assert!(codec.pending.is_empty());
        assert!(
            codec.decode(valid).is_err(),
            "duplicate response must be rejected"
        );
        let request = codec
            .encode(json!({"jsonrpc":"2.0","id":"failure","method":"ping"}))
            .unwrap();
        let error = codec
            .decode(proto::Message {
                r#type: proto::MessageType::Response as i32,
                request_id: request.request_id,
                error: Some(proto::Error {
                    code: -32602,
                    message: "invalid".into(),
                    data: br#"[1,{"why":"bad"}]"#.to_vec(),
                }),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(error["id"], "failure");
        assert_eq!(
            error["error"],
            json!({"code":-32602,"message":"invalid","data":[1,{"why":"bad"}]})
        );
        assert!(codec.pending.is_empty());

        // Preserve the pinned protobuf convention: absent result bytes mean {}.
        let request = codec
            .encode(json!({"jsonrpc":"2.0","id":"empty","method":"ping"}))
            .unwrap();
        assert_eq!(
            codec
                .decode(proto::Message {
                    r#type: proto::MessageType::Response as i32,
                    request_id: request.request_id,
                    ..Default::default()
                })
                .unwrap()["result"],
            json!({})
        );
    }

    #[test]
    fn ids_keep_types_and_cancellation_targets_the_wire_id() {
        let mut codec = Codec::default();
        let numeric = codec
            .encode(json!({"jsonrpc":"2.0","id":1,"method":"ping"}))
            .unwrap();
        let string = codec
            .encode(json!({"jsonrpc":"2.0","id":"1","method":"ping"}))
            .unwrap();
        assert_ne!(numeric.request_id, string.request_id);
        let cancellation = codec.encode(json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":1}})).unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&cancellation.params).unwrap()["requestId"],
            numeric.request_id
        );
        for (message, id) in [(numeric, json!(1)), (string, json!("1"))] {
            let response = codec
                .decode(proto::Message {
                    r#type: proto::MessageType::Response as i32,
                    request_id: message.request_id,
                    result: b"{}".to_vec(),
                    ..Default::default()
                })
                .unwrap();
            assert_eq!(response["id"], id);
        }
        assert!(codec.pending.is_empty());
    }
    #[test]
    fn subscription_references_restore_types_until_terminal_response() {
        let mut codec = Codec::default();
        for id in [json!(7), json!("7"), json!("n:7")] {
            let request = codec
                .encode(
                    json!({"jsonrpc":"2.0","id":id,"method":"subscriptions/listen","params":{}}),
                )
                .unwrap();
            for method in [
                "notifications/subscriptions/acknowledged",
                "notifications/resources/updated",
                "notifications/tasks/status",
            ] {
                let params = json!({"_meta":{"io.modelcontextprotocol/subscriptionId":request.request_id},"requestId":request.request_id});
                let decoded = codec
                    .decode(proto::Message {
                        r#type: proto::MessageType::Notification as i32,
                        method: method.into(),
                        params: serde_json::to_vec(&params).unwrap(),
                        ..Default::default()
                    })
                    .unwrap();
                assert_eq!(
                    decoded["params"]["_meta"]["io.modelcontextprotocol/subscriptionId"],
                    id
                );
                assert_eq!(decoded["params"]["requestId"], json!(request.request_id));
                assert!(codec.pending.contains_key(&request.request_id));
            }
            let result = json!({"_meta":{"io.modelcontextprotocol/subscriptionId":request.request_id},"resultType":"complete"});
            let decoded = codec
                .decode(proto::Message {
                    r#type: proto::MessageType::Response as i32,
                    request_id: request.request_id,
                    result: serde_json::to_vec(&result).unwrap(),
                    ..Default::default()
                })
                .unwrap();
            assert_eq!(
                decoded["result"]["_meta"]["io.modelcontextprotocol/subscriptionId"],
                id
            );
            assert!(codec.pending.is_empty());
            assert!(codec.subscriptions.is_empty());
        }
    }

    #[test]
    fn opaque_peer_references_and_unrelated_requests_are_not_rewritten() {
        for is_client in [false, true] {
            let mut codec = Codec {
                is_client,
                ..Default::default()
            };
            let request = codec
                .encode(json!({"jsonrpc":"2.0","id":7,"method":"ping"}))
                .unwrap();
            let params = json!({"_meta":{"io.modelcontextprotocol/subscriptionId":request.request_id}, "requestId":request.request_id});
            let decoded = codec
                .decode(proto::Message {
                    r#type: proto::MessageType::Notification as i32,
                    method: "notifications/cancelled".into(),
                    params: serde_json::to_vec(&params).unwrap(),
                    ..Default::default()
                })
                .unwrap();
            assert_eq!(decoded["params"], params);
        }
        let mut server = Codec {
            is_client: false,
            ..Default::default()
        };
        let incoming = "n:7";
        let cancellation = server.encode(json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":incoming}})).unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&cancellation.params).unwrap()["requestId"],
            incoming
        );
        let mut client = Codec::default();
        client
            .encode(json!({"jsonrpc":"2.0","id":7,"method":"subscriptions/listen"}))
            .unwrap();
        let unknown =
            json!({"_meta":{"io.modelcontextprotocol/subscriptionId":"s:n:7"},"requestId":"s:n:7"});
        let decoded = client
            .decode(proto::Message {
                r#type: proto::MessageType::Notification as i32,
                method: "notifications/cancelled".into(),
                params: serde_json::to_vec(&unknown).unwrap(),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(decoded["params"], unknown);
    }

    #[test]
    fn cancellation_releases_admission_and_allows_one_valid_late_response() {
        let mut codec = Codec::default();
        for id in 0..1100 {
            let wire = codec
                .encode(json!({"jsonrpc":"2.0","id":id,"method":"ping"}))
                .unwrap();
            codec.encode(json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":id}})).unwrap();
            assert!(codec.pending.is_empty());
            assert!(codec.retired.len() <= 1024);
            if id == 1099 {
                let malformed = proto::Message {
                    r#type: proto::MessageType::Response as i32,
                    request_id: wire.request_id.clone(),
                    result: b"{".to_vec(),
                    ..Default::default()
                };
                assert!(codec.decode(malformed).is_err());
                let valid = proto::Message {
                    r#type: proto::MessageType::Response as i32,
                    request_id: wire.request_id,
                    result: b"{}".to_vec(),
                    ..Default::default()
                };
                assert_eq!(codec.decode(valid.clone()).unwrap()["id"], id);
                assert!(codec.decode(valid).is_err());
            }
        }
        assert!(
            codec
                .decode(proto::Message {
                    r#type: proto::MessageType::Response as i32,
                    request_id: "n:0".into(),
                    result: b"{}".to_vec(),
                    ..Default::default()
                })
                .is_err()
        );
    }

    #[test]
    fn server_cancellation_restores_and_retires_only_local_subscriptions() {
        let mut client = Codec::default();
        let wire = client
            .encode(json!({"jsonrpc":"2.0","id":7,"method":"subscriptions/listen"}))
            .unwrap();
        let event = proto::Message {
            r#type: proto::MessageType::Notification as i32,
            method: "notifications/cancelled".into(),
            params: serde_json::to_vec(&json!({"requestId":wire.request_id})).unwrap(),
            ..Default::default()
        };
        assert_eq!(client.decode(event).unwrap()["params"]["requestId"], 7);
        assert!(client.pending.is_empty());
        assert!(client.subscriptions.is_empty());
        assert_eq!(client.retired.len(), 1);
        assert_eq!(
            client
                .decode(proto::Message {
                    r#type: proto::MessageType::Response as i32,
                    request_id: wire.request_id,
                    result: b"{}".to_vec(),
                    ..Default::default()
                })
                .unwrap()["id"],
            7
        );
        assert!(client.retired.is_empty());
    }

    #[test]
    fn errors_keep_arbitrary_json_data_and_escaped_ids() {
        let mut codec = Codec::default();
        let id = "quote\"slash\\newline\n";
        let wire = codec.encode(json!({"jsonrpc":"2.0","id":id,"error":{"code":-32602,"message":"invalid","data":[1,{"why":"bad"}]}})).unwrap();
        assert_eq!(wire.request_id, id);
        assert_eq!(
            serde_json::from_slice::<Value>(&wire.error.unwrap().data).unwrap(),
            json!([1,{"why":"bad"}])
        );
        assert!(codec.decode(proto::Message::default()).is_err());
        assert!(
            codec
                .decode(proto::Message {
                    r#type: proto::MessageType::Response as i32,
                    request_id: "unknown".into(),
                    ..Default::default()
                })
                .is_err()
        );
    }
}
