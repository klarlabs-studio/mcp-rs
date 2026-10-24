// Derived from rmcp 3.5.1; maintained and modified by mcp-rs. See ../../../UPSTREAM.md.
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use futures::{
    Stream, StreamExt,
    future::BoxFuture,
    stream::{BoxStream, FuturesUnordered},
};
use http::{HeaderName, HeaderValue, StatusCode};
pub use sse_stream::Error as SseError;
use sse_stream::Sse;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use super::common::client_side_sse::{
    DEFAULT_MAX_SSE_EVENT_SIZE, ExponentialBackoff, SseRetryPolicy, SseStreamReconnect,
};
use crate::{
    RoleClient,
    model::{
        ClientJsonRpcMessage, ClientNotification, ClientRequest, ErrorData, GetExtensions, GetMeta,
        InitializedNotification, JsonObject, ProtocolVersion, RequestId, ServerJsonRpcMessage,
        ServerNotification, ServerResult,
    },
    service::{InboundStreamOrigin, OutboundRequestLifetime},
    transport::{
        common::{client_side_sse::SseAutoReconnectStream, mcp_headers},
        worker::{
            RequestCancellationRegistration, Worker, WorkerQuitReason, WorkerSendRequest,
            WorkerTransport,
        },
    },
};

type BoxedSseStream = BoxStream<'static, Result<Sse, SseError>>;
type SseTaskResult<E> = (Option<RequestId>, Result<(), StreamableHttpError<E>>);
const SESSION_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
// Connection reuse is best effort after the final response. Bound trailing work
// even when a backend produces ready messages without yielding to the runtime.
const MAX_RESPONSE_DRAIN_MESSAGES: usize = 32;

const DEFAULT_MAX_ROUTING_SCHEMAS: usize = 1024;
const DEFAULT_MAX_ROUTING_SCHEMA_BYTES: usize = 4 * 1024 * 1024;

struct RetainedToolSchema {
    schema: Arc<JsonObject>,
    bytes: usize,
}

// Measure compact JSON without allocating an additional serialized copy. Stop
// counting as soon as one entry cannot fit, before evicting any unrelated entry.
struct RoutingSchemaSize {
    bytes: usize,
    limit: usize,
}

impl std::io::Write for RoutingSchemaSize {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(buffer.len())
            .filter(|bytes| *bytes <= self.limit)
            .ok_or_else(|| std::io::Error::other("routing schema byte budget exceeded"))?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Only routing projections are retained; discovery results keep their full schemas.
struct ToolHeaderCache {
    schemas: HashMap<String, RetainedToolSchema>,
    discovery_order: VecDeque<String>,
    capacity: usize,
    byte_capacity: usize,
    retained_bytes: usize,
    generation: Arc<()>,
    discoveries: HashMap<RequestId, Arc<()>>,
}

impl ToolHeaderCache {
    fn new(capacity: usize, byte_capacity: usize) -> Self {
        Self {
            schemas: HashMap::new(),
            discovery_order: VecDeque::new(),
            capacity,
            byte_capacity,
            retained_bytes: 0,
            generation: Arc::new(()),
            discoveries: HashMap::new(),
        }
    }

    fn track_discovery(&mut self, id: RequestId) {
        self.discoveries.insert(id, self.generation.clone());
    }

    fn finish_discovery(&mut self, id: &RequestId) -> bool {
        let generation = self.discoveries.remove(id).or_else(|| {
            id.numeric_string_value()
                .and_then(|number| self.discoveries.remove(&RequestId::Number(number)))
        });
        generation.is_some_and(|generation| Arc::ptr_eq(&generation, &self.generation))
    }

    fn invalidate(&mut self) {
        self.schemas.clear();
        self.retained_bytes = 0;
        self.discovery_order.clear();
        // Retain in-flight generations until completion; an older response must
        // still be validated, but cannot change the current routing contracts.
        self.generation = Arc::new(());
    }

    fn get(&self, name: &str) -> Option<&Arc<JsonObject>> {
        self.schemas.get(name).map(|entry| &entry.schema)
    }

    fn remove(&mut self, name: &str) {
        if let Some(entry) = self.schemas.remove(name) {
            self.retained_bytes -= entry.bytes;
            self.discovery_order.retain(|entry| entry != name);
        }
    }

    // Called only after the complete incoming schema has passed validation.
    fn insert(&mut self, name: String, schema: &JsonObject) {
        self.remove(&name);
        if self.capacity == 0 || self.byte_capacity == 0 || name.len() > self.byte_capacity {
            return;
        }
        let properties = schema
            .get("properties")
            .and_then(serde_json::Value::as_object)
            .into_iter()
            .flatten()
            .filter_map(|(name, property)| {
                let annotation = property.get("x-mcp-header")?;
                let mut projected = JsonObject::new();
                projected.insert("x-mcp-header".into(), annotation.clone());
                if let Some(kind) = property.get("type") {
                    projected.insert("type".into(), kind.clone());
                }
                Some((name.clone(), serde_json::Value::Object(projected)))
            })
            .collect::<JsonObject>();
        if properties.is_empty() {
            return;
        }
        let mut projected = JsonObject::new();
        if schema.get("type").and_then(serde_json::Value::as_str) == Some("object") {
            projected.insert("type".into(), "object".into());
        }
        projected.insert("properties".into(), serde_json::Value::Object(properties));
        let mut size = RoutingSchemaSize {
            bytes: name.len(),
            limit: self.byte_capacity,
        };
        if serde_json::to_writer(&mut size, &projected).is_err() {
            return;
        }
        while self.schemas.len() >= self.capacity
            || self.retained_bytes > self.byte_capacity - size.bytes
        {
            let Some(oldest) = self.discovery_order.pop_front() else {
                break;
            };
            if let Some(entry) = self.schemas.remove(&oldest) {
                self.retained_bytes -= entry.bytes;
            }
        }
        self.retained_bytes += size.bytes;
        self.discovery_order.push_back(name.clone());
        self.schemas.insert(
            name,
            RetainedToolSchema {
                schema: Arc::new(projected),
                bytes: size.bytes,
            },
        );
    }
}

#[cfg(test)]
impl<const N: usize> From<[(String, Arc<JsonObject>); N]> for ToolHeaderCache {
    fn from(entries: [(String, Arc<JsonObject>); N]) -> Self {
        let mut cache = Self::new(
            DEFAULT_MAX_ROUTING_SCHEMAS,
            DEFAULT_MAX_ROUTING_SCHEMA_BYTES,
        );
        for (name, schema) in entries {
            cache.insert(name, &schema);
        }
        cache
    }
}

fn build_request_headers(
    base: &HashMap<HeaderName, HeaderValue>,
    message: &ClientJsonRpcMessage,
    tool_cache: &ToolHeaderCache,
    version: &ProtocolVersion,
) -> HashMap<HeaderName, HeaderValue> {
    use serde_json::Value;

    let mut headers = base.clone();
    if *version >= ProtocolVersion::STANDARD_HEADERS
        && let Ok(value) = serde_json::to_value(message)
    {
        // HeaderName normalizes names to lowercase. Routing values describe this
        // message, never transport-wide defaults, including absent arguments.
        headers.retain(|name, _| {
            !matches!(name.as_str(), "mcp-method" | "mcp-name")
                && !name.as_str().starts_with("mcp-param-")
        });
        let schema = value
            .get("method")
            .and_then(Value::as_str)
            .filter(|method| *method == "tools/call")
            .and_then(|_| value.get("params"))
            .and_then(|params| params.get("name"))
            .and_then(Value::as_str)
            .and_then(|name| tool_cache.get(name))
            .map(Arc::as_ref);
        for (name, val) in mcp_headers::standard_request_headers(&value, schema) {
            headers.insert(name, val);
        }
    }
    headers
}

fn request_version_headers(
    base: &HashMap<HeaderName, HeaderValue>,
    message: &ClientJsonRpcMessage,
    fallback: &ProtocolVersion,
    tool_cache: &ToolHeaderCache,
) -> (ProtocolVersion, HashMap<HeaderName, HeaderValue>) {
    let version = match message {
        ClientJsonRpcMessage::Request(request) => request
            .request
            .get_meta()
            .protocol_version()
            .unwrap_or_else(|| fallback.clone()),
        _ => fallback.clone(),
    };
    let mut headers = build_request_headers(base, message, tool_cache, &version);
    if let Ok(value) = HeaderValue::from_str(version.as_str()) {
        headers.insert(HeaderName::from_static("mcp-protocol-version"), value);
    }
    (version, headers)
}

fn cache_tools_from_response(
    cache: &mut ToolHeaderCache,
    message: &mut ServerJsonRpcMessage,
    protocol_version: &ProtocolVersion,
) {
    if protocol_version < &ProtocolVersion::STANDARD_HEADERS {
        return;
    }
    if matches!(message, ServerJsonRpcMessage::Notification(notification)
        if matches!(notification.notification, ServerNotification::ToolListChangedNotification(_)))
    {
        cache.invalidate();
        return;
    }
    let response_id = match &*message {
        ServerJsonRpcMessage::Response(response) => Some(&response.id),
        ServerJsonRpcMessage::Error(error) => error.id.as_ref(),
        _ => None,
    };
    let current_discovery = response_id.is_some_and(|id| cache.finish_discovery(id));
    if let ServerJsonRpcMessage::Response(response) = message
        && let ServerResult::ListToolsResult(list) = &mut response.result
    {
        list.tools.retain(|tool| {
            let Some(schema) = &tool.input_schema else {
                // Omission does not replace a previously learned full schema.
                return true;
            };
            if let Err(reason) = mcp_headers::validate_param_header_annotations(schema) {
                tracing::warn!(tool = %tool.name, "rejecting invalid x-mcp-header annotations: {reason}");
                // An invalid refresh must retire a previously learned routing
                // contract, rather than silently continuing to send old headers.
                if current_discovery {
                    cache.remove(tool.name.as_ref());
                }
                false
            } else if current_discovery {
                cache.insert(tool.name.to_string(), schema);
                true
            } else {
                true
            }
        });
    }
}

fn negotiate_version_headers(
    init_response: &ServerJsonRpcMessage,
    base: HashMap<HeaderName, HeaderValue>,
) -> (ProtocolVersion, HashMap<HeaderName, HeaderValue>) {
    let mut version = ProtocolVersion::default();
    let mut headers = base;
    if let ServerJsonRpcMessage::Response(response) = init_response
        && let ServerResult::InitializeResult(init_result) = &response.result
    {
        version = init_result.protocol_version.clone();
        // HeaderName::from_static requires lowercase
        if let Ok(hv) = HeaderValue::from_str(init_result.protocol_version.as_str()) {
            headers.insert(HeaderName::from_static("mcp-protocol-version"), hv);
        }
    }
    (version, headers)
}

#[derive(Debug, Error)]
#[error("authorization required: {www_authenticate_header}")]
#[non_exhaustive]
pub struct AuthRequiredError {
    pub www_authenticate_header: String,
}

impl AuthRequiredError {
    /// Create a new `AuthRequiredError` instance.
    pub fn new(www_authenticate_header: String) -> Self {
        Self {
            www_authenticate_header,
        }
    }
}

#[derive(Debug, Error)]
#[error("insufficient scope: {www_authenticate_header}")]
#[non_exhaustive]
pub struct InsufficientScopeError {
    pub www_authenticate_header: String,
    pub required_scope: Option<String>,
}

impl InsufficientScopeError {
    /// Create a new `InsufficientScopeError` instance.
    pub fn new(www_authenticate_header: String, required_scope: Option<String>) -> Self {
        Self {
            www_authenticate_header,
            required_scope,
        }
    }

    /// check if scope upgrade is possible (i.e., we know what scope is required)
    pub fn can_upgrade(&self) -> bool {
        self.required_scope.is_some()
    }

    /// get the required scope for upgrade
    pub fn get_required_scope(&self) -> Option<&str> {
        self.required_scope.as_deref()
    }
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum StreamableHttpError<E: std::error::Error + Send + Sync + 'static> {
    #[error("SSE error: {0}")]
    Sse(#[from] SseError),
    #[error("Io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Client error: {0}")]
    Client(E),
    #[error("unexpected end of stream")]
    UnexpectedEndOfStream,
    /// A buffered HTTP response exceeded the configured decoded-byte limit.
    #[error("HTTP response body exceeds the {limit}-byte limit")]
    ResponseBodyTooLarge { limit: usize },
    #[error("unexpected server response: {0}")]
    UnexpectedServerResponse(Cow<'static, str>),
    #[error("Unexpected content type: {0:?}")]
    UnexpectedContentType(Option<String>),
    #[error("Server does not support SSE")]
    ServerDoesNotSupportSse,
    #[error("Server does not support delete session")]
    ServerDoesNotSupportDeleteSession,
    #[error("Tokio join error: {0}")]
    TokioJoinError(#[from] tokio::task::JoinError),
    #[error("Deserialize error: {0}")]
    Deserialize(#[from] serde_json::Error),
    #[error("Transport channel closed")]
    TransportChannelClosed,
    #[error("Missing session id in HTTP response")]
    MissingSessionIdInResponse,
    #[cfg(feature = "auth")]
    #[error("Auth error: {0}")]
    Auth(#[from] crate::transport::auth::AuthError),
    #[error("Auth required")]
    AuthRequired(#[source] AuthRequiredError),
    #[error("Insufficient scope")]
    InsufficientScope(#[source] InsufficientScopeError),
    #[error("Header name '{0}' is reserved and conflicts with default headers")]
    ReservedHeaderConflict(String),
    #[error("Session expired (HTTP 404)")]
    SessionExpired,
    /// Session recovery timed out. The server may have processed an interrupted POST.
    #[error("Session recovery timed out; the server may have processed the POST")]
    SessionRecoveryTimeout,
    /// A cancellation or reply POST did not finish in time.
    #[error("Control POST timed out")]
    ControlRequestTimeout,
}

/// Bytes of an unusable JSON body kept in
/// [`StreamableHttpError::UnexpectedServerResponse`].
pub(crate) const JSON_RPC_BODY_PREVIEW_LEN: usize = 256;

/// Short, lossy view of a body that failed to parse as JSON-RPC.
pub(crate) fn json_rpc_body_preview(body: &[u8]) -> Cow<'static, str> {
    if body.is_empty() {
        return Cow::Borrowed("<empty>");
    }
    let lossy = String::from_utf8_lossy(body);
    if lossy.len() <= JSON_RPC_BODY_PREVIEW_LEN {
        return Cow::Owned(lossy.into_owned());
    }
    let mut end = JSON_RPC_BODY_PREVIEW_LEN;
    while end > 0 && !lossy.is_char_boundary(end) {
        end -= 1;
    }
    Cow::Owned(lossy[..end].to_owned())
}

/// Request-POST parse failure, including a body preview so empty / HTML / `{}`
/// are distinguishable from the serde diagnostic alone.
pub(crate) fn json_rpc_parse_error_message(err: &serde_json::Error, body: &[u8]) -> String {
    format!(
        "could not parse JSON response as ServerJsonRpcMessage: {err}: {}",
        json_rpc_body_preview(body)
    )
}

impl<E: std::error::Error + Send + Sync + 'static> StreamableHttpError<E> {
    /// The `WWW-Authenticate` challenge carried by this error, when the
    /// server answered 401 ([`AuthRequired`](Self::AuthRequired)) or 403
    /// ([`InsufficientScope`](Self::InsufficientScope)). Feed it to
    /// [`AuthorizationRequest::with_challenge`](crate::transport::auth::AuthorizationRequest::with_challenge)
    /// to authorize reactively.
    #[cfg(feature = "auth")]
    pub fn auth_challenge(&self) -> Option<&str> {
        match self {
            Self::AuthRequired(error) => Some(&error.www_authenticate_header),
            Self::InsufficientScope(error) => Some(&error.www_authenticate_header),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Error)]
#[non_exhaustive]
pub enum StreamableHttpProtocolError {
    #[error("Missing session id in response")]
    MissingSessionIdInResponse,
}

#[expect(
    clippy::large_enum_variant,
    reason = "boxing the streaming response would add an allocation to the common response path"
)]
#[non_exhaustive]
pub enum StreamableHttpPostResponse {
    Accepted,
    Json(ServerJsonRpcMessage, Option<String>),
    Sse(BoxedSseStream, Option<String>),
}

impl std::fmt::Debug for StreamableHttpPostResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Accepted => write!(f, "Accepted"),
            Self::Json(arg0, arg1) => f.debug_tuple("Json").field(arg0).field(arg1).finish(),
            Self::Sse(_, arg1) => f.debug_tuple("Sse").field(arg1).finish(),
        }
    }
}

impl StreamableHttpPostResponse {
    pub async fn expect_initialized<E>(
        self,
    ) -> Result<(ServerJsonRpcMessage, Option<String>), StreamableHttpError<E>>
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        match self {
            Self::Json(message, session_id) => Ok((message, session_id)),
            Self::Sse(mut stream, session_id) => {
                while let Some(event) = stream.next().await {
                    let event = event?;
                    let payload = event.data.unwrap_or_default();
                    if payload.trim().is_empty() {
                        continue;
                    }

                    let message: ServerJsonRpcMessage = serde_json::from_str(&payload)?;

                    if matches!(message, ServerJsonRpcMessage::Response(_)) {
                        return Ok((message, session_id));
                    }

                    debug!(
                        ?message,
                        "received message before initialize response; continuing to drain stream"
                    );
                }

                Err(StreamableHttpError::UnexpectedServerResponse(
                    "empty sse stream".into(),
                ))
            }
            _ => Err(StreamableHttpError::UnexpectedServerResponse(
                "expect initialized, accepted".into(),
            )),
        }
    }

    pub fn expect_json<E>(self) -> Result<ServerJsonRpcMessage, StreamableHttpError<E>>
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        match self {
            Self::Json(message, ..) => Ok(message),
            got => Err(StreamableHttpError::UnexpectedServerResponse(
                format!("expect json, got {got:?}").into(),
            )),
        }
    }

    pub fn expect_accepted_or_json<E>(self) -> Result<(), StreamableHttpError<E>>
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        match self {
            Self::Accepted => Ok(()),
            // Tolerate servers that return 200 with JSON for notifications
            Self::Json(..) => Ok(()),
            got => Err(StreamableHttpError::UnexpectedServerResponse(
                format!("expect accepted or json, got {got:?}").into(),
            )),
        }
    }
}

/// Convert a sessionless discovery rejection into a response the lifecycle
/// layer can classify. The server's own JSON-RPC error is preserved so the
/// lifecycle can tell a modern rejection (retry at a supported version) from a
/// legacy one; `invalid_request` is synthesized only when the body has no error
/// to keep, as with an empty or plain-text 4xx from middleware.
///
/// The id is re-correlated because such middleware rejections cannot echo it
/// (the Python SDK sends the literal `"server-error"`); otherwise the lifecycle
/// discards the response as uncorrelated without classifying it.
///
/// Keep authentication failures and server errors on their original paths.
pub(super) fn legacy_discover_response(
    message: &ClientJsonRpcMessage,
    session_was_attached: bool,
    status: StatusCode,
    body: &str,
) -> Option<StreamableHttpPostResponse> {
    if session_was_attached
        || !status.is_client_error()
        || matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN)
    {
        return None;
    }

    let ClientJsonRpcMessage::Request(request) = message else {
        return None;
    };
    if !matches!(request.request, ClientRequest::DiscoverRequest(_)) {
        return None;
    }

    let error = match serde_json::from_str::<ServerJsonRpcMessage>(body) {
        Ok(ServerJsonRpcMessage::Error(error)) => error.error,
        _ => ErrorData::invalid_request(
            format!("server/discover rejected with HTTP {status}: {body}"),
            None,
        ),
    };
    Some(StreamableHttpPostResponse::Json(
        ServerJsonRpcMessage::error(error, Some(request.id.clone())),
        None,
    ))
}

/// Byte limits passed to an HTTP client's POST implementation.
///
/// SSE events and buffered response bodies have independent 16 MiB defaults.
/// A zero limit rejects nonempty content of that kind. The body limit applies
/// to decoded response bytes, including HTTP error bodies, before JSON parsing.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct StreamableHttpClientLimits {
    /// Maximum raw bytes in one SSE event.
    pub max_sse_event_size: usize,
    /// Maximum decoded bytes collected from a non-SSE response body.
    pub max_response_body_size: usize,
}

impl Default for StreamableHttpClientLimits {
    fn default() -> Self {
        Self {
            max_sse_event_size: DEFAULT_MAX_SSE_EVENT_SIZE,
            max_response_body_size: 16 * 1024 * 1024,
        }
    }
}

/// HTTP backend used by [`StreamableHttpClientTransport`].
///
/// Custom implementations must override [`Self::post_message_with_limits`]
/// and [`Self::get_stream_with_max_sse_event_size`] to enforce the transport's
/// configured body and event-size limits.
///
/// For legacy http, the transport keeps an open response stream alive until
/// its cancellation send finishes or is dropped. This lets a custom client
/// handle the cancellation using state owned by that stream.
pub trait StreamableHttpClient: Clone + Send + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    fn post_message(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> impl Future<Output = Result<StreamableHttpPostResponse, StreamableHttpError<Self::Error>>>
    + Send
    + '_;
    /// Send a message while enforcing a maximum raw SSE event size.
    ///
    /// `max_sse_event_size` is not a per-request option: it is the
    /// transport-wide [`StreamableHttpClientTransportConfig::max_sse_event_size`]
    /// value, passed identically on every call because the limit must be applied
    /// inside the client (at the raw byte layer, before SSE parsing) rather than
    /// by the caller.
    ///
    /// Custom clients that parse SSE responses should override this method.
    /// The default implementation delegates to [`Self::post_message`].
    fn post_message_with_max_sse_event_size(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
        _max_sse_event_size: usize,
    ) -> impl Future<Output = Result<StreamableHttpPostResponse, StreamableHttpError<Self::Error>>>
    + Send
    + '_ {
        self.post_message(uri, message, session_id, auth_header, custom_headers)
    }
    /// Send a POST with the transport's SSE and buffered-body byte limits.
    ///
    /// Custom backends must override this method to enforce the response-body
    /// limit while reading decoded bytes, before buffering or parsing JSON.
    /// The default delegates to [`Self::post_message_with_max_sse_event_size`]
    /// and does not enforce the buffered-body limit. The built-in reqwest client
    /// enforces both limits. Wrappers must forward them to the underlying backend.
    fn post_message_with_limits(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
        limits: StreamableHttpClientLimits,
    ) -> impl Future<Output = Result<StreamableHttpPostResponse, StreamableHttpError<Self::Error>>>
    + Send
    + '_ {
        self.post_message_with_max_sse_event_size(
            uri,
            message,
            session_id,
            auth_header,
            custom_headers,
            limits.max_sse_event_size,
        )
    }

    fn delete_session(
        &self,
        uri: Arc<str>,
        session_id: Arc<str>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> impl Future<Output = Result<(), StreamableHttpError<Self::Error>>> + Send + '_;
    /// Open an SSE stream, optionally scoped to a legacy session.
    ///
    /// `session_id` is `None` when resuming a stateless response using only
    /// `last_event_id`.
    fn get_stream(
        &self,
        uri: Arc<str>,
        session_id: Option<Arc<str>>,
        last_event_id: Option<String>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> impl Future<
        Output = Result<
            BoxStream<'static, Result<Sse, SseError>>,
            StreamableHttpError<Self::Error>,
        >,
    > + Send
    + '_;
    /// Open an SSE stream while enforcing a maximum raw event size.
    ///
    /// `max_sse_event_size` is not a per-request option: it is the
    /// transport-wide [`StreamableHttpClientTransportConfig::max_sse_event_size`]
    /// value, passed identically on every call because the limit must be applied
    /// inside the client (at the raw byte layer, before SSE parsing) rather than
    /// by the caller.
    ///
    /// Custom clients that parse SSE responses should override this method.
    /// The default implementation delegates to [`Self::get_stream`].
    fn get_stream_with_max_sse_event_size(
        &self,
        uri: Arc<str>,
        session_id: Option<Arc<str>>,
        last_event_id: Option<String>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
        _max_sse_event_size: usize,
    ) -> impl Future<
        Output = Result<
            BoxStream<'static, Result<Sse, SseError>>,
            StreamableHttpError<Self::Error>,
        >,
    > + Send
    + '_ {
        self.get_stream(uri, session_id, last_event_id, auth_header, custom_headers)
    }
}

#[non_exhaustive]
pub struct RetryConfig {
    pub max_times: Option<usize>,
    pub min_duration: Duration,
}

struct StreamableHttpClientReconnect<C> {
    pub client: C,
    pub session_id: Option<Arc<str>>,
    pub uri: Arc<str>,
    pub auth_header: Option<String>,
    pub custom_headers: HashMap<HeaderName, HeaderValue>,
    pub max_sse_event_size: usize,
}

impl<C: StreamableHttpClient> SseStreamReconnect for StreamableHttpClientReconnect<C> {
    type Error = StreamableHttpError<C::Error>;
    type Future = BoxFuture<'static, Result<BoxedSseStream, Self::Error>>;
    fn retry_connection(&mut self, last_event_id: Option<&str>) -> Self::Future {
        let client = self.client.clone();
        let uri = self.uri.clone();
        let session_id = self.session_id.clone();
        let auth_header = self.auth_header.clone();
        let custom_headers = self.custom_headers.clone();
        let max_sse_event_size = self.max_sse_event_size;
        let last_event_id = last_event_id.map(|s| s.to_owned());
        Box::pin(async move {
            client
                .get_stream_with_max_sse_event_size(
                    uri,
                    session_id,
                    last_event_id,
                    auth_header,
                    custom_headers,
                    max_sse_event_size,
                )
                .await
        })
    }

    fn map_fatal_stream_error(&mut self, error: SseError) -> Option<Self::Error> {
        Some(StreamableHttpError::Sse(error))
    }
}

/// Info retained for cleaning up the session when the worker exits.
struct SessionCleanupInfo<C> {
    client: C,
    uri: Arc<str>,
    session_id: Arc<str>,
    auth_header: Option<String>,
    protocol_headers: HashMap<HeaderName, HeaderValue>,
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct StreamableHttpClientWorker<C: StreamableHttpClient> {
    pub client: C,
    pub config: StreamableHttpClientTransportConfig,
}

struct PostResult<C: StreamableHttpClient> {
    send_request: WorkerSendRequest<StreamableHttpClientWorker<C>>,
    // None means the send future was dropped, or the request or transport was cancelled.
    response: Option<Result<StreamableHttpPostResponse, StreamableHttpError<C::Error>>>,
    // The protocol version used to send this POST.
    version: ProtocolVersion,
}

struct PostSession {
    id: Option<Arc<str>>,
    headers: HashMap<HeaderName, HeaderValue>,
    version: ProtocolVersion,
    cancellation: CancellationToken,
}

impl<C: StreamableHttpClient + Default> StreamableHttpClientWorker<C> {
    pub fn new_simple(url: impl Into<Arc<str>>) -> Self {
        Self {
            client: C::default(),
            config: StreamableHttpClientTransportConfig {
                uri: url.into(),
                ..Default::default()
            },
        }
    }
}

impl<C: StreamableHttpClient> StreamableHttpClientWorker<C> {
    pub fn new(client: C, config: StreamableHttpClientTransportConfig) -> Self {
        Self { client, config }
    }
}

impl<C: StreamableHttpClient> StreamableHttpClientWorker<C> {
    // Run initialization and protocol-version changes without other ordinary POSTs.
    fn is_ordering_barrier(
        message: &ClientJsonRpcMessage,
        negotiated_version: &ProtocolVersion,
    ) -> bool {
        match message {
            ClientJsonRpcMessage::Request(request) => {
                matches!(
                    &request.request,
                    ClientRequest::InitializeRequest(_) | ClientRequest::DiscoverRequest(_)
                ) || request
                    .request
                    .get_meta()
                    .protocol_version()
                    .is_some_and(|version| &version != negotiated_version)
            }
            ClientJsonRpcMessage::Notification(notification) => matches!(
                &notification.notification,
                ClientNotification::InitializedNotification(_)
            ),
            _ => false,
        }
    }

    fn post_request(
        client: C,
        config: &StreamableHttpClientTransportConfig,
        mut send_request: WorkerSendRequest<Self>,
        session: PostSession,
        transport_cancellation: CancellationToken,
    ) -> BoxFuture<'static, PostResult<C>> {
        let uri = config.uri.clone();
        let auth_header = config.auth_header.clone();
        let limits = config.client_limits();
        let control_request_timeout = config.control_request_timeout;
        let is_control = Self::is_control_message(&send_request.message);
        let cancellation = send_request
            .cancellation_token()
            .unwrap_or_else(|| transport_cancellation.child_token());
        Box::pin(async move {
            let response = tokio::select! {
                biased;
                _ = cancellation.cancelled() => None,
                _ = send_request.responder.closed() => None,
                _ = session.cancellation.cancelled() => {
                    Some(Err(StreamableHttpError::SessionRecoveryTimeout))
                },
                _ = tokio::time::sleep(control_request_timeout), if is_control => {
                    Some(Err(StreamableHttpError::ControlRequestTimeout))
                },
                response = client.post_message_with_limits(
                    uri,
                    send_request.message.clone(),
                    session.id,
                    auth_header,
                    session.headers,
                    limits,
                ) => Some(response),
            };
            PostResult {
                send_request,
                response,
                version: session.version,
            }
        })
    }

    fn cancellation_request_id(message: &ClientJsonRpcMessage) -> Option<&RequestId> {
        match message {
            ClientJsonRpcMessage::Notification(notification) => match &notification.notification {
                ClientNotification::CancelledNotification(cancelled) => {
                    cancelled.params.request_id.as_ref()
                }
                _ => None,
            },
            _ => None,
        }
    }

    fn client_request_id(message: &ClientJsonRpcMessage) -> Option<RequestId> {
        match message {
            ClientJsonRpcMessage::Request(request) => Some(request.id.clone()),
            _ => None,
        }
    }

    fn server_response_id(message: &ServerJsonRpcMessage) -> Option<&RequestId> {
        match message {
            ServerJsonRpcMessage::Response(response) => Some(&response.id),
            ServerJsonRpcMessage::Error(error) => error.id.as_ref(),
            _ => None,
        }
    }

    fn clear_stream_response_pending(
        pending_stream_response_ids: &mut HashSet<RequestId>,
        message: &ServerJsonRpcMessage,
    ) -> Option<RequestId> {
        let response_id = Self::server_response_id(message)?;
        if let Some(id) = pending_stream_response_ids.take(response_id) {
            return Some(id);
        }
        let id = RequestId::Number(response_id.numeric_string_value()?);
        pending_stream_response_ids.take(&id)
    }

    fn prepare_stream_message(
        message: &mut ServerJsonRpcMessage,
        pending_stream_response_ids: &mut HashSet<RequestId>,
        request_stream_cancellations: &mut HashMap<RequestId, Arc<RequestCancellationRegistration>>,
        tool_header_cache: &mut ToolHeaderCache,
        protocol_version: &ProtocolVersion,
    ) {
        if let Some(request_id) =
            Self::clear_stream_response_pending(pending_stream_response_ids, message)
        {
            drop(request_stream_cancellations.remove(&request_id));
        }
        cache_tools_from_response(tool_header_cache, message, protocol_version);
    }

    async fn drain_queued_stream_messages(
        sse_worker_rx: &mut tokio::sync::mpsc::Receiver<ServerJsonRpcMessage>,
        to_handler: &tokio::sync::mpsc::Sender<ServerJsonRpcMessage>,
        pending_stream_response_ids: &mut HashSet<RequestId>,
        request_stream_cancellations: &mut HashMap<RequestId, Arc<RequestCancellationRegistration>>,
        tool_header_cache: &mut ToolHeaderCache,
        protocol_version: &ProtocolVersion,
    ) -> Result<(), WorkerQuitReason<StreamableHttpError<C::Error>>> {
        loop {
            match sse_worker_rx.try_recv() {
                Ok(mut message) => {
                    Self::prepare_stream_message(
                        &mut message,
                        pending_stream_response_ids,
                        request_stream_cancellations,
                        tool_header_cache,
                        protocol_version,
                    );
                    to_handler
                        .send(message)
                        .await
                        .map_err(|_| WorkerQuitReason::HandlerTerminated)?;
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => return Ok(()),
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => return Ok(()),
            }
        }
    }

    async fn fail_pending_stream_responses(
        context: &mut super::worker::WorkerContext<Self>,
        pending_stream_response_ids: &mut HashSet<RequestId>,
    ) -> Result<(), WorkerQuitReason<StreamableHttpError<C::Error>>> {
        if pending_stream_response_ids.is_empty() {
            return Ok(());
        }

        let pending_ids = std::mem::take(pending_stream_response_ids);
        for id in pending_ids {
            context
                .send_to_handler(ServerJsonRpcMessage::error(
                    ErrorData::internal_error(
                        "streamable HTTP session was re-initialized before the response arrived",
                        None,
                    ),
                    Some(id),
                ))
                .await?;
        }
        Ok(())
    }

    async fn fail_pending_responses_except_retries(
        context: &mut super::worker::WorkerContext<Self>,
        pending_stream_response_ids: &mut HashSet<RequestId>,
        recovery_posts: &VecDeque<WorkerSendRequest<Self>>,
    ) -> Result<(), WorkerQuitReason<StreamableHttpError<C::Error>>> {
        // Keep only retries that have not already received a stream response.
        let retry_ids: Vec<_> = recovery_posts
            .iter()
            .filter_map(|request| Self::client_request_id(&request.message))
            .filter(|id| pending_stream_response_ids.remove(id))
            .collect();
        Self::fail_pending_stream_responses(context, pending_stream_response_ids).await?;
        pending_stream_response_ids.extend(retry_ids);
        Ok(())
    }

    fn fail_recovery_posts(
        recovery_posts: &mut VecDeque<WorkerSendRequest<Self>>,
        pending_stream_response_ids: &mut HashSet<RequestId>,
        error: StreamableHttpError<C::Error>,
    ) {
        // The backend error cannot be cloned. Return it to one caller
        // and return the original session-expired error to the others.
        let mut recovery_error = Some(error);
        for send_request in recovery_posts.drain(..) {
            let pending = Self::client_request_id(&send_request.message)
                .is_none_or(|id| pending_stream_response_ids.remove(&id));
            let result = if pending {
                Err(recovery_error
                    .take()
                    .unwrap_or(StreamableHttpError::SessionExpired))
            } else {
                Ok(())
            };
            let _ = send_request.responder.send(result);
        }
    }

    /// Convert a request-scoped SSE response, resuming with Last-Event-ID.
    fn response_sse_to_jsonrpc(
        stream: BoxedSseStream,
        reconnect: StreamableHttpClientReconnect<C>,
        retry_config: Arc<dyn SseRetryPolicy>,
    ) -> BoxStream<'static, Result<ServerJsonRpcMessage, StreamableHttpError<C::Error>>> {
        SseAutoReconnectStream::new_after_event_id(stream, reconnect, retry_config).boxed()
    }

    async fn run_response_stream(
        mut sse_stream: BoxStream<
            'static,
            Result<ServerJsonRpcMessage, StreamableHttpError<C::Error>>,
        >,
        sse_worker_tx: tokio::sync::mpsc::Sender<ServerJsonRpcMessage>,
        origin: InboundStreamOrigin,
        request_ct: CancellationToken,
        stream_ct: CancellationToken,
        uses_modern_http: bool,
        caller_lifetime: CancellationToken,
    ) -> Result<(), StreamableHttpError<C::Error>> {
        tokio::select! {
            biased;
            _ = caller_lifetime.cancelled(), if uses_modern_http => {
                // The POST may already be acknowledged. Retire its response stream
                // without relying on a separate cancellation notification POST.
                request_ct.cancel();
                Ok(())
            }
            _ = request_ct.cancelled(), if !uses_modern_http => {
                // Stop reading, but keep the stream until the adapter
                // handles cancellation or the send is dropped.
                stream_ct.cancelled().await;
                Ok(())
            }
            result = Self::execute_sse_stream(
                sse_stream.as_mut(), sse_worker_tx, origin, true, stream_ct.clone(),
            ) => result,
        }
    }

    async fn execute_sse_stream(
        sse_stream: impl Stream<Item = Result<ServerJsonRpcMessage, StreamableHttpError<C::Error>>>
        + Send,
        sse_worker_tx: tokio::sync::mpsc::Sender<ServerJsonRpcMessage>,
        origin: InboundStreamOrigin,
        close_on_response: bool,
        ct: CancellationToken,
    ) -> Result<(), StreamableHttpError<C::Error>> {
        tokio::select! {
            biased;
            _ = ct.cancelled() => {
                tracing::debug!("cancelled");
                Ok(())
            }
            result = async move {
                let mut sse_stream = std::pin::pin!(sse_stream);
                loop {
                    let message = sse_stream.next().await;
                    let Some(mut message) = message.transpose()? else {
                        break;
                    };
                    // SEP-2260: mark inbound requests with the stream they arrived on
                    // for the client receive-side association check.
                    if let ServerJsonRpcMessage::Request(request) = &mut message {
                        request.request.extensions_mut().insert(origin.clone());
                    }
                    let is_response = matches!(
                        message,
                        ServerJsonRpcMessage::Response(_) | ServerJsonRpcMessage::Error(_)
                    );
                    let yield_result = sse_worker_tx.send(message).await;
                    if yield_result.is_err() {
                        tracing::trace!("streamable http transport worker dropped, exiting");
                        break;
                    }
                    if close_on_response && is_response {
                        tracing::debug!("got response, draining sse stream for connection reuse");
                        // Try to reach EOF for HTTP/1.1 connection reuse without
                        // following an unbounded tail or polling past an error.
                        let _ = tokio::time::timeout(std::time::Duration::from_millis(50), async {
                            for _ in 0..MAX_RESPONSE_DRAIN_MESSAGES {
                                match sse_stream.next().await {
                                    Some(Ok(_)) => tokio::task::yield_now().await,
                                    Some(Err(_)) | None => break,
                                }
                            }
                        })
                        .await;
                        break;
                    }
                }
                Ok(())
            } => result,
        }
    }

    fn spawn_common_stream(
        streams: &mut tokio::task::JoinSet<SseTaskResult<C::Error>>,
        client: C,
        session_id: Arc<str>,
        config: &StreamableHttpClientTransportConfig,
        protocol_headers: HashMap<HeaderName, HeaderValue>,
        sse_worker_tx: tokio::sync::mpsc::Sender<ServerJsonRpcMessage>,
        transport_task_ct: CancellationToken,
    ) {
        let uri = config.uri.clone();
        let auth_header = config.auth_header.clone();
        let retry_config = config.retry_config.clone();
        let reconnect_uri = config.uri.clone();
        let reconnect_auth_header = config.auth_header.clone();
        let max_sse_event_size = config.max_sse_event_size;

        streams.spawn(async move {
            let result = match client
                .get_stream_with_max_sse_event_size(
                    uri,
                    Some(session_id.clone()),
                    None,
                    auth_header,
                    protocol_headers.clone(),
                    max_sse_event_size,
                )
                .await
            {
                Ok(stream) => {
                    let sse_stream = SseAutoReconnectStream::new(
                        stream,
                        StreamableHttpClientReconnect {
                            client,
                            session_id: Some(session_id),
                            uri: reconnect_uri,
                            auth_header: reconnect_auth_header,
                            custom_headers: protocol_headers,
                            max_sse_event_size,
                        },
                        retry_config,
                    );
                    Self::execute_sse_stream(
                        sse_stream,
                        sse_worker_tx,
                        InboundStreamOrigin::Unassociated,
                        false,
                        transport_task_ct.child_token(),
                    )
                    .await
                }
                Err(StreamableHttpError::ServerDoesNotSupportSse) => {
                    tracing::debug!("server doesn't support sse, skip common stream");
                    Ok(())
                }
                Err(error) => {
                    tracing::error!("fail to get common stream: {error}");
                    Err(error)
                }
            };
            (None, result)
        });
    }

    /// Performs a transparent re-initialization handshake after a session-expired 404.
    ///
    /// Takes an owned clone of the client (avoiding `&self` across `.await` so the
    /// future remains `Send` without requiring `C: Sync`).  POSTs the saved
    /// initialize request without a session ID, extracts the new session ID and
    /// protocol version, sends `notifications/initialized`, and returns the new
    /// `(session_id, protocol_headers)` pair.  The init result message is **not**
    /// forwarded to the handler because the handler already processed the original
    /// initialization.
    async fn perform_reinitialization(
        client: C,
        saved_init_request: ClientJsonRpcMessage,
        uri: Arc<str>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
        limits: StreamableHttpClientLimits,
    ) -> Result<
        (
            Option<Arc<str>>,
            ProtocolVersion,
            HashMap<HeaderName, HeaderValue>,
        ),
        StreamableHttpError<C::Error>,
    > {
        let (init_msg, new_session_id_str) = client
            .post_message_with_limits(
                uri.clone(),
                saved_init_request,
                None,
                auth_header.clone(),
                custom_headers.clone(),
                limits,
            )
            .await?
            .expect_initialized::<C::Error>()
            .await?;

        let new_session_id: Option<Arc<str>> = new_session_id_str.map(|s| Arc::from(s.as_str()));

        let (negotiated_version, new_protocol_headers) =
            negotiate_version_headers(&init_msg, custom_headers);

        let initialized_notification = ClientJsonRpcMessage::notification(
            ClientNotification::InitializedNotification(InitializedNotification {
                method: Default::default(),
                extensions: Default::default(),
            }),
        );
        // SEP-2243: notifications carry no Mcp-Param-*, so an empty tool cache suffices.
        let initialized_headers = build_request_headers(
            &new_protocol_headers,
            &initialized_notification,
            &ToolHeaderCache::new(0, DEFAULT_MAX_ROUTING_SCHEMA_BYTES),
            &negotiated_version,
        );
        client
            .post_message_with_limits(
                uri,
                initialized_notification,
                new_session_id.clone(),
                auth_header,
                initialized_headers,
                limits,
            )
            .await?
            .expect_accepted_or_json::<C::Error>()?;

        Ok((new_session_id, negotiated_version, new_protocol_headers))
    }
}

impl<C: StreamableHttpClient> Worker for StreamableHttpClientWorker<C> {
    type Role = RoleClient;
    type Error = StreamableHttpError<C::Error>;
    fn is_control_message(message: &ClientJsonRpcMessage) -> bool {
        match message {
            ClientJsonRpcMessage::Response(_) | ClientJsonRpcMessage::Error(_) => true,
            ClientJsonRpcMessage::Notification(notification) => matches!(
                notification.notification,
                ClientNotification::CancelledNotification(_)
            ),
            ClientJsonRpcMessage::Request(_) => false,
        }
    }
    fn supports_request_cancellation() -> bool {
        true
    }
    fn err_closed() -> Self::Error {
        StreamableHttpError::TransportChannelClosed
    }
    fn err_join(e: tokio::task::JoinError) -> Self::Error {
        StreamableHttpError::TokioJoinError(e)
    }
    fn config(&self) -> super::worker::WorkerConfig {
        super::worker::WorkerConfig {
            name: Some("StreamableHttpClientWorker".into()),
            channel_buffer_capacity: self.config.channel_buffer_capacity,
        }
    }
    async fn run(
        self,
        mut context: super::worker::WorkerContext<Self>,
    ) -> Result<(), WorkerQuitReason<Self::Error>> {
        let channel_buffer_capacity = self.config.channel_buffer_capacity;
        let (sse_worker_tx, mut sse_worker_rx) =
            tokio::sync::mpsc::channel::<ServerJsonRpcMessage>(channel_buffer_capacity);
        let config = self.config.clone();
        let transport_task_ct = context.cancellation_token.clone();
        let _drop_guard = transport_task_ct.clone().drop_guard();
        let WorkerSendRequest {
            responder,
            message: startup_request,
            ..
        } = context.recv_from_handler().await?;
        let is_legacy_startup = matches!(
            &startup_request,
            ClientJsonRpcMessage::Request(request)
                if matches!(&request.request, ClientRequest::InitializeRequest(_))
        );
        if is_legacy_startup {
            let error = StreamableHttpError::UnexpectedServerResponse(
                "initialize is retired; use server/discover".into(),
            );
            let _ = responder.send(Err(error));
            return Err(WorkerQuitReason::fatal(
                StreamableHttpError::TransportChannelClosed,
                "retired lifecycle",
            ));
        }
        let mut saved_init_request = is_legacy_startup.then(|| startup_request.clone());
        let empty_tool_cache = ToolHeaderCache::new(0, DEFAULT_MAX_ROUTING_SCHEMA_BYTES);
        let (bootstrap_version, bootstrap_headers) = if is_legacy_startup {
            (ProtocolVersion::default(), config.custom_headers.clone())
        } else {
            request_version_headers(
                &config.custom_headers,
                &startup_request,
                &ProtocolVersion::default(),
                &empty_tool_cache,
            )
        };
        let (message, session_id) = match self
            .client
            .post_message_with_limits(
                config.uri.clone(),
                startup_request,
                None,
                config.auth_header.clone(),
                bootstrap_headers.clone(),
                config.client_limits(),
            )
            .await
        {
            Ok(res) => {
                let _ = responder.send(Ok(()));
                res.expect_initialized::<C::Error>().await.map_err(
                    WorkerQuitReason::fatal_context("process initialize response"),
                )?
            }
            Err(err) => {
                let msg = format!("{:?}", err);
                let _ = responder.send(Err(err));
                return Err(WorkerQuitReason::fatal(
                    StreamableHttpError::TransportChannelClosed,
                    msg,
                ));
            }
        };
        let mut uses_modern_http = !is_legacy_startup;
        let mut session_id: Option<Arc<str>> = if uses_modern_http {
            None
        } else if let Some(session_id) = session_id {
            Some(session_id.into())
        } else {
            if !self.config.allow_stateless {
                return Err(WorkerQuitReason::fatal(
                    StreamableHttpError::<C::Error>::MissingSessionIdInResponse,
                    "process initialize response",
                ));
            }
            None
        };
        let (mut negotiated_version, mut protocol_headers) = if is_legacy_startup {
            negotiate_version_headers(&message, config.custom_headers.clone())
        } else {
            (bootstrap_version, bootstrap_headers)
        };
        // SEP-2243: tool input schemas (name -> schema) cached from tools/list responses,
        // used to promote annotated tools/call arguments to Mcp-Param-* headers.
        let mut tool_header_cache =
            ToolHeaderCache::new(config.max_routing_schemas, config.max_routing_schema_bytes);

        // Store session info for cleanup when run() exits (not spawned, so cleanup completes before close() returns)
        let mut session_cleanup_info = session_id.as_ref().map(|sid| SessionCleanupInfo {
            client: self.client.clone(),
            uri: config.uri.clone(),
            session_id: sid.clone(),
            auth_header: config.auth_header.clone(),
            protocol_headers: protocol_headers.clone(),
        });

        context.send_to_handler(message).await?;
        if is_legacy_startup {
            let initialized_notification = context.recv_from_handler().await?;
            let initialized_headers = build_request_headers(
                &protocol_headers,
                &initialized_notification.message,
                &tool_header_cache,
                &negotiated_version,
            );
            self.client
                .post_message_with_limits(
                    config.uri.clone(),
                    initialized_notification.message,
                    session_id.clone(),
                    config.auth_header.clone(),
                    initialized_headers,
                    config.client_limits(),
                )
                .await
                .map_err(WorkerQuitReason::fatal_context(
                    "send initialized notification",
                ))?
                .expect_accepted_or_json::<C::Error>()
                .map_err(WorkerQuitReason::fatal_context(
                    "process initialized notification response",
                ))?;
            let _ = initialized_notification.responder.send(Ok(()));
        }
        #[expect(
            clippy::large_enum_variant,
            reason = "the event is short-lived and boxing would add allocation in the event loop"
        )]
        enum Event<C: StreamableHttpClient> {
            ClientMessage(WorkerSendRequest<StreamableHttpClientWorker<C>>),
            ControlMessage(WorkerSendRequest<StreamableHttpClientWorker<C>>),
            StartPost(WorkerSendRequest<StreamableHttpClientWorker<C>>),
            PostResult(PostResult<C>),
            RecoveryTimeout,
            ServerMessage(ServerJsonRpcMessage),
            StreamResult {
                request_id: Option<RequestId>,
                result: Result<(), StreamableHttpError<C::Error>>,
            },
        }
        let mut streams = tokio::task::JoinSet::new();
        let mut pending_stream_response_ids = HashSet::new();
        let mut request_stream_cancellations =
            HashMap::<RequestId, Arc<RequestCancellationRegistration>>::new();
        let mut posts = FuturesUnordered::<BoxFuture<'static, PostResult<C>>>::new();
        let mut control_posts = FuturesUnordered::<BoxFuture<'static, PostResult<C>>>::new();
        let mut session_cancellation = CancellationToken::new();
        let mut pending_message: Option<WorkerSendRequest<Self>> = None;
        let mut recovery_posts = VecDeque::<WorkerSendRequest<Self>>::new();
        let mut recovery_deadline: Option<tokio::time::Instant> = None;
        let mut retrying_recovery = false;
        let mut barrier_in_flight = false;
        let max_concurrent_requests = config.max_concurrent_requests.max(1);
        let mut awaiting_fallback_initialized = false;
        if let Some(session_id) = &session_id {
            Self::spawn_common_stream(
                &mut streams,
                self.client.clone(),
                session_id.clone(),
                &config,
                protocol_headers.clone(),
                sse_worker_tx.clone(),
                transport_task_ct.clone(),
            );
        }
        // Each POST uses the session and headers chosen when it starts.
        // Only this loop updates the current session and protocol version.
        let loop_result: Result<(), WorkerQuitReason<Self::Error>> = 'main_loop: loop {
            // Failed, cancelled and closed requests no longer need a generation.
            // Response admission happens before this next-loop cleanup.
            tool_header_cache
                .discoveries
                .retain(|id, _| pending_stream_response_ids.contains(id));
            if retrying_recovery && recovery_posts.is_empty() && posts.is_empty() {
                retrying_recovery = false;
            }
            if !retrying_recovery
                && !recovery_posts.is_empty()
                && posts.is_empty()
                && control_posts.is_empty()
            {
                // Old POSTs have finished or reached the drain deadline.
                // Retry only ordinary POSTs that returned SessionExpired, at most once each.
                session_cancellation.cancel();
                recovery_deadline = None;
                let recovery = tokio::select! {
                    _ = transport_task_ct.cancelled() => {
                        break 'main_loop Err(WorkerQuitReason::Cancelled);
                    }
                    result = tokio::time::timeout(
                        config.session_recovery_timeout,
                        Self::perform_reinitialization(
                            self.client.clone(),
                            saved_init_request.clone().expect("session recovery requires an initialize request"),
                            config.uri.clone(),
                            config.auth_header.clone(),
                            config.custom_headers.clone(),
                            config.client_limits(),
                        ),
                    ) => result.unwrap_or(Err(StreamableHttpError::SessionRecoveryTimeout)),
                };
                match recovery {
                    Ok((new_session_id, new_version, new_headers)) => {
                        streams.abort_all();
                        while streams.join_next().await.is_some() {}
                        request_stream_cancellations.clear();
                        Self::drain_queued_stream_messages(
                            &mut sse_worker_rx,
                            &context.to_handler_tx,
                            &mut pending_stream_response_ids,
                            &mut request_stream_cancellations,
                            &mut tool_header_cache,
                            &negotiated_version,
                        )
                        .await?;
                        Self::fail_pending_responses_except_retries(
                            &mut context,
                            &mut pending_stream_response_ids,
                            &recovery_posts,
                        )
                        .await?;
                        session_id = new_session_id;
                        negotiated_version = new_version;
                        protocol_headers = new_headers;
                        session_cleanup_info = session_id.as_ref().map(|sid| SessionCleanupInfo {
                            client: self.client.clone(),
                            uri: config.uri.clone(),
                            session_id: sid.clone(),
                            auth_header: config.auth_header.clone(),
                            protocol_headers: protocol_headers.clone(),
                        });
                        // Do not send controls queued during recovery to the new session.
                        context.advance_control_generation();
                        session_cancellation = CancellationToken::new();
                        if let Some(session_id) = &session_id {
                            Self::spawn_common_stream(
                                &mut streams,
                                self.client.clone(),
                                session_id.clone(),
                                &config,
                                protocol_headers.clone(),
                                sse_worker_tx.clone(),
                                transport_task_ct.clone(),
                            );
                        }
                        retrying_recovery = true;
                    }
                    Err(error) => {
                        session_cancellation = CancellationToken::new();
                        Self::fail_recovery_posts(
                            &mut recovery_posts,
                            &mut pending_stream_response_ids,
                            error,
                        );
                    }
                }
                continue;
            }

            let has_post_capacity = posts.len() < max_concurrent_requests;
            let may_start = (retrying_recovery || recovery_posts.is_empty())
                && !barrier_in_flight
                && has_post_capacity;
            let may_receive = may_start && pending_message.is_none() && !retrying_recovery;
            let queued = if retrying_recovery {
                recovery_posts.front_mut()
            } else {
                pending_message.as_mut()
            };
            let has_queued = queued.is_some();
            let can_process_queued = queued.as_ref().is_some_and(|request| {
                let retry_completed = retrying_recovery
                    && Self::client_request_id(&request.message)
                        .is_some_and(|id| !pending_stream_response_ids.contains(&id));
                let ordering_satisfied =
                    !Self::is_ordering_barrier(&request.message, &negotiated_version)
                        || (posts.is_empty() && control_posts.is_empty());
                retry_completed || (may_start && ordering_satisfied)
            });
            let event = tokio::select! {
                _ = async {
                    if can_process_queued {
                        return;
                    }
                    let request = queued.expect("a POST is queued");
                    let cancellation = request.cancellation_token().unwrap_or_default();
                    tokio::select! {
                        _ = request.responder.closed() => {}
                        _ = cancellation.cancelled() => {}
                    }
                }, if has_queued => {
                    let request = if retrying_recovery {
                        recovery_posts.pop_front()
                    } else {
                        pending_message.take()
                    };
                    Event::StartPost(request.expect("a POST is ready to start"))
                }
                _ = transport_task_ct.cancelled() => {
                    tracing::debug!("cancelled");
                    break 'main_loop Err(WorkerQuitReason::Cancelled);
                }
                message = context.from_handler_rx.recv(), if may_receive => {
                    match message {
                        Some(msg) => Event::ClientMessage(msg),
                        None => break 'main_loop Err(WorkerQuitReason::HandlerTerminated),
                    }
                },
                message = context.control_from_handler_rx.recv(),
                    if control_posts.is_empty() && !session_cancellation.is_cancelled() => {
                    match message {
                        Some(msg) => Event::ControlMessage(msg),
                        None => break 'main_loop Err(WorkerQuitReason::HandlerTerminated),
                    }
                },
                Some(result) = posts.next(), if !posts.is_empty() => {
                    Event::PostResult(result)
                },
                Some(result) = control_posts.next(), if !control_posts.is_empty() => {
                    Event::PostResult(result)
                },
                _ = async {
                    if let Some(deadline) = recovery_deadline {
                        tokio::time::sleep_until(deadline).await;
                    }
                }, if recovery_deadline.is_some() => Event::RecoveryTimeout,
                message = sse_worker_rx.recv() => {
                    let Some(message) = message else {
                        tracing::trace!("transport dropped, exiting");
                        break 'main_loop Err(WorkerQuitReason::HandlerTerminated);
                    };
                    Event::ServerMessage(message)
                },
                terminated_stream = streams.join_next(), if !streams.is_empty() => {
                    match terminated_stream {
                        Some(Ok((request_id, result))) => {
                            Event::StreamResult { request_id, result }
                        }
                        Some(Err(error)) => Event::StreamResult {
                            request_id: None,
                            result: Err(StreamableHttpError::TokioJoinError(error)),
                        },
                        None => continue,
                    }
                }
            };
            match event {
                Event::ClientMessage(send_request) => {
                    pending_message = Some(send_request);
                }
                Event::ControlMessage(send_request) => {
                    if send_request.responder.is_closed() {
                        continue;
                    }
                    let cancellation_request_id =
                        Self::cancellation_request_id(&send_request.message);
                    let stale = send_request.control_generation() != context.control_generation();
                    if stale {
                        // Do not send old controls to a replacement session.
                        let result = match cancellation_request_id {
                            Some(_) => Ok(()),
                            None => Err(StreamableHttpError::SessionExpired),
                        };
                        let _ = send_request.responder.send(result);
                        continue;
                    }
                    if let Some(request_id) = cancellation_request_id {
                        drop(request_stream_cancellations.remove(request_id));
                        pending_stream_response_ids.remove(request_id);
                        if uses_modern_http {
                            let _ = send_request.responder.send(Ok(()));
                            continue;
                        }
                    }
                    let (version, headers) = request_version_headers(
                        &protocol_headers,
                        &send_request.message,
                        &negotiated_version,
                        &tool_header_cache,
                    );
                    control_posts.push(Self::post_request(
                        self.client.clone(),
                        &config,
                        send_request,
                        PostSession {
                            id: session_id.clone(),
                            headers,
                            version,
                            cancellation: session_cancellation.clone(),
                        },
                        transport_task_ct.clone(),
                    ));
                }
                Event::RecoveryTimeout => {
                    recovery_deadline = None;
                    session_cancellation.cancel();
                    tracing::warn!("old-session POSTs did not finish before the recovery deadline");
                }
                Event::StartPost(send_request) => {
                    let request_id = Self::client_request_id(&send_request.message);
                    let send_cancelled = send_request.responder.is_closed()
                        || send_request
                            .cancellation_token()
                            .is_some_and(|token| token.is_cancelled());
                    let retry_completed = retrying_recovery
                        && request_id
                            .as_ref()
                            .is_some_and(|id| !pending_stream_response_ids.contains(id));
                    if send_cancelled || retry_completed {
                        if retrying_recovery && let Some(id) = &request_id {
                            pending_stream_response_ids.remove(id);
                        }
                        let _ = send_request.responder.send(Ok(()));
                        continue;
                    }
                    let message = &send_request.message;
                    let is_fallback_initialize = saved_init_request.is_none()
                        && matches!(
                            message,
                            ClientJsonRpcMessage::Request(request)
                                if matches!(
                                    &request.request,
                                    ClientRequest::InitializeRequest(_)
                                )
                        );
                    if is_fallback_initialize {
                        saved_init_request = Some(message.clone());
                        let WorkerSendRequest {
                            message, responder, ..
                        } = send_request;
                        // Servers do not assign sessions to `server/discover`, so a
                        // fallback initialize starts from a clean slate: no session
                        // ID, no cleanup state, and no streams to tear down.
                        debug_assert!(
                            session_id.is_none()
                                && session_cleanup_info.is_none()
                                && streams.is_empty(),
                            "discover bootstrap must not create session state"
                        );
                        uses_modern_http = false;

                        let response = self
                            .client
                            .post_message_with_limits(
                                config.uri.clone(),
                                message,
                                None,
                                config.auth_header.clone(),
                                config.custom_headers.clone(),
                                config.client_limits(),
                            )
                            .await;
                        let response = match response {
                            Ok(response) => {
                                let _ = responder.send(Ok(()));
                                response
                            }
                            Err(error) => {
                                let _ = responder.send(Err(error));
                                continue;
                            }
                        };
                        let (initialize_response, new_session_id) = response
                            .expect_initialized::<C::Error>()
                            .await
                            .map_err(WorkerQuitReason::fatal_context(
                                "process fallback initialize response",
                            ))?;
                        session_id = new_session_id.map(Arc::from);
                        if session_id.is_none() && !config.allow_stateless {
                            return Err(WorkerQuitReason::fatal(
                                StreamableHttpError::<C::Error>::MissingSessionIdInResponse,
                                "process fallback initialize response",
                            ));
                        }
                        (negotiated_version, protocol_headers) = negotiate_version_headers(
                            &initialize_response,
                            config.custom_headers.clone(),
                        );
                        session_cleanup_info =
                            session_id.as_ref().map(|session_id| SessionCleanupInfo {
                                client: self.client.clone(),
                                uri: config.uri.clone(),
                                session_id: session_id.clone(),
                                auth_header: config.auth_header.clone(),
                                protocol_headers: protocol_headers.clone(),
                            });
                        context.send_to_handler(initialize_response).await?;
                        awaiting_fallback_initialized = true;
                        continue;
                    }

                    let barrier = Self::is_ordering_barrier(message, &negotiated_version);
                    debug_assert!(!barrier || (posts.is_empty() && control_posts.is_empty()));
                    let inline_version = match message {
                        ClientJsonRpcMessage::Request(request) => {
                            request.request.get_meta().protocol_version()
                        }
                        _ => None,
                    };
                    let (request_version, request_headers) = request_version_headers(
                        &protocol_headers,
                        message,
                        &negotiated_version,
                        &tool_header_cache,
                    );
                    if inline_version.is_some() {
                        negotiated_version = request_version.clone();
                        if let Ok(value) = HeaderValue::from_str(request_version.as_str()) {
                            protocol_headers
                                .insert(HeaderName::from_static("mcp-protocol-version"), value);
                        }
                        if let Some(cleanup) = &mut session_cleanup_info {
                            cleanup.protocol_headers = protocol_headers.clone();
                        }
                    }
                    barrier_in_flight = barrier;
                    // The common stream can return a response before this POST finishes.
                    if let Some(request_id) = request_id {
                        if matches!(message, ClientJsonRpcMessage::Request(request)
                            if matches!(request.request, ClientRequest::ListToolsRequest(_)))
                        {
                            tool_header_cache.track_discovery(request_id.clone());
                        }
                        pending_stream_response_ids.insert(request_id);
                    }
                    posts.push(Self::post_request(
                        self.client.clone(),
                        &config,
                        send_request,
                        PostSession {
                            id: session_id.clone(),
                            headers: request_headers,
                            version: request_version,
                            cancellation: session_cancellation.clone(),
                        },
                        transport_task_ct.clone(),
                    ));
                }
                Event::PostResult(PostResult {
                    send_request,
                    response,
                    version,
                }) => {
                    let is_control = Self::is_control_message(&send_request.message);
                    if !is_control {
                        // An ordering barrier runs without other ordinary POSTs.
                        barrier_in_flight = false;
                    }
                    let request_id = Self::client_request_id(&send_request.message);
                    if request_id
                        .as_ref()
                        .is_some_and(|id| !pending_stream_response_ids.contains(id))
                    {
                        // A stream response or cancellation already completed this request.
                        let _ = send_request.responder.send(Ok(()));
                        continue;
                    }
                    let will_retry =
                        matches!(&response, Some(Err(StreamableHttpError::SessionExpired)))
                            && !is_control
                            && !retrying_recovery
                            && config.reinit_on_expired_session
                            && saved_init_request.is_some();
                    let awaits_stream_response = matches!(
                        &response,
                        Some(Ok(StreamableHttpPostResponse::Accepted
                            | StreamableHttpPostResponse::Sse(..)))
                    );
                    if !will_retry
                        && !awaits_stream_response
                        && let Some(id) = &request_id
                    {
                        pending_stream_response_ids.remove(id);
                    }
                    let Some(response) = response else {
                        let _ = send_request.responder.send(Ok(()));
                        continue;
                    };
                    if will_retry {
                        if recovery_posts.is_empty() {
                            recovery_deadline =
                                Some(tokio::time::Instant::now() + config.session_recovery_timeout);
                        }
                        recovery_posts.push_back(send_request);
                        continue;
                    }
                    let request_cancellation = send_request.cancellation_registration();
                    let WorkerSendRequest {
                        message, responder, ..
                    } = send_request;
                    let is_initialized_notification = matches!(
                        &message,
                        ClientJsonRpcMessage::Notification(notification)
                            if matches!(
                                &notification.notification,
                                ClientNotification::InitializedNotification(_)
                            )
                    );
                    let send_result = match response {
                        Err(e) => Err(e),
                        Ok(StreamableHttpPostResponse::Accepted) => {
                            tracing::trace!("client message accepted");
                            Ok(())
                        }
                        Ok(StreamableHttpPostResponse::Json(mut message, ..)) => {
                            cache_tools_from_response(
                                &mut tool_header_cache,
                                &mut message,
                                &version,
                            );
                            context.send_to_handler(message).await?;
                            Ok(())
                        }
                        Ok(StreamableHttpPostResponse::Sse(stream, ..)) => {
                            let stream_request_id = request_id;
                            let sse_stream = Self::response_sse_to_jsonrpc(
                                stream,
                                StreamableHttpClientReconnect {
                                    session_id: session_id.clone(),
                                    client: self.client.clone(),
                                    uri: config.uri.clone(),
                                    auth_header: config.auth_header.clone(),
                                    custom_headers: protocol_headers.clone(),
                                    max_sse_event_size: config.max_sse_event_size,
                                },
                                self.config.retry_config.clone(),
                            );
                            let request_ct = request_cancellation
                                .as_ref()
                                .map(|registration| registration.token())
                                .unwrap_or_else(|| transport_task_ct.child_token());
                            // A legacy adapter may need the open stream to handle cancellation.
                            let stream_ct = if uses_modern_http {
                                request_ct.clone()
                            } else {
                                request_cancellation
                                    .as_ref()
                                    .map(|registration| registration.lifetime_token())
                                    .unwrap_or_else(|| request_ct.clone())
                            };
                            if let (Some(request_id), Some(registration)) =
                                (stream_request_id.as_ref(), request_cancellation)
                            {
                                request_stream_cancellations
                                    .insert(request_id.clone(), registration);
                            }
                            let caller_lifetime = match &message {
                                ClientJsonRpcMessage::Request(request) => request
                                    .request
                                    .extensions()
                                    .get::<OutboundRequestLifetime>()
                                    .map(|lifetime| lifetime.0.clone())
                                    .unwrap_or_default(),
                                _ => CancellationToken::new(),
                            };
                            let stream_tx = sse_worker_tx.clone();
                            let origin = match &stream_request_id {
                                Some(id) => InboundStreamOrigin::OutboundRequest(id.clone()),
                                None => InboundStreamOrigin::Unassociated,
                            };
                            streams.spawn(async move {
                                let result = Self::run_response_stream(
                                    sse_stream,
                                    stream_tx,
                                    origin,
                                    request_ct,
                                    stream_ct,
                                    uses_modern_http,
                                    caller_lifetime,
                                )
                                .await;
                                (stream_request_id, result)
                            });
                            tracing::trace!("got new sse stream");
                            Ok(())
                        }
                    };
                    if send_result.is_ok()
                        && awaiting_fallback_initialized
                        && is_initialized_notification
                    {
                        if let Some(session_id) = &session_id {
                            Self::spawn_common_stream(
                                &mut streams,
                                self.client.clone(),
                                session_id.clone(),
                                &config,
                                protocol_headers.clone(),
                                sse_worker_tx.clone(),
                                transport_task_ct.clone(),
                            );
                        }
                        awaiting_fallback_initialized = false;
                    }
                    let _ = responder.send(send_result);
                }
                Event::ServerMessage(mut json_rpc_message) => {
                    // Share admission and cleanup with stream-completion drains.
                    Self::prepare_stream_message(
                        &mut json_rpc_message,
                        &mut pending_stream_response_ids,
                        &mut request_stream_cancellations,
                        &mut tool_header_cache,
                        &negotiated_version,
                    );
                    // send the message to the handler
                    if let Err(e) = context.send_to_handler(json_rpc_message).await {
                        break 'main_loop Err(e);
                    }
                }
                Event::StreamResult { request_id, result } => {
                    if let Some(request_id) = request_id {
                        Self::drain_queued_stream_messages(
                            &mut sse_worker_rx,
                            &context.to_handler_tx,
                            &mut pending_stream_response_ids,
                            &mut request_stream_cancellations,
                            &mut tool_header_cache,
                            &negotiated_version,
                        )
                        .await?;
                        let cancelled = request_stream_cancellations
                            .remove(&request_id)
                            .is_some_and(|registration| registration.token().is_cancelled());
                        if pending_stream_response_ids.remove(&request_id) && !cancelled {
                            context
                                .send_to_handler(ServerJsonRpcMessage::error(
                                    ErrorData::transport_closed(
                                        "streamable HTTP response stream closed before its final response",
                                    ),
                                    Some(request_id),
                                ))
                                .await?;
                        }
                    }
                    if result.is_err() {
                        tracing::warn!(
                            "sse client event stream terminated with error: {:?}",
                            result
                        );
                    }
                }
            }
        };

        // Stop outstanding http requests before deleting their session.
        transport_task_ct.cancel();
        drop(posts);
        drop(control_posts);
        drop(pending_message);
        drop(recovery_posts);
        streams.abort_all();

        // Cleanup session before returning (ensures close() waits for session deletion)
        // Use a timeout to prevent indefinite hangs if the server is unresponsive
        if let Some(cleanup) = session_cleanup_info {
            let cleanup_session_id = cleanup.session_id.clone();
            match tokio::time::timeout(
                SESSION_CLEANUP_TIMEOUT,
                cleanup.client.delete_session(
                    cleanup.uri,
                    cleanup.session_id,
                    cleanup.auth_header,
                    cleanup.protocol_headers,
                ),
            )
            .await
            {
                Ok(Ok(_)) => {
                    tracing::info!(
                        session_id = cleanup_session_id.as_ref(),
                        "delete session success"
                    )
                }
                Ok(Err(StreamableHttpError::ServerDoesNotSupportDeleteSession)) => {
                    tracing::info!(
                        session_id = cleanup_session_id.as_ref(),
                        "server doesn't support delete session"
                    )
                }
                Ok(Err(e)) => {
                    tracing::error!(
                        session_id = cleanup_session_id.as_ref(),
                        "fail to delete session: {e}"
                    );
                }
                Err(_elapsed) => {
                    tracing::warn!(
                        session_id = cleanup_session_id.as_ref(),
                        "session cleanup timed out after {:?}",
                        SESSION_CLEANUP_TIMEOUT
                    );
                }
            }
        }

        loop_result
    }
}

/// A client-agnostic HTTP transport for RMCP that supports streaming responses.
///
/// This transport allows you to choose your preferred HTTP client implementation
/// by implementing the [`StreamableHttpClient`] trait. The transport handles
/// session management, SSE streaming, and automatic reconnection.
///
/// # Usage
///
/// ## Using reqwest
///
/// ```rust,no_run
/// use mcp_protocol::transport::StreamableHttpClientTransport;
///
/// // Enable the reqwest feature in Cargo.toml:
/// // rmcp = { version = "0.5", features = ["transport-streamable-http-client-reqwest"] }
///
/// let transport = StreamableHttpClientTransport::from_uri("http://localhost:8000/mcp");
/// ```
///
/// ## Using a custom HTTP client
///
/// ```rust,no_run
/// use mcp_protocol::transport::streamable_http_client::{
///     StreamableHttpClient,
///     StreamableHttpClientTransport,
///     StreamableHttpClientTransportConfig
/// };
/// use std::sync::Arc;
/// use std::collections::HashMap;
/// use futures::stream::BoxStream;
/// use mcp_protocol::model::ClientJsonRpcMessage;
/// use http::{HeaderName, HeaderValue};
/// use sse_stream::{Sse, Error as SseError};
///
/// #[derive(Clone)]
/// struct MyHttpClient;
///
/// #[derive(Debug, thiserror::Error)]
/// struct MyError;
///
/// impl std::fmt::Display for MyError {
///     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
///         write!(f, "MyError")
///     }
/// }
///
/// impl StreamableHttpClient for MyHttpClient {
///     type Error = MyError;
///
///     async fn post_message(
///         &self,
///         _uri: Arc<str>,
///         _message: ClientJsonRpcMessage,
///         _session_id: Option<Arc<str>>,
///         _auth_header: Option<String>,
///         _custom_headers: HashMap<HeaderName, HeaderValue>,
///     ) -> Result<mcp_protocol::transport::streamable_http_client::StreamableHttpPostResponse, mcp_protocol::transport::streamable_http_client::StreamableHttpError<Self::Error>> {
///         todo!()
///     }
///
///     async fn delete_session(
///         &self,
///         _uri: Arc<str>,
///         _session_id: Arc<str>,
///         _auth_header: Option<String>,
///         _custom_headers: HashMap<HeaderName, HeaderValue>,
///     ) -> Result<(), mcp_protocol::transport::streamable_http_client::StreamableHttpError<Self::Error>> {
///         todo!()
///     }
///
///     async fn get_stream(
///         &self,
///         _uri: Arc<str>,
///         _session_id: Option<Arc<str>>,
///         _last_event_id: Option<String>,
///         _auth_header: Option<String>,
///         _custom_headers: HashMap<HeaderName, HeaderValue>,
///     ) -> Result<BoxStream<'static, Result<Sse, SseError>>, mcp_protocol::transport::streamable_http_client::StreamableHttpError<Self::Error>> {
///         todo!()
///     }
/// }
///
/// let transport = StreamableHttpClientTransport::with_client(
///     MyHttpClient,
///     StreamableHttpClientTransportConfig::with_uri("http://localhost:8000/mcp")
/// );
/// ```
///
/// # Feature Flags
///
/// - `transport-streamable-http-client`: Base feature providing the generic transport infrastructure
/// - `transport-streamable-http-client-reqwest`: Includes reqwest HTTP client support with convenience methods
pub type StreamableHttpClientTransport<C> = WorkerTransport<StreamableHttpClientWorker<C>>;

impl<C: StreamableHttpClient> StreamableHttpClientTransport<C> {
    /// Creates a new transport with a custom HTTP client implementation.
    ///
    /// This method allows you to use any HTTP client that implements the [`StreamableHttpClient`] trait.
    /// Use this when you want to use a custom HTTP client or when the reqwest feature is not enabled.
    ///
    /// # Arguments
    ///
    /// * `client` - Your HTTP client implementation
    /// * `config` - Transport configuration including the server URI
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use mcp_protocol::transport::streamable_http_client::{
    ///     StreamableHttpClient,
    ///     StreamableHttpClientTransport,
    ///     StreamableHttpClientTransportConfig
    /// };
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    /// use futures::stream::BoxStream;
    /// use mcp_protocol::model::ClientJsonRpcMessage;
    /// use http::{HeaderName, HeaderValue};
    /// use sse_stream::{Sse, Error as SseError};
    ///
    /// // Define your custom client
    /// #[derive(Clone)]
    /// struct MyHttpClient;
    ///
    /// #[derive(Debug, thiserror::Error)]
    /// struct MyError;
    ///
    /// impl std::fmt::Display for MyError {
    ///     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    ///         write!(f, "MyError")
    ///     }
    /// }
    ///
    /// impl StreamableHttpClient for MyHttpClient {
    ///     type Error = MyError;
    ///
    ///     async fn post_message(
    ///         &self,
    ///         _uri: Arc<str>,
    ///         _message: ClientJsonRpcMessage,
    ///         _session_id: Option<Arc<str>>,
    ///         _auth_header: Option<String>,
    ///         _custom_headers: HashMap<HeaderName, HeaderValue>,
    ///     ) -> Result<mcp_protocol::transport::streamable_http_client::StreamableHttpPostResponse, mcp_protocol::transport::streamable_http_client::StreamableHttpError<Self::Error>> {
    ///         todo!()
    ///     }
    ///
    ///     async fn delete_session(
    ///         &self,
    ///         _uri: Arc<str>,
    ///         _session_id: Arc<str>,
    ///         _auth_header: Option<String>,
    ///         _custom_headers: HashMap<HeaderName, HeaderValue>,
    ///     ) -> Result<(), mcp_protocol::transport::streamable_http_client::StreamableHttpError<Self::Error>> {
    ///         todo!()
    ///     }
    ///
    ///     async fn get_stream(
    ///         &self,
    ///         _uri: Arc<str>,
    ///         _session_id: Option<Arc<str>>,
    ///         _last_event_id: Option<String>,
    ///         _auth_header: Option<String>,
    ///         _custom_headers: HashMap<HeaderName, HeaderValue>,
    ///     ) -> Result<BoxStream<'static, Result<Sse, SseError>>, mcp_protocol::transport::streamable_http_client::StreamableHttpError<Self::Error>> {
    ///         todo!()
    ///     }
    /// }
    ///
    /// let transport = StreamableHttpClientTransport::with_client(
    ///     MyHttpClient,
    ///     StreamableHttpClientTransportConfig::with_uri("http://localhost:8000/mcp")
    /// );
    /// ```
    pub fn with_client(client: C, config: StreamableHttpClientTransportConfig) -> Self {
        let worker = StreamableHttpClientWorker::new(client, config);
        WorkerTransport::spawn(worker)
    }
}
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct StreamableHttpClientTransportConfig {
    pub uri: Arc<str>,
    pub retry_config: Arc<dyn SseRetryPolicy>,
    pub channel_buffer_capacity: usize,
    /// Maximum number of ordinary http POSTs in progress (default: 16).
    /// A POST stops counting when it completes or opens an sse response stream.
    /// Zero is treated as one. Cancellation and replies use a separate queue
    /// with one extra POST slot and use [`Self::control_request_timeout`].
    pub max_concurrent_requests: usize,
    /// Maximum time a cancellation or reply POST can run after it starts (default: five seconds).
    /// Time spent waiting in the control queue does not count toward this timeout.
    pub control_request_timeout: Duration,
    /// Maximum wait for old POSTs to finish before session recovery (default: five seconds).
    /// The new initialization handshake has a separate timeout of the same length.
    /// An unfinished old POST returns [`StreamableHttpError::SessionRecoveryTimeout`]
    /// and is not retried because the server may have processed it.
    pub session_recovery_timeout: Duration,
    /// if true, the transport will not require a session to be established
    pub allow_stateless: bool,
    /// The value to send in the authorization header
    pub auth_header: Option<String>,
    /// Custom HTTP headers to include with every request
    pub custom_headers: HashMap<HeaderName, HeaderValue>,
    /// Maximum number of tool routing schemas retained (default: 1024).
    /// Oldest full-discovery entries are evicted first; a full refresh moves an
    /// entry to the back. Calls and lean discovery do not change this order.
    /// Zero disables retention. Evicted schemas must be rediscovered with
    /// `detail: full` before calling tools with required promoted headers.
    /// Clear the peer response cache first to force that discovery across HTTP.
    /// Only annotated parameter names, primitive types and header names are kept.
    pub max_routing_schemas: usize,
    /// Aggregate retained routing payload budget (default: 4 MiB).
    /// Each entry counts its UTF-8 tool name plus compact JSON routing projection.
    /// Allocator/container overhead, in-flight discovery and the peer response
    /// cache are separate. The count limit in [`Self::max_routing_schemas`] also
    /// applies. Zero disables retention. Oldest full-discovery entries are evicted
    /// until both limits fit; a single oversized entry is delivered but not
    /// retained and does not evict unrelated entries. An oversized refresh retires
    /// that tool's previous entry. Full rediscovery across HTTP is required to
    /// restore evicted entries that fit the configured limits.
    pub max_routing_schema_bytes: usize,
    /// Maximum raw size of one SSE event accepted from the server.
    ///
    /// The built-in reqwest client enforces this value. Custom
    /// [`StreamableHttpClient`] implementations must override
    /// [`StreamableHttpClient::post_message_with_limits`] and
    /// [`StreamableHttpClient::get_stream_with_max_sse_event_size`] to enforce it.
    pub max_sse_event_size: usize,
    /// Maximum decoded bytes collected from one non-SSE HTTP response (default: 16 MiB).
    /// Applies to JSON replies and HTTP error bodies before parsing. Zero rejects
    /// nonempty bodies. Successful SSE responses use [`Self::max_sse_event_size`]
    /// instead; this is not a bound on an entire stream or process memory.
    /// Custom HTTP backends must implement
    /// [`StreamableHttpClient::post_message_with_limits`] to enforce this limit.
    pub max_response_body_size: usize,
    /// Automatically creates a new session when the server reports an expired
    /// session (`http 404`).
    ///
    /// Ordinary POSTs that fail with `SessionExpired` in the same session share one
    /// recovery attempt:
    /// 1. Wait for old POSTs, up to [`Self::session_recovery_timeout`].
    /// 2. Repeat the original `initialize` handshake and open new streams.
    /// 3. Retry each ordinary POST that failed with `SessionExpired` once.
    ///
    /// Control POSTs and other POST failures are not retried. If recovery or a retry
    /// fails, the transport returns an error to the caller.
    pub reinit_on_expired_session: bool,
}

impl StreamableHttpClientTransportConfig {
    pub fn with_uri(uri: impl Into<Arc<str>>) -> Self {
        Self {
            uri: uri.into(),
            ..Default::default()
        }
    }

    /// Set how many ordinary POSTs can run at once. One keeps them serial; zero also means one.
    pub fn max_concurrent_requests(mut self, limit: usize) -> Self {
        self.max_concurrent_requests = limit.max(1);
        self
    }

    /// Set the maximum retained routing schemas. Zero disables retention.
    /// See [`Self::max_routing_schemas`] for eviction and rediscovery behavior.
    pub fn max_routing_schemas(mut self, limit: usize) -> Self {
        self.max_routing_schemas = limit;
        self
    }

    /// Set the aggregate routing payload byte budget. Zero disables retention.
    /// See [`Self::max_routing_schema_bytes`] for accounting and eviction behavior.
    pub fn max_routing_schema_bytes(mut self, bytes: usize) -> Self {
        self.max_routing_schema_bytes = bytes;
        self
    }

    /// Set the timeout for cancellation and reply POSTs, starting when each POST starts.
    pub fn control_request_timeout(mut self, timeout: Duration) -> Self {
        self.control_request_timeout = timeout;
        self
    }

    /// Set the separate timeouts for waiting for old POSTs and creating a replacement session.
    pub fn session_recovery_timeout(mut self, timeout: Duration) -> Self {
        self.session_recovery_timeout = timeout;
        self
    }

    /// Set the authorization header to send with requests
    ///
    /// # Arguments
    ///
    /// * `value` - A bearer token without the `Bearer ` prefix
    pub fn auth_header<T: Into<String>>(mut self, value: T) -> Self {
        // set our authorization header
        self.auth_header = Some(value.into());
        self
    }

    /// Set custom HTTP headers to include with every request.
    ///
    /// For standardized HTTP requests, `Mcp-Method`, `Mcp-Name`, and
    /// `Mcp-Param-*` are reserved: the transport rebuilds them from each message
    /// and its discovered tool schema. Custom values cannot override them or
    /// supply headers for omitted arguments.
    ///
    /// # Arguments
    ///
    /// * `custom_headers` - A HashMap of header names to header values
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::collections::HashMap;
    /// use http::{HeaderName, HeaderValue};
    /// use mcp_protocol::transport::streamable_http_client::StreamableHttpClientTransportConfig;
    ///
    /// let mut headers = HashMap::new();
    /// headers.insert(
    ///     HeaderName::from_static("x-custom-header"),
    ///     HeaderValue::from_static("custom-value")
    /// );
    ///
    /// let config = StreamableHttpClientTransportConfig::with_uri("http://localhost:8000")
    ///     .custom_headers(headers);
    /// ```
    pub fn custom_headers(mut self, custom_headers: HashMap<HeaderName, HeaderValue>) -> Self {
        self.custom_headers = custom_headers;
        self
    }

    /// Set the maximum raw size of one SSE event accepted from the server.
    pub fn max_sse_event_size(mut self, bytes: usize) -> Self {
        self.max_sse_event_size = bytes;
        self
    }

    /// Set the maximum decoded bytes buffered for a non-SSE response body.
    /// See [`Self::max_response_body_size`] for coverage and custom-backend behavior.
    pub fn max_response_body_size(mut self, bytes: usize) -> Self {
        self.max_response_body_size = bytes;
        self
    }

    fn client_limits(&self) -> StreamableHttpClientLimits {
        StreamableHttpClientLimits {
            max_sse_event_size: self.max_sse_event_size,
            max_response_body_size: self.max_response_body_size,
        }
    }

    /// Set whether the transport should attempt transparent re-initialization on session expiration
    /// See [`Self::reinit_on_expired_session`] for details.
    /// # Example
    /// ```rust,no_run
    /// use mcp_protocol::transport::streamable_http_client::StreamableHttpClientTransportConfig;
    /// let config = StreamableHttpClientTransportConfig::with_uri("http://localhost:8000")
    ///     .reinit_on_expired_session(true);
    /// ```
    pub fn reinit_on_expired_session(mut self, enable: bool) -> Self {
        self.reinit_on_expired_session = enable;
        self
    }
}

impl Default for StreamableHttpClientTransportConfig {
    fn default() -> Self {
        Self {
            uri: "localhost".into(),
            retry_config: Arc::new(ExponentialBackoff::default()),
            channel_buffer_capacity: 16,
            max_concurrent_requests: 16,
            max_routing_schemas: DEFAULT_MAX_ROUTING_SCHEMAS,
            max_routing_schema_bytes: DEFAULT_MAX_ROUTING_SCHEMA_BYTES,
            control_request_timeout: Duration::from_secs(5),
            session_recovery_timeout: Duration::from_secs(5),
            allow_stateless: true,
            auth_header: None,
            custom_headers: HashMap::new(),
            max_sse_event_size: DEFAULT_MAX_SSE_EVENT_SIZE,
            max_response_body_size: StreamableHttpClientLimits::default().max_response_body_size,
            reinit_on_expired_session: true,
        }
    }
}

#[cfg(test)]
mod routing_schema_tests {
    use super::*;
    use serde_json::json;

    fn discover(cache: &mut ToolHeaderCache, tools: serde_json::Value) -> serde_json::Value {
        let mut response = serde_json::from_value(json!({
            "jsonrpc":"2.0", "id":1, "result":{"tools":tools}
        }))
        .unwrap();
        cache.track_discovery(RequestId::Number(1));
        cache_tools_from_response(cache, &mut response, &ProtocolVersion::V_2026_07_28);
        serde_json::to_value(response).unwrap()["result"]["tools"].clone()
    }

    fn annotated_tool(name: &str) -> serde_json::Value {
        json!({"name":name, "inputSchema":{"type":"object", "properties":{
            "region":{"type":"string", "x-mcp-header":"Region"}
        }}})
    }

    fn admit(cache: &mut ToolHeaderCache, value: serde_json::Value) -> serde_json::Value {
        let mut message = serde_json::from_value(value).unwrap();
        cache_tools_from_response(cache, &mut message, &ProtocolVersion::V_2026_07_28);
        serde_json::to_value(message).unwrap()
    }

    fn invalidate(cache: &mut ToolHeaderCache) {
        admit(
            cache,
            json!({"jsonrpc":"2.0", "method":"notifications/tools/list_changed"}),
        );
    }

    #[test]
    fn tool_list_change_clears_routing_and_rejects_older_discovery_writes() {
        let mut cache = ToolHeaderCache::new(2, DEFAULT_MAX_ROUTING_SCHEMA_BYTES);
        discover(&mut cache, json!([annotated_tool("deploy")]));
        cache.track_discovery(RequestId::Number(2));
        invalidate(&mut cache);
        assert!(cache.schemas.is_empty());
        assert!(cache.discovery_order.is_empty());
        let response = admit(
            &mut cache,
            json!({
                "jsonrpc":"2.0", "id":"2", "result":{"tools":[annotated_tool("deploy")]}
            }),
        );
        assert_eq!(response["result"]["tools"].as_array().unwrap().len(), 1);
        assert!(cache.schemas.is_empty());
        assert!(cache.discoveries.is_empty());
        discover(&mut cache, json!([annotated_tool("deploy")]));
        assert!(cache.get("deploy").is_some());
    }

    #[test]
    fn stale_invalid_and_unsolicited_responses_cannot_retire_current_routing() {
        let mut cache = ToolHeaderCache::new(2, DEFAULT_MAX_ROUTING_SCHEMA_BYTES);
        cache.track_discovery(RequestId::Number(2));
        cache.track_discovery(RequestId::Number(3));
        invalidate(&mut cache);
        let mut current = annotated_tool("deploy");
        current["inputSchema"]["properties"]["region"]["x-mcp-header"] = json!("Zone");
        discover(&mut cache, json!([current]));
        let expected = cache.get("deploy").unwrap().clone();
        let invalid = json!({"name":"deploy", "inputSchema":{"type":"object", "properties":{
            "region":{"type":"number", "x-mcp-header":"Region"}
        }}});
        let response = admit(
            &mut cache,
            json!({
                "jsonrpc":"2.0", "id":2, "result":{"tools":[invalid]}
            }),
        );
        assert_eq!(response["result"]["tools"], json!([]));
        assert_eq!(cache.get("deploy"), Some(&expected));
        for id in [3, 77, 3] {
            admit(
                &mut cache,
                json!({"jsonrpc":"2.0", "id":id,
                "result":{"tools":[annotated_tool("deploy")]}}),
            );
            assert_eq!(cache.get("deploy"), Some(&expected));
        }
        assert!(cache.discoveries.is_empty());
    }

    #[test]
    fn unrelated_notifications_preserve_routing_and_error_completes_discovery() {
        let mut cache = ToolHeaderCache::new(2, DEFAULT_MAX_ROUTING_SCHEMA_BYTES);
        discover(&mut cache, json!([annotated_tool("deploy")]));
        cache.track_discovery(RequestId::Number(2));
        admit(
            &mut cache,
            json!({"jsonrpc":"2.0", "method":"notifications/prompts/list_changed"}),
        );
        assert!(cache.get("deploy").is_some());
        admit(
            &mut cache,
            json!({"jsonrpc":"2.0", "id":"2", "error":{"code":-32603,"message":"failed"}}),
        );
        assert!(cache.discoveries.is_empty());
        assert!(cache.get("deploy").is_some());
        cache.track_discovery(RequestId::Number(3));
        invalidate(&mut cache);
        invalidate(&mut cache);
        admit(
            &mut cache,
            json!({"jsonrpc":"2.0", "id":3,
            "result":{"tools":[annotated_tool("deploy")]}}),
        );
        assert!(cache.schemas.is_empty());
    }

    #[cfg(feature = "__reqwest")]
    #[tokio::test]
    async fn completion_drain_invalidates_before_admitting_late_discovery() {
        let mut cache = ToolHeaderCache::new(2, DEFAULT_MAX_ROUTING_SCHEMA_BYTES);
        discover(&mut cache, json!([annotated_tool("deploy")]));
        cache.track_discovery(RequestId::Number(2));
        let (stream_tx, mut stream_rx) = tokio::sync::mpsc::channel(4);
        for value in [
            json!({"jsonrpc":"2.0", "method":"notifications/tools/list_changed"}),
            json!({"jsonrpc":"2.0", "id":"2", "result":{"tools":[annotated_tool("deploy")]}}),
        ] {
            stream_tx
                .send(serde_json::from_value(value).unwrap())
                .await
                .unwrap();
        }
        drop(stream_tx);
        let (handler_tx, mut handler_rx) = tokio::sync::mpsc::channel(4);
        let mut pending = HashSet::from([RequestId::Number(2)]);
        StreamableHttpClientWorker::<reqwest::Client>::drain_queued_stream_messages(
            &mut stream_rx,
            &handler_tx,
            &mut pending,
            &mut HashMap::new(),
            &mut cache,
            &ProtocolVersion::V_2026_07_28,
        )
        .await
        .unwrap();
        assert!(pending.is_empty());
        assert!(cache.schemas.is_empty());
        assert!(cache.discovery_order.is_empty());
        assert!(cache.discoveries.is_empty());
        assert!(matches!(
            handler_rx.try_recv().unwrap(),
            ServerJsonRpcMessage::Notification(_)
        ));
        assert!(matches!(
            handler_rx.try_recv().unwrap(),
            ServerJsonRpcMessage::Response(_)
        ));
        assert!(handler_rx.try_recv().is_err());
    }

    fn tool_bytes(tool: &serde_json::Value) -> usize {
        tool["name"].as_str().unwrap().len()
            + serde_json::to_vec(&tool["inputSchema"]).unwrap().len()
    }

    #[test]
    fn routing_byte_budget_counts_utf8_and_json_escaping_at_exact_boundary() {
        let tool = json!({"name":"déploy", "inputSchema":{"type":"object", "properties":{
            "地域\"\n": {"type":"string", "x-mcp-header":"Region"}
        }}});
        let bytes = tool_bytes(&tool);
        let mut cache = ToolHeaderCache::new(8, bytes);
        let mut documented = tool.clone();
        documented["inputSchema"]["description"] = json!("x".repeat(10_000));
        assert_eq!(
            discover(&mut cache, json!([documented])),
            json!([documented])
        );
        assert_eq!(cache.retained_bytes, bytes);
        assert!(cache.get("déploy").is_some());
        let mut too_small = ToolHeaderCache::new(8, bytes - 1);
        assert_eq!(discover(&mut too_small, json!([tool])), json!([tool]));
        assert_eq!(too_small.retained_bytes, 0);
        assert!(too_small.schemas.is_empty());
    }

    #[test]
    fn routing_byte_budget_evicts_multiple_entries_and_releases_every_retired_byte() {
        let a = annotated_tool("a");
        let mut large = annotated_tool("d");
        large["inputSchema"]["properties"]["extra"] =
            json!({"type":"boolean", "x-mcp-header":"Extra"});
        let small_bytes = tool_bytes(&a);
        let large_bytes = tool_bytes(&large);
        assert!(large_bytes > small_bytes && large_bytes < 2 * small_bytes);
        let mut cache = ToolHeaderCache::new(8, 3 * small_bytes);
        discover(
            &mut cache,
            json!([a, annotated_tool("b"), annotated_tool("c")]),
        );
        assert_eq!(cache.retained_bytes, 3 * small_bytes);
        discover(&mut cache, json!([large]));
        assert_eq!(cache.discovery_order, ["c", "d"]);
        assert_eq!(cache.retained_bytes, small_bytes + large_bytes);
        discover(&mut cache, json!([annotated_tool("c")]));
        assert_eq!(cache.discovery_order, ["d", "c"]);
        assert_eq!(cache.retained_bytes, small_bytes + large_bytes);
        discover(
            &mut cache,
            json!([{"name":"d", "inputSchema":{"type":"object"}}]),
        );
        assert_eq!(cache.retained_bytes, small_bytes);
        discover(
            &mut cache,
            json!([{"name":"c", "inputSchema":{"type":"object", "properties":{
                "region":{"type":"number", "x-mcp-header":"Region"}
            }}}]),
        );
        assert_eq!(cache.retained_bytes, 0);
        assert!(cache.discovery_order.is_empty());
        discover(&mut cache, json!([annotated_tool("a")]));
        invalidate(&mut cache);
        assert_eq!(cache.retained_bytes, 0);
        discover(&mut cache, json!([annotated_tool("b")]));
        assert_eq!(cache.retained_bytes, small_bytes);
    }

    #[test]
    fn oversized_routing_refresh_retires_only_its_old_entry() {
        let mut cache = ToolHeaderCache::new(8, 2 * tool_bytes(&annotated_tool("a")));
        discover(
            &mut cache,
            json!([annotated_tool("a"), annotated_tool("b")]),
        );
        let mut oversized = annotated_tool("a");
        oversized["inputSchema"]["properties"]["region"]["x-mcp-header"] = json!("R".repeat(1024));
        assert_eq!(discover(&mut cache, json!([oversized])), json!([oversized]));
        assert!(cache.get("a").is_none());
        assert!(cache.get("b").is_some());
        assert_eq!(cache.discovery_order, ["b"]);
        assert_eq!(cache.retained_bytes, tool_bytes(&annotated_tool("b")));
        discover(&mut cache, json!([annotated_tool(&"n".repeat(1024))]));
        assert_eq!(cache.discovery_order, ["b"]);
        discover(&mut cache, json!([annotated_tool("a")]));
        assert_eq!(cache.discovery_order, ["b", "a"]);
    }

    #[test]
    fn routing_byte_budget_zero_and_churn_keep_validation_and_accounting_bounded() {
        let config = StreamableHttpClientTransportConfig::default();
        assert_eq!(config.max_routing_schema_bytes, 4 * 1024 * 1024);
        let config = config.max_routing_schema_bytes(0);
        let mut cache =
            ToolHeaderCache::new(config.max_routing_schemas, config.max_routing_schema_bytes);
        let tools = discover(
            &mut cache,
            json!([
                annotated_tool("valid"), {"name":"invalid", "inputSchema":{"type":"object", "properties":{
                    "region":{"type":"number", "x-mcp-header":"Region"}
                }}}
            ]),
        );
        assert_eq!(tools, json!([annotated_tool("valid")]));
        assert_eq!(cache.retained_bytes, 0);
        assert!(cache.schemas.is_empty());
        let mut cache = ToolHeaderCache::new(usize::MAX, 512);
        for index in 0..100 {
            discover(
                &mut cache,
                json!([annotated_tool(&format!("tool-{index}"))]),
            );
            assert!(cache.retained_bytes <= 512);
            let actual: usize = cache
                .schemas
                .iter()
                .map(|(name, entry)| {
                    name.len() + serde_json::to_vec(entry.schema.as_ref()).unwrap().len()
                })
                .sum();
            assert_eq!(cache.retained_bytes, actual);
            assert_eq!(cache.discovery_order.len(), cache.schemas.len());
        }
    }

    #[test]
    fn routing_retention_evicts_oldest_full_discovery_and_refreshes_existing_entries() {
        let mut cache = ToolHeaderCache::new(2, DEFAULT_MAX_ROUTING_SCHEMA_BYTES);
        discover(
            &mut cache,
            json!([annotated_tool("a"), annotated_tool("b")]),
        );
        discover(
            &mut cache,
            json!([annotated_tool("a"), annotated_tool("c")]),
        );
        assert!(cache.get("a").is_some());
        assert!(cache.get("b").is_none());
        assert!(cache.get("c").is_some());
        // Lean discovery neither erases a schema nor changes eviction order.
        discover(&mut cache, json!([{"name":"a"}]));
        assert!(cache.get("a").is_some());
        discover(&mut cache, json!([annotated_tool("d")]));
        assert!(cache.get("a").is_none());
        assert_eq!(cache.discovery_order, ["c", "d"]);
        for n in 0..100 {
            discover(&mut cache, json!([annotated_tool(&format!("tool-{n}"))]));
            assert_eq!(cache.schemas.len(), 2);
            assert_eq!(cache.discovery_order.len(), 2);
        }
        assert_eq!(cache.discovery_order, ["tool-98", "tool-99"]);
    }

    #[test]
    fn routing_retention_projects_annotations_without_changing_discovery_results() {
        let mut cache = ToolHeaderCache::new(1, DEFAULT_MAX_ROUTING_SCHEMA_BYTES);
        let tool = json!({"name":"deploy", "inputSchema":{
            "type":"object", "description":"large root documentation",
            "$defs":{"unused":{"type":"string", "description":"large definition"}},
            "required":["region"], "properties":{
                "region":{"type":"string", "x-mcp-header":"Region",
                    "description":"large property documentation", "enum":["eu","us"]},
                "payload":{"type":"object", "properties":{"nested":{"type":"string"}}}
            }
        }});
        assert_eq!(discover(&mut cache, json!([tool])), json!([tool]));
        let minimal = annotated_tool("deploy")["inputSchema"]
            .as_object()
            .unwrap()
            .clone();
        assert_eq!(cache.get("deploy").unwrap().as_ref(), &minimal);
        // Removing annotations retires the old routing contract without using a slot.
        discover(
            &mut cache,
            json!([{"name":"deploy", "inputSchema":{"type":"object"}}]),
        );
        assert!(cache.schemas.is_empty());
        assert!(cache.discovery_order.is_empty());
        discover(
            &mut cache,
            json!([annotated_tool("active"),
            {"name":"plain", "inputSchema":{"type":"object"}}]),
        );
        assert!(cache.get("active").is_some());
        assert!(cache.get("plain").is_none());
    }

    #[test]
    fn disabled_routing_retention_still_filters_invalid_schemas() {
        let config = StreamableHttpClientTransportConfig::default();
        assert_eq!(config.max_routing_schemas, 1024);
        let config = config.max_routing_schemas(0);
        let mut cache =
            ToolHeaderCache::new(config.max_routing_schemas, config.max_routing_schema_bytes);
        let tools = discover(
            &mut cache,
            json!([
                annotated_tool("valid"),
                {"name":"bad", "inputSchema":{"type":"object", "properties":{
                    "region":{"type":"number", "x-mcp-header":"Region"}
                }}}
            ]),
        );
        assert_eq!(tools, json!([annotated_tool("valid")]));
        assert!(cache.schemas.is_empty());
        assert!(cache.discovery_order.is_empty());
    }

    fn custom_routing_headers() -> HashMap<HeaderName, HeaderValue> {
        [
            ("mcp-method", "stale/method"),
            ("mcp-name", "stale-name"),
            ("mcp-param-region", "stale-region"),
            ("mcp-param-unrelated", "stale-param"),
            ("x-tenant", "tenant-a"),
            ("mcp-parametric", "application-value"),
        ]
        .into_iter()
        .map(|(name, value)| {
            (
                HeaderName::from_static(name),
                HeaderValue::from_static(value),
            )
        })
        .collect()
    }

    #[test]
    fn request_routing_headers_exclude_custom_values_without_matching_body_fields() {
        let base = custom_routing_headers();
        for request in [
            json!({"method":"ping"}),
            json!({"method":"tools/call", "params":{"name":"deploy", "arguments":{}}}),
            json!({"method":"tools/call", "params":{"name":"deploy", "arguments":{"region":"eu"}}}),
        ] {
            let message = serde_json::from_value(json!({
                "jsonrpc":"2.0", "id":1,
                "method":request["method"], "params":request.get("params").cloned().unwrap_or(json!({}))
            })).unwrap();
            let headers = build_request_headers(
                &base,
                &message,
                &ToolHeaderCache::new(0, DEFAULT_MAX_ROUTING_SCHEMA_BYTES),
                &ProtocolVersion::V_2026_07_28,
            );
            assert_eq!(
                headers[&HeaderName::from_static("mcp-method")],
                request["method"].as_str().unwrap()
            );
            assert_eq!(
                headers
                    .get(&HeaderName::from_static("mcp-name"))
                    .map(|value| value.to_str().unwrap()),
                request
                    .pointer("/params/name")
                    .and_then(serde_json::Value::as_str)
            );
            assert!(
                !headers
                    .keys()
                    .any(|name| name.as_str().starts_with("mcp-param-"))
            );
            assert_eq!(headers[&HeaderName::from_static("x-tenant")], "tenant-a");
            assert_eq!(
                headers[&HeaderName::from_static("mcp-parametric")],
                "application-value"
            );
        }
        assert_eq!(base[&HeaderName::from_static("mcp-name")], "stale-name");
    }

    #[test]
    fn optional_promoted_headers_are_rebuilt_for_each_request() {
        let cache = ToolHeaderCache::from([(
            "deploy".to_owned(),
            Arc::new(
                json!({"type":"object", "properties":{
                    "region":{"type":"string", "x-mcp-header":"Region"}
                }})
                .as_object()
                .unwrap()
                .clone(),
            ),
        )]);
        for arguments in [json!({"region":"eu"}), json!({})] {
            let message = serde_json::from_value(json!({
                "jsonrpc":"2.0", "id":1, "method":"tools/call",
                "params":{"name":"deploy", "arguments":arguments}
            }))
            .unwrap();
            let headers = build_request_headers(
                &custom_routing_headers(),
                &message,
                &cache,
                &ProtocolVersion::V_2026_07_28,
            );
            assert_eq!(
                headers
                    .get(&HeaderName::from_static("mcp-param-region"))
                    .map(|value| value.to_str().unwrap()),
                arguments.get("region").and_then(serde_json::Value::as_str)
            );
            assert!(!headers.contains_key(&HeaderName::from_static("mcp-param-unrelated")));
        }
    }

    #[cfg(feature = "__reqwest")]
    #[tokio::test]
    async fn completed_stream_drain_validates_and_learns_routing_schemas_before_delivery() {
        let valid = Arc::new(
            json!({"type":"object", "properties":{
                "region":{"type":"string", "x-mcp-header":"Region"}
            }})
            .as_object()
            .unwrap()
            .clone(),
        );
        let mut cache = ToolHeaderCache::from([
            ("invalid".to_owned(), valid.clone()),
            ("unrelated".to_owned(), valid.clone()),
        ]);
        let response = |id, tools| {
            serde_json::from_value(json!({
                "jsonrpc":"2.0", "id":id, "result":{"tools":tools}
            }))
            .unwrap()
        };
        let (stream_tx, mut stream_rx) = tokio::sync::mpsc::channel(4);
        // Queue both terminal responses before entering the drain. This forces
        // the stream-completion path without relying on select! scheduling.
        stream_tx
            .send(response(
                1,
                json!([
                    {"name":"deploy", "inputSchema":valid},
                    {"name":"invalid", "inputSchema":{"type":"object", "properties":{
                        "region":{"type":"number", "x-mcp-header":"Region"}
                    }}},
                    {"name":"unrelated"}
                ]),
            ))
            .await
            .unwrap();
        stream_tx
            .send(response(2, json!([{"name":"deploy"}])))
            .await
            .unwrap();
        drop(stream_tx);
        let (handler_tx, mut handler_rx) = tokio::sync::mpsc::channel(4);
        let mut pending = HashSet::from([RequestId::Number(1), RequestId::Number(2)]);
        cache.track_discovery(RequestId::Number(1));
        cache.track_discovery(RequestId::Number(2));
        StreamableHttpClientWorker::<reqwest::Client>::drain_queued_stream_messages(
            &mut stream_rx,
            &handler_tx,
            &mut pending,
            &mut HashMap::new(),
            &mut cache,
            &ProtocolVersion::V_2026_07_28,
        )
        .await
        .unwrap();
        assert!(pending.is_empty());
        assert_eq!(cache.get("deploy"), Some(&valid));
        assert_eq!(cache.get("unrelated"), Some(&valid));
        assert!(!cache.schemas.contains_key("invalid"));
        for (id, expected_names) in [(1, vec!["deploy", "unrelated"]), (2, vec!["deploy"])] {
            let ServerJsonRpcMessage::Response(response) = handler_rx.try_recv().unwrap() else {
                panic!("expected response")
            };
            assert_eq!(response.id, RequestId::Number(id));
            let ServerResult::ListToolsResult(list) = response.result else {
                panic!("expected tools list")
            };
            assert_eq!(
                list.tools
                    .iter()
                    .map(|tool| tool.name.as_ref())
                    .collect::<Vec<_>>(),
                expected_names
            );
        }
        assert!(handler_rx.try_recv().is_err());
        let message = serde_json::from_value(json!({
            "jsonrpc":"2.0", "id":3, "method":"tools/call",
            "params":{"name":"deploy", "arguments":{"region":"eu"}}
        }))
        .unwrap();
        let headers = build_request_headers(
            &HashMap::new(),
            &message,
            &cache,
            &ProtocolVersion::V_2026_07_28,
        );
        assert_eq!(headers[&HeaderName::from_static("mcp-param-region")], "eu");
    }

    #[test]
    fn invalid_full_refresh_retires_previous_routing_schema_only_for_that_tool() {
        let valid = Arc::new(
            json!({"type":"object", "properties":{
                "region":{"type":"string", "x-mcp-header":"Region"}
            }})
            .as_object()
            .unwrap()
            .clone(),
        );
        let mut cache = ToolHeaderCache::from([
            ("deploy".to_owned(), valid.clone()),
            ("other".to_owned(), valid.clone()),
        ]);
        let mut response: ServerJsonRpcMessage = serde_json::from_value(json!({
            "jsonrpc":"2.0", "id":1, "result":{"tools":[{
                "name":"deploy", "inputSchema":{"type":"object", "$defs":{
                    "nested":{"type":"string", "x-mcp-header":"Nested"}
                }}
            }]}
        }))
        .unwrap();
        cache.track_discovery(RequestId::Number(1));
        cache_tools_from_response(&mut cache, &mut response, &ProtocolVersion::V_2026_07_28);
        assert!(!cache.schemas.contains_key("deploy"));
        assert_eq!(cache.get("other"), Some(&valid));
        let ServerJsonRpcMessage::Response(response) = response else {
            panic!("expected response")
        };
        let ServerResult::ListToolsResult(list) = response.result else {
            panic!("expected tools list")
        };
        assert!(list.tools.is_empty());
    }
}

#[cfg(all(test, feature = "__reqwest"))]
mod response_stream_lifetime_tests {
    use super::*;

    type TestWorker = StreamableHttpClientWorker<reqwest::Client>;

    // Ending the task must drop the actual stream, not merely stop polling it.
    fn pending_stream(
        dropped: CancellationToken,
    ) -> BoxStream<'static, Result<ServerJsonRpcMessage, StreamableHttpError<reqwest::Error>>> {
        let guard = dropped.drop_guard();
        futures::stream::once(async move {
            let _guard = guard;
            std::future::pending().await
        })
        .boxed()
    }

    fn notification() -> ServerJsonRpcMessage {
        serde_json::from_value(serde_json::json!({
            "jsonrpc":"2.0", "method":"notifications/tools/list_changed"
        }))
        .unwrap()
    }

    fn response() -> ServerJsonRpcMessage {
        serde_json::from_value(serde_json::json!({
            "jsonrpc":"2.0", "id":7, "result":{"tools":[]}
        }))
        .unwrap()
    }

    async fn drain_tail(
        tail: impl Stream<Item = Result<ServerJsonRpcMessage, StreamableHttpError<reqwest::Error>>>
        + Send,
        cancellation: CancellationToken,
    ) {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let source = futures::stream::iter([Ok(response())]).chain(tail);
        tokio::time::timeout(
            Duration::from_secs(1),
            TestWorker::execute_sse_stream(
                source,
                tx,
                InboundStreamOrigin::OutboundRequest(RequestId::Number(7)),
                true,
                cancellation,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(
            rx.recv().await,
            Some(ServerJsonRpcMessage::Response(_))
        ));
        // Trailing messages cannot be delivered as new responses/notifications.
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn ready_response_tail_has_a_finite_work_budget() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let polls = Arc::new(AtomicUsize::new(0));
        let observed = polls.clone();
        let dropped = CancellationToken::new();
        let guard = dropped.clone().drop_guard();
        let tail = futures::stream::poll_fn(move |_| {
            let _guard = &guard;
            let count = observed.fetch_add(1, Ordering::Relaxed);
            // Fail promptly against an unbounded reader instead of hanging the test.
            assert!(count < 128, "response drain exhausted its work budget");
            std::task::Poll::Ready(Some(Ok(notification())))
        });
        drain_tail(tail, CancellationToken::new()).await;
        assert!(polls.load(Ordering::Relaxed) <= 32);
        assert!(dropped.is_cancelled());
    }

    #[tokio::test(start_paused = true)]
    async fn ready_response_tail_yields_to_stream_cancellation() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let cancellation = CancellationToken::new();
        let signal = cancellation.clone();
        let polls = Arc::new(AtomicUsize::new(0));
        let observed = polls.clone();
        let tail = futures::stream::poll_fn(move |_| {
            let count = observed.fetch_add(1, Ordering::Relaxed);
            assert!(count < 128, "cancelled drain kept consuming messages");
            signal.cancel();
            std::task::Poll::Ready(Some(Ok(notification())))
        });
        drain_tail(tail, cancellation).await;
        assert_eq!(polls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn terminal_response_drain_stops_at_the_first_error() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let polls = Arc::new(AtomicUsize::new(0));
        let observed = polls.clone();
        let tail = futures::stream::poll_fn(move |_| {
            assert_eq!(
                observed.fetch_add(1, Ordering::Relaxed),
                0,
                "polled past stream error"
            );
            std::task::Poll::Ready(Some(Err(StreamableHttpError::SessionExpired)))
        });
        // The final response was already delivered; a drain error must not replace it.
        drain_tail(tail, CancellationToken::new()).await;
        assert_eq!(polls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn short_response_tail_reaches_eof_for_connection_reuse() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let eof = Arc::new(AtomicBool::new(false));
        let observed = eof.clone();
        let mut left = 3;
        let tail = futures::stream::poll_fn(move |_| {
            if left > 0 {
                left -= 1;
                std::task::Poll::Ready(Some(Ok(notification())))
            } else {
                observed.store(true, Ordering::Relaxed);
                std::task::Poll::Ready(None)
            }
        });
        drain_tail(tail, CancellationToken::new()).await;
        assert!(eof.load(Ordering::Relaxed));
    }

    #[tokio::test(start_paused = true)]
    async fn stream_cancellation_interrupts_full_handler_queue_for_request_and_common_streams() {
        for close_on_response in [false, true] {
            let cancellation = CancellationToken::new();
            let sibling = cancellation.child_token();
            let stream_ct = cancellation.child_token();
            let dropped = CancellationToken::new();
            let (tx, mut rx) = tokio::sync::mpsc::channel(1);
            tx.send(notification()).await.unwrap();
            let source =
                futures::stream::iter([Ok(notification())]).chain(pending_stream(dropped.clone()));
            let origin = if close_on_response {
                InboundStreamOrigin::OutboundRequest(RequestId::Number(7))
            } else {
                InboundStreamOrigin::Unassociated
            };
            let mut stream = Box::pin(TestWorker::execute_sse_stream(
                source,
                tx,
                origin,
                close_on_response,
                stream_ct.clone(),
            ));
            assert!(futures::poll!(stream.as_mut()).is_pending());
            assert!(!dropped.is_cancelled());
            stream_ct.cancel();
            // Must finish without draining handler capacity or advancing time.
            assert!(matches!(
                futures::poll!(stream.as_mut()),
                std::task::Poll::Ready(Ok(()))
            ));
            drop(stream);
            assert!(dropped.is_cancelled());
            assert!(!sibling.is_cancelled());
            assert!(!cancellation.is_cancelled());
            assert!(rx.recv().await.is_some());
            assert!(rx.recv().await.is_none());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn stream_cancellation_interrupts_terminal_response_drain() {
        let cancellation = CancellationToken::new();
        let dropped = CancellationToken::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let source = futures::stream::iter([Ok(response())]).chain(pending_stream(dropped.clone()));
        let mut stream = Box::pin(TestWorker::execute_sse_stream(
            source,
            tx,
            InboundStreamOrigin::OutboundRequest(RequestId::Number(7)),
            true,
            cancellation.clone(),
        ));
        let before = tokio::time::Instant::now();
        assert!(futures::poll!(stream.as_mut()).is_pending());
        assert!(matches!(
            rx.recv().await,
            Some(ServerJsonRpcMessage::Response(_))
        ));
        cancellation.cancel();
        assert!(matches!(
            futures::poll!(stream.as_mut()),
            std::task::Poll::Ready(Ok(()))
        ));
        drop(stream);
        assert!(dropped.is_cancelled());
        assert_eq!(tokio::time::Instant::now(), before);
    }

    #[tokio::test(start_paused = true)]
    async fn uncancelled_terminal_response_keeps_bounded_connection_reuse_drain() {
        let dropped = CancellationToken::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let source = futures::stream::iter([Ok(response())]).chain(pending_stream(dropped.clone()));
        let mut stream = Box::pin(TestWorker::execute_sse_stream(
            source,
            tx,
            InboundStreamOrigin::OutboundRequest(RequestId::Number(7)),
            true,
            CancellationToken::new(),
        ));
        assert!(futures::poll!(stream.as_mut()).is_pending());
        assert!(matches!(
            rx.recv().await,
            Some(ServerJsonRpcMessage::Response(_))
        ));
        tokio::time::advance(Duration::from_millis(49)).await;
        assert!(futures::poll!(stream.as_mut()).is_pending());
        assert!(!dropped.is_cancelled());
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(matches!(
            futures::poll!(stream.as_mut()),
            std::task::Poll::Ready(Ok(()))
        ));
        drop(stream);
        assert!(dropped.is_cancelled());
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn caller_drop_releases_native_response_stream_before_or_after_polling() {
        for already_cancelled in [false, true] {
            let caller = CancellationToken::new();
            let request = CancellationToken::new();
            let dropped = CancellationToken::new();
            let (tx, _rx) = tokio::sync::mpsc::channel(1);
            if already_cancelled {
                caller.cancel();
            }
            let mut stream = Box::pin(TestWorker::run_response_stream(
                pending_stream(dropped.clone()),
                tx,
                InboundStreamOrigin::OutboundRequest(RequestId::Number(7)),
                request.clone(),
                request.clone(),
                true,
                caller.clone(),
            ));
            if !already_cancelled {
                assert!(futures::poll!(stream.as_mut()).is_pending());
                assert!(!dropped.is_cancelled());
                caller.cancel();
            }
            tokio::time::timeout(Duration::from_secs(1), stream)
                .await
                .unwrap()
                .unwrap();
            assert!(dropped.is_cancelled());
            // Completion handling must recognize abandonment, not synthesize an error.
            assert!(request.is_cancelled());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn caller_drop_interrupts_a_response_stream_blocked_on_handler_capacity() {
        let caller = CancellationToken::new();
        let request = CancellationToken::new();
        let dropped = CancellationToken::new();
        let message: ServerJsonRpcMessage = serde_json::from_value(serde_json::json!({
            "jsonrpc":"2.0", "method":"notifications/tools/list_changed"
        }))
        .unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        tx.send(message.clone()).await.unwrap();
        let source = futures::stream::iter([Ok(message)])
            .chain(pending_stream(dropped.clone()))
            .boxed();
        let mut stream = Box::pin(TestWorker::run_response_stream(
            source,
            tx,
            InboundStreamOrigin::OutboundRequest(RequestId::Number(7)),
            request.clone(),
            request.clone(),
            true,
            caller.clone(),
        ));
        assert!(futures::poll!(stream.as_mut()).is_pending());
        caller.cancel();
        tokio::time::timeout(Duration::from_secs(1), stream)
            .await
            .unwrap()
            .unwrap();
        assert!(dropped.is_cancelled());
        assert!(request.is_cancelled());
        assert!(rx.recv().await.is_some());
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn optional_adapter_retains_stream_until_its_existing_lifetime_ends() {
        let caller = CancellationToken::new();
        let request = CancellationToken::new();
        let lifetime = CancellationToken::new();
        let dropped = CancellationToken::new();
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let mut stream = Box::pin(TestWorker::run_response_stream(
            pending_stream(dropped.clone()),
            tx,
            InboundStreamOrigin::OutboundRequest(RequestId::Number(7)),
            request.clone(),
            lifetime.clone(),
            false,
            caller.clone(),
        ));
        assert!(futures::poll!(stream.as_mut()).is_pending());
        caller.cancel();
        assert!(futures::poll!(stream.as_mut()).is_pending());
        assert!(!dropped.is_cancelled());
        request.cancel();
        assert!(futures::poll!(stream.as_mut()).is_pending());
        assert!(!dropped.is_cancelled());
        lifetime.cancel();
        tokio::time::timeout(Duration::from_secs(1), stream)
            .await
            .unwrap()
            .unwrap();
        assert!(dropped.is_cancelled());
    }
}
