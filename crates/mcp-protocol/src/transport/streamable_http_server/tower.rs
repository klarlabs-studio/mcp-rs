// Derived from rmcp 3.5.1; maintained and modified by mcp-rs. See ../../../UPSTREAM.md.
use std::{
    borrow::Cow,
    convert::Infallible,
    fmt::Display,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use futures::{Stream, StreamExt, future::BoxFuture};
use http::{HeaderMap, Method, Request, Response, header::ALLOW};
use http_body::Body;
use http_body_util::{BodyExt, Full, combinators::BoxBody};
use pin_project_lite::pin_project;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use super::session::{EventStore, EventStoreError, SessionManager};
use crate::{
    RoleServer,
    model::{
        ClientJsonRpcMessage, ClientRequest, ErrorCode, ErrorData, GetExtensions, GetMeta,
        JsonObject, JsonRpcError, ProtocolVersion, RequestId, ServerJsonRpcMessage,
    },
    service::serve_directly_with_ct,
    transport::{
        OneshotTransport,
        common::{
            http_header::{
                EVENT_STREAM_MIME_TYPE, HEADER_LAST_EVENT_ID, HEADER_MCP_METHOD,
                HEADER_MCP_PROTOCOL_VERSION, JSON_MIME_TYPE,
            },
            mcp_headers,
            server_side_http::{
                BoxResponse, ServerSseMessage, accepted_response, expect_json,
                internal_error_response, sse_stream_response,
            },
        },
    },
};

/// Default maximum POST request body size (4 MiB).
pub(crate) const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = 4 * 1024 * 1024;
const STATELESS_STREAM_CHANNEL_CAPACITY: usize = 16;

struct ErrorResponse(Box<BoxResponse>);

impl ErrorResponse {
    fn into_response(self) -> BoxResponse {
        *self.0
    }
}

impl From<BoxResponse> for ErrorResponse {
    fn from(response: BoxResponse) -> Self {
        Self(Box::new(response))
    }
}

type HttpResult<T> = Result<T, ErrorResponse>;

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct StreamableHttpServerConfig {
    /// The ping message duration for SSE connections.
    pub sse_keep_alive: Option<Duration>,
    /// The retry interval for SSE priming events.
    pub sse_retry: Option<Duration>,
    /// Prefer
    /// `Content-Type: application/json` for simple request-response tools.
    /// If the handler emits a notification or request before the final response,
    /// the server falls back to `text/event-stream` so no message is lost.
    pub json_response: bool,
    /// Cancellation token for the Streamable HTTP server.
    ///
    /// When this token is cancelled, all active sessions are terminated and
    /// the server stops accepting new requests.
    pub cancellation_token: CancellationToken,
    /// Allowed hostnames or `host:port` authorities for inbound `Host` validation.
    ///
    /// By default, Streamable HTTP servers only accept loopback hosts to
    /// prevent DNS rebinding attacks against locally running servers. Public
    /// deployments should override this list with their own hostnames.
    /// examples:
    ///     allowed_hosts = ["localhost", "127.0.0.1", "0.0.0.0"]
    /// or with ports:
    ///     allowed_hosts = ["example.com", "example.com:8080"]
    pub allowed_hosts: Vec<String>,
    /// Allowed browser origins for inbound `Origin` validation.
    ///
    /// Defaults to an empty list, which disables Origin validation for backward
    /// compatibility. A non-empty list enables validation. Requests carrying
    /// an `Origin` header must match per RFC 6454 `(scheme, host, port)`;
    /// missing-`Origin` requests still pass. Entries must include a scheme;
    /// `"null"` matches the browser's `Origin: null`.
    ///
    /// Ports:
    /// - `:*` matches any port.
    /// - An explicit port matches only that port. An `Origin` without a port
    ///   uses the scheme default (443 for `https`, 80 for `http`).
    /// - An entry without a port currently matches any port. This is
    ///   deprecated: a future release will match only the scheme default
    ///   port. Use `:*` or an explicit port instead.
    ///
    /// Call [`StreamableHttpServerConfig::enforce_origin_validation`] to enable
    /// validation with an empty list, rejecting every present Origin value.
    /// Example: `allowed_origins = ["https://app.example.com:443", "http://localhost:*"]`.
    pub allowed_origins: Vec<String>,
    validate_empty_origin_allowlist: bool,
    /// Maximum POST request body size in bytes.
    ///
    /// Enforced while streaming the body, independent of `Content-Length`,
    /// chunked transfer encoding, or HTTP version. Oversized payloads receive
    /// a `413 Payload Too Large` response.
    pub max_request_body_bytes: usize,
    /// Require stateless JSON-RPC request POSTs to carry per-request protocol
    /// signals before handler dispatch.
    ///
    /// Non-initialize requests must carry `MCP-Protocol-Version`; ordinary
    /// non-discovery requests must also carry
    /// `_meta.io.modelcontextprotocol/protocolVersion`. `server/discover`
    /// retains its existing request-metadata validation. For `2026-07-28`
    /// requests, the server handler continues to require the remaining
    /// per-request metadata, including `clientCapabilities`. Initialize,
    /// notifications, and other message kinds retain their existing rules.
    ///
    /// Enabled by default. All request handling still validates modern inline
    /// metadata before dispatch even when the HTTP header policy is disabled.
    pub stateless_protocol_metadata_required: bool,
}

impl Default for StreamableHttpServerConfig {
    fn default() -> Self {
        Self {
            sse_keep_alive: Some(Duration::from_secs(15)),
            sse_retry: Some(Duration::from_secs(3)),

            json_response: false,
            cancellation_token: CancellationToken::new(),
            allowed_hosts: vec!["localhost".into(), "127.0.0.1".into(), "::1".into()],
            allowed_origins: vec![],
            validate_empty_origin_allowlist: false,
            max_request_body_bytes: DEFAULT_MAX_REQUEST_BODY_BYTES,
            stateless_protocol_metadata_required: true,
        }
    }
}

impl StreamableHttpServerConfig {
    pub fn with_allowed_hosts(
        mut self,
        allowed_hosts: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.allowed_hosts = allowed_hosts.into_iter().map(Into::into).collect();
        self
    }
    /// Disable allowed hosts. This will allow requests with any `Host` header, which is NOT recommended for public deployments.
    pub fn disable_allowed_hosts(mut self) -> Self {
        self.allowed_hosts.clear();
        self
    }
    pub fn with_allowed_origins(
        mut self,
        allowed_origins: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.allowed_origins = allowed_origins.into_iter().map(Into::into).collect();
        self
    }
    /// Enable Origin validation, including when the allowed Origins list is empty.
    pub fn enforce_origin_validation(mut self) -> Self {
        self.validate_empty_origin_allowlist = true;
        self
    }
    /// Disable Origin validation, allowing requests with any `Origin` header.
    pub fn disable_allowed_origins(mut self) -> Self {
        self.allowed_origins.clear();
        self.validate_empty_origin_allowlist = false;
        self
    }
    pub fn with_sse_keep_alive(mut self, duration: Option<Duration>) -> Self {
        self.sse_keep_alive = duration;
        self
    }

    pub fn with_sse_retry(mut self, duration: Option<Duration>) -> Self {
        self.sse_retry = duration;
        self
    }

    pub fn with_json_response(mut self, json_response: bool) -> Self {
        self.json_response = json_response;
        self
    }

    pub fn with_cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancellation_token = token;
        self
    }

    /// Set the maximum POST request body size in bytes.
    pub fn with_max_request_body_bytes(mut self, bytes: usize) -> Self {
        self.max_request_body_bytes = bytes;
        self
    }

    /// Require per-request protocol signals on stateless JSON-RPC request
    /// POSTs.
    ///
    /// See [`StreamableHttpServerConfig::stateless_protocol_metadata_required`].
    pub fn with_stateless_protocol_metadata_required(
        mut self,
        stateless_protocol_metadata_required: bool,
    ) -> Self {
        self.stateless_protocol_metadata_required = stateless_protocol_metadata_required;
        self
    }
}

/// Validates the `MCP-Protocol-Version` header on incoming HTTP requests.
///
/// Per the MCP 2025-06-18 spec:
/// - If the header is present but contains an unsupported version, return 400 Bad Request.
/// - If the header is absent, assume `2025-03-26` for backwards compatibility (no error).
fn validate_protocol_version_header(
    headers: &http::HeaderMap,
    allow_unknown: bool,
) -> HttpResult<()> {
    if let Some(value) = headers.get(HEADER_MCP_PROTOCOL_VERSION) {
        let version_str = value.to_str().map_err(|_| {
            Response::builder()
                .status(http::StatusCode::BAD_REQUEST)
                .body(
                    Full::new(Bytes::from(
                        "Bad Request: Invalid MCP-Protocol-Version header encoding",
                    ))
                    .boxed(),
                )
                .expect("valid response")
        })?;
        let is_known = ProtocolVersion::KNOWN_VERSIONS
            .iter()
            .any(|v| v.as_str() == version_str);
        if !allow_unknown && !is_known {
            return Err(Response::builder()
                .status(http::StatusCode::BAD_REQUEST)
                .body(
                    Full::new(Bytes::from(format!(
                        "Bad Request: Unsupported MCP-Protocol-Version: {version_str}"
                    )))
                    .boxed(),
                )
                .expect("valid response")
                .into());
        }
    }
    Ok(())
}

fn message_has_per_request_protocol_version(message: &ClientJsonRpcMessage) -> bool {
    match message {
        ClientJsonRpcMessage::Request(request) => {
            request.request.get_meta().protocol_version().is_some()
        }
        _ => false,
    }
}

fn method_not_allowed_response() -> BoxResponse {
    Response::builder()
        .status(http::StatusCode::METHOD_NOT_ALLOWED)
        .header(ALLOW, "POST")
        .body(Full::new(Bytes::from("Method Not Allowed")).boxed())
        .expect("valid response")
}

async fn persist_and_forward_event(
    event_store: &dyn EventStore,
    stream_id: &str,
    mut event: ServerSseMessage,
    output: &mut Option<tokio::sync::mpsc::Sender<ServerSseMessage>>,
) -> Result<(), EventStoreError> {
    event.event_id = Some(event_store.store_event(stream_id, &event).await?);
    if let Some(sender) = output
        && sender.send(event).await.is_err()
    {
        *output = None;
    }
    Ok(())
}

fn invalid_request_jsonrpc_response(
    id: Option<RequestId>,
    message: impl Into<Cow<'static, str>>,
) -> BoxResponse {
    let err = JsonRpcError::new(id, ErrorData::invalid_request(message, None));
    let body = serde_json::to_vec(&err).expect("serialize JsonRpcError");
    Response::builder()
        .status(http::StatusCode::BAD_REQUEST)
        .header(http::header::CONTENT_TYPE, JSON_MIME_TYPE)
        .body(Full::new(Bytes::from(body)).boxed())
        .expect("valid response")
}

fn invalid_params_jsonrpc_response(
    id: Option<RequestId>,
    message: impl Into<Cow<'static, str>>,
) -> BoxResponse {
    let err = JsonRpcError::new(id, ErrorData::invalid_params(message, None));
    let body = serde_json::to_vec(&err).expect("serialize JsonRpcError");
    Response::builder()
        .status(http::StatusCode::BAD_REQUEST)
        .header(http::header::CONTENT_TYPE, JSON_MIME_TYPE)
        .body(Full::new(Bytes::from(body)).boxed())
        .expect("valid response")
}

/// Absent header is allowed; the first initialize round-trip may legitimately omit it.
fn validate_header_matches_init_body(
    headers: &http::HeaderMap,
    body_version: &str,
    request_id: Option<RequestId>,
) -> HttpResult<()> {
    let Some(header_value) = headers.get(HEADER_MCP_PROTOCOL_VERSION) else {
        return Ok(());
    };
    let header_str = header_value.to_str().map_err(|_| {
        invalid_request_jsonrpc_response(
            request_id.clone(),
            "Invalid Request: MCP-Protocol-Version header is not valid UTF-8",
        )
    })?;
    if header_str != body_version {
        tracing::warn!(
            header = header_str,
            body = body_version,
            "rejecting initialize: MCP-Protocol-Version header does not match params.protocolVersion"
        );
        return Err(invalid_request_jsonrpc_response(
            request_id,
            format!(
                "Invalid Request: MCP-Protocol-Version header ({header_str}) does not match initialize params.protocolVersion ({body_version})"
            ),
        )
        .into());
    }
    Ok(())
}

fn validate_request_protocol_version_meta(
    headers: &HeaderMap,
    message: &ClientJsonRpcMessage,
) -> HttpResult<()> {
    let ClientJsonRpcMessage::Request(request) = message else {
        return Ok(());
    };
    if matches!(&request.request, ClientRequest::InitializeRequest(_)) {
        return Ok(());
    }
    let is_discover = matches!(&request.request, ClientRequest::DiscoverRequest(_));
    let meta = request.request.get_meta();
    let header_version = headers
        .get(HEADER_MCP_PROTOCOL_VERSION)
        .and_then(|value| value.to_str().ok());
    let Some(meta_version) = meta.protocol_version() else {
        let requires_request_metadata = is_discover
            || header_version
                .is_some_and(|version| version >= ProtocolVersion::V_2026_07_28.as_str());
        if requires_request_metadata {
            let missing = meta.missing_required_keys(&ProtocolVersion::V_2026_07_28);
            return Err(invalid_params_jsonrpc_response(
                Some(request.id.clone()),
                format!(
                    "Invalid params: request _meta is missing or has malformed required fields: {}",
                    missing.join(", ")
                ),
            )
            .into());
        }
        return Ok(());
    };
    let Some(header_version) = header_version else {
        return Err(header_mismatch_jsonrpc_response(
            Some(request.id.clone()),
            "request _meta protocolVersion requires MCP-Protocol-Version header",
        )
        .into());
    };
    if header_version != meta_version.as_str() {
        return Err(header_mismatch_jsonrpc_response(
            Some(request.id.clone()),
            format!(
                "MCP-Protocol-Version header ({header_version}) does not match request _meta protocolVersion ({meta_version})"
            ),
        )
        .into());
    }
    Ok(())
}

/// When `stateless_protocol_metadata_required` is enabled in stateless mode,
/// every non-initialize Streamable HTTP JSON-RPC request POST must carry the
/// `MCP-Protocol-Version` HTTP header. A missing header is rejected with
/// HTTP 400 / JSON-RPC `-32020` before handler dispatch. `server/discover`
/// is included so the seam aligns with the per-POST header contract; its
/// body-metadata rule is preserved unchanged.
fn validate_required_protocol_header(
    config: &StreamableHttpServerConfig,
    headers: &HeaderMap,
    message: &ClientJsonRpcMessage,
) -> HttpResult<()> {
    if !config.stateless_protocol_metadata_required {
        return Ok(());
    }
    let ClientJsonRpcMessage::Request(request) = message else {
        // Notifications, response messages, and error messages are exempt.
        return Ok(());
    };
    if matches!(&request.request, ClientRequest::InitializeRequest(_)) {
        // Initialize keeps its own header-matching rule.
        return Ok(());
    }
    if headers.contains_key(HEADER_MCP_PROTOCOL_VERSION) {
        return Ok(());
    }
    Err(header_mismatch_jsonrpc_response(
        Some(request.id.clone()),
        "Missing MCP-Protocol-Version header for request requiring per-request protocol metadata",
    )
    .into())
}

/// When `stateless_protocol_metadata_required` is enabled in stateless mode,
/// every non-initialize, non-discover Streamable HTTP JSON-RPC request must
/// carry `io.modelcontextprotocol/protocolVersion` in `_meta`. A missing entry
/// is rejected with HTTP 400 / JSON-RPC `-32602` (invalid_params). `initialize`,
/// `server/discover` (whose body-metadata rule is already enforced by
/// `validate_request_protocol_version_meta`), notifications, and other message
/// kinds are exempt.
fn validate_required_protocol_meta(
    config: &StreamableHttpServerConfig,
    message: &ClientJsonRpcMessage,
) -> HttpResult<()> {
    if !config.stateless_protocol_metadata_required {
        return Ok(());
    }
    let ClientJsonRpcMessage::Request(request) = message else {
        return Ok(());
    };
    if matches!(
        &request.request,
        ClientRequest::InitializeRequest(_) | ClientRequest::DiscoverRequest(_)
    ) {
        return Ok(());
    }
    if request.request.get_meta().protocol_version().is_some() {
        return Ok(());
    }
    Err(invalid_params_jsonrpc_response(
        Some(request.id.clone()),
        "Invalid params: request requires protocolVersion in request _meta",
    )
    .into())
}

/// `request_meta_malformed` marks a request missing required `_meta` fields;
/// only then is `-32602` a malformed-request 400 rather than an in-band error.
fn jsonrpc_http_status(
    message: &ServerJsonRpcMessage,
    request_meta_malformed: bool,
) -> http::StatusCode {
    let ServerJsonRpcMessage::Error(error) = message else {
        return http::StatusCode::OK;
    };
    // Legacy requests bypass this mapper and retain HTTP 200 JSON-RPC errors.
    match error.error.code {
        ErrorCode::UNSUPPORTED_PROTOCOL_VERSION
        | ErrorCode::MISSING_REQUIRED_CLIENT_CAPABILITY
        | ErrorCode::HEADER_MISMATCH => http::StatusCode::BAD_REQUEST,
        ErrorCode::INVALID_PARAMS if request_meta_malformed => http::StatusCode::BAD_REQUEST,
        ErrorCode::METHOD_NOT_FOUND => http::StatusCode::NOT_FOUND,
        _ => http::StatusCode::OK,
    }
}

fn jsonrpc_message_response(
    message: ServerJsonRpcMessage,
    status: http::StatusCode,
) -> HttpResult<BoxResponse> {
    let body =
        serde_json::to_vec(&message).map_err(internal_error_response("serialize json response"))?;
    Ok(Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, JSON_MIME_TYPE)
        .body(Full::new(Bytes::from(body)).boxed())
        .expect("valid response"))
}

fn header_mismatch_jsonrpc_response(
    id: Option<RequestId>,
    message: impl Into<Cow<'static, str>>,
) -> BoxResponse {
    let err = JsonRpcError::new(id, ErrorData::header_mismatch(message, None));
    let body = serde_json::to_vec(&err).expect("serialize JsonRpcError");
    Response::builder()
        .status(http::StatusCode::BAD_REQUEST)
        .header(http::header::CONTENT_TYPE, JSON_MIME_TYPE)
        .body(Full::new(Bytes::from(body)).boxed())
        .expect("valid response")
}

/// Validates SEP-2243 `Mcp-Method` / `Mcp-Name` / `Mcp-Param-*` headers against the body.
///
/// Only enforced when the request declares a protocol version `>= STANDARD_HEADERS`.
/// The `initialize` handshake is exempt from *requiring* them: clients emit these
/// headers only after the version has been negotiated. But like
/// `validate_header_matches_init_body`, a *supplied* `Mcp-Method` header that
/// contradicts the body is rejected — middleboxes route on the header without
/// parsing the body. `tool_schema` supplies the called tool's input schema
/// so annotated `Mcp-Param-*` headers can be checked (no schema => those are skipped).
fn validate_standard_headers(
    headers: &HeaderMap,
    message: &ClientJsonRpcMessage,
    tool_schema: impl Fn(&str) -> Option<Arc<JsonObject>>,
) -> HttpResult<()> {
    let version_requires_headers = headers
        .get(HEADER_MCP_PROTOCOL_VERSION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|version| version >= ProtocolVersion::STANDARD_HEADERS.as_str());
    if !version_requires_headers {
        return Ok(());
    }

    let request_id = match message {
        ClientJsonRpcMessage::Request(req) => {
            if matches!(&req.request, ClientRequest::InitializeRequest(_)) {
                // The handshake may omit SEP-2243 headers, but a supplied
                // Mcp-Method header must still agree with the body: middleboxes
                // route on the header without parsing the body. `get_all` is
                // used so every supplied value is inspected: a contradictory
                // duplicate must not hide behind a matching first value, and a
                // present-but-non-UTF8 value is rejected rather than silently
                // treated as absent. Duplicates (even identical ones) are
                // rejected explicitly instead of passing by accident.
                let mut method_values = headers.get_all(HEADER_MCP_METHOD).iter();
                let Some(first) = method_values.next() else {
                    return Ok(());
                };
                if method_values.next().is_some() {
                    return Err(header_mismatch_jsonrpc_response(
                        Some(req.id.clone()),
                        "multiple Mcp-Method header values supplied; initialize requires exactly one",
                    )
                    .into());
                }
                if first != http::HeaderValue::from_static("initialize") {
                    return Err(header_mismatch_jsonrpc_response(
                        Some(req.id.clone()),
                        format!(
                            "Mcp-Method header `{}` does not match body method `initialize`",
                            String::from_utf8_lossy(first.as_bytes())
                        ),
                    )
                    .into());
                }
                return Ok(());
            }
            Some(req.id.clone())
        }
        ClientJsonRpcMessage::Notification(_) => None,
        _ => return Ok(()),
    };

    let Ok(value) = serde_json::to_value(message) else {
        return Ok(());
    };
    // For tools/call, look up the tool schema so Mcp-Param-* headers are validated.
    let schema = value
        .get("method")
        .and_then(|method| method.as_str())
        .filter(|method| *method == "tools/call")
        .and_then(|_| value.get("params"))
        .and_then(|params| params.get("name"))
        .and_then(|name| name.as_str())
        .and_then(tool_schema);
    if let Err(reason) = mcp_headers::validate_request_headers(headers, &value, schema.as_deref()) {
        return Err(header_mismatch_jsonrpc_response(request_id, reason).into());
    }
    Ok(())
}

fn forbidden_response(message: impl Into<String>) -> BoxResponse {
    Response::builder()
        .status(http::StatusCode::FORBIDDEN)
        .body(Full::new(Bytes::from(message.into())).boxed())
        .expect("valid response")
}

fn normalize_host(host: &str) -> String {
    host.trim_matches('[')
        .trim_matches(']')
        .to_ascii_lowercase()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalizedAuthority {
    host: String,
    port: Option<u16>,
}

fn normalize_authority(host: &str, port: Option<u16>) -> NormalizedAuthority {
    NormalizedAuthority {
        host: normalize_host(host),
        port,
    }
}

fn parse_allowed_authority(allowed: &str) -> Option<NormalizedAuthority> {
    let allowed = allowed.trim();
    if allowed.is_empty() {
        return None;
    }

    if let Ok(authority) = http::uri::Authority::try_from(allowed) {
        return Some(normalize_authority(authority.host(), authority.port_u16()));
    }

    Some(normalize_authority(allowed, None))
}

fn host_is_allowed(host: &NormalizedAuthority, allowed_hosts: &[String]) -> bool {
    if allowed_hosts.is_empty() {
        // If the allowed hosts list is empty, allow all hosts (not recommended).
        return true;
    }
    allowed_hosts
        .iter()
        .filter_map(|allowed| parse_allowed_authority(allowed))
        .any(|allowed| {
            allowed.host == host.host
                && match allowed.port {
                    Some(port) => host.port == Some(port),
                    None => true,
                }
        })
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum NormalizedOrigin {
    Null,
    Tuple {
        scheme: String,
        host: String,
        port: Option<u16>,
    },
}

fn parse_origin_value(value: &str) -> Option<NormalizedOrigin> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if value.eq_ignore_ascii_case("null") {
        return Some(NormalizedOrigin::Null);
    }
    let uri = http::Uri::try_from(value).ok()?;
    let scheme = uri.scheme_str()?.to_ascii_lowercase();
    let authority = uri.authority()?;
    Some(NormalizedOrigin::Tuple {
        scheme,
        host: normalize_host(authority.host()),
        port: authority.port_u16(),
    })
}

/// The port a scheme implies when an origin serialization omits it (RFC 6454 §4).
fn default_port(scheme: &str) -> Option<u16> {
    match scheme {
        "http" => Some(80),
        "https" => Some(443),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AllowedPort {
    /// `:*`
    Any,
    /// No port in the entry. Matches any port until the deprecation ends.
    Unspecified,
    Exact(u16),
}

impl AllowedPort {
    fn matches(self, scheme: &str, port: Option<u16>) -> bool {
        match self {
            AllowedPort::Any | AllowedPort::Unspecified => true,
            // RFC 6454 §6.2 omits the default port when serializing an origin.
            AllowedPort::Exact(allowed) => port.or_else(|| default_port(scheme)) == Some(allowed),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AllowedOrigin {
    Null,
    Tuple {
        scheme: String,
        host: String,
        port: AllowedPort,
    },
}

fn parse_allowed_origin(value: &str) -> Option<AllowedOrigin> {
    let value = value.trim();
    if let Some(base) = value.strip_suffix(":*") {
        let NormalizedOrigin::Tuple {
            scheme,
            host,
            port: None,
        } = parse_origin_value(base)?
        else {
            return None;
        };
        return Some(AllowedOrigin::Tuple {
            scheme,
            host,
            port: AllowedPort::Any,
        });
    }
    Some(match parse_origin_value(value)? {
        NormalizedOrigin::Null => AllowedOrigin::Null,
        NormalizedOrigin::Tuple { scheme, host, port } => AllowedOrigin::Tuple {
            scheme,
            host,
            port: port.map_or(AllowedPort::Unspecified, AllowedPort::Exact),
        },
    })
}

fn origin_is_allowed(origin: &NormalizedOrigin, allowed_origins: &[String]) -> bool {
    allowed_origins
        .iter()
        .filter_map(|raw| parse_allowed_origin(raw))
        .any(|allowed| match (&allowed, origin) {
            (AllowedOrigin::Null, NormalizedOrigin::Null) => true,
            (
                AllowedOrigin::Tuple {
                    scheme: a_scheme,
                    host: a_host,
                    port: a_port,
                },
                NormalizedOrigin::Tuple {
                    scheme: o_scheme,
                    host: o_host,
                    port: o_port,
                },
            ) => a_scheme == o_scheme && a_host == o_host && a_port.matches(o_scheme, *o_port),
            _ => false,
        })
}

fn warn_on_portless_allowed_origins(allowed_origins: &[String]) {
    for raw in allowed_origins {
        if let Some(AllowedOrigin::Tuple {
            port: AllowedPort::Unspecified,
            ..
        }) = parse_allowed_origin(raw)
        {
            tracing::warn!(
                allowed_origin = raw.as_str(),
                "allowed origin without a port matches any port; a future release will match \
                 only the scheme default port. Use `:*` or an explicit port instead",
            );
        }
    }
}

fn bad_request_response(message: &str) -> BoxResponse {
    let body = Full::from(message.to_string()).boxed();

    http::Response::builder()
        .status(http::StatusCode::BAD_REQUEST)
        .header(http::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(body)
        .expect("failed to build bad request response")
}

fn parse_host_header(uri: &http::Uri, headers: &HeaderMap) -> HttpResult<NormalizedAuthority> {
    if let Some(host) = headers.get(http::header::HOST) {
        let host_str = host
            .to_str()
            .inspect_err(|_| {
                tracing::warn!(host = ?host, "rejected request with non-UTF-8 Host header");
            })
            .map_err(|_| bad_request_response("Bad Request: Invalid Host header encoding"))?;
        let authority = http::uri::Authority::try_from(host_str)
            .inspect_err(|_| {
                tracing::warn!(
                    host = host_str,
                    "rejected request with malformed Host header"
                );
            })
            .map_err(|_| bad_request_response("Bad Request: Invalid Host header"))?;
        return Ok(normalize_authority(authority.host(), authority.port_u16()));
    }
    // HTTP/2 carries the host in `:authority`; middleware such as
    // `axum::Router::nest` can drop the `Host` header hyper synthesizes from it.
    let authority = uri.authority().ok_or_else(|| {
        tracing::warn!("rejected request with missing Host header and no :authority");
        bad_request_response("Bad Request: missing Host header")
    })?;
    Ok(normalize_authority(authority.host(), authority.port_u16()))
}

fn validate_dns_rebinding_headers(
    uri: &http::Uri,
    headers: &HeaderMap,
    config: &StreamableHttpServerConfig,
) -> HttpResult<()> {
    let host = parse_host_header(uri, headers)?;
    if !host_is_allowed(&host, &config.allowed_hosts) {
        tracing::warn!(
            host = ?host,
            "rejected request with disallowed Host header (possible DNS rebinding attempt)",
        );
        return Err(forbidden_response("Forbidden: Host header is not allowed").into());
    }
    validate_origin_header(headers, config)?;
    Ok(())
}

fn validate_origin_header(
    headers: &HeaderMap,
    config: &StreamableHttpServerConfig,
) -> HttpResult<()> {
    if !config.validate_empty_origin_allowlist && config.allowed_origins.is_empty() {
        return Ok(());
    }
    let Some(origin_header) = headers.get(http::header::ORIGIN) else {
        return Ok(());
    };
    let origin_str = origin_header
        .to_str()
        .inspect_err(|_| {
            tracing::warn!(origin = ?origin_header, "rejected request with non-UTF-8 Origin header");
        })
        .map_err(|_| forbidden_response("Forbidden: Invalid Origin header encoding"))?;
    let origin = parse_origin_value(origin_str).ok_or_else(|| {
        tracing::warn!(
            origin = origin_str,
            "rejected request with malformed Origin header",
        );
        forbidden_response("Forbidden: Invalid Origin header")
    })?;
    if !origin_is_allowed(&origin, &config.allowed_origins) {
        tracing::warn!(
            origin = ?origin,
            "rejected request with disallowed Origin header (possible cross-origin attack)",
        );
        return Err(forbidden_response("Forbidden: Origin header is not allowed").into());
    }
    Ok(())
}

/// # Streamable HTTP server
///
/// An HTTP service that implements the
/// [Streamable HTTP transport](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http)
/// for MCP servers.
///
/// ## Session management
///
/// Requests are stateless and use the 2026-07-28 discovery lifecycle.
///
/// ## Accessing HTTP request data from tool handlers
///
/// The service consumes the request body but injects the remaining
/// [`http::request::Parts`] into [`crate::model::Extensions`], which is
/// accessible through [`crate::service::RequestContext`].
///
/// ### Reading the raw HTTP parts
///
/// ```rust
/// use mcp_protocol::handler::server::tool::Extension;
/// use http::request::Parts;
/// async fn my_tool(Extension(parts): Extension<Parts>) {
///     tracing::info!("http parts:{parts:?}")
/// }
/// ```
///
/// ### Reading the session ID inside a tool handler
///
/// ```rust,ignore
/// use mcp_protocol::handler::server::tool::Extension;
/// use mcp_protocol::service::RequestContext;
/// use mcp_protocol::model::RoleServer;
///
/// #[tool(description = "session-aware tool")]
/// async fn my_tool(
///     &self,
///     Extension(parts): Extension<http::request::Parts>,
/// ) -> Result<CallToolResult, mcp_protocol::ErrorData> {
///     if let Some(session_id) = parts.headers.get("mcp-session-id") {
///         tracing::info!(?session_id, "called from session");
///     }
///     // ...
///     # todo!()
/// }
/// ```
///
/// ### Accessing custom axum/tower extension state
///
/// State added via axum's `Extension` layer is available inside
/// `Parts.extensions`:
///
/// ```rust,ignore
/// use mcp_protocol::service::RequestContext;
/// use mcp_protocol::model::RoleServer;
///
/// #[derive(Clone)]
/// struct AppState { /* ... */ }
///
/// #[tool(description = "example")]
/// async fn my_tool(
///     &self,
///     ctx: RequestContext<RoleServer>,
/// ) -> Result<CallToolResult, mcp_protocol::ErrorData> {
///     let parts = ctx.extensions.get::<http::request::Parts>().unwrap();
///     let state = parts.extensions.get::<AppState>().unwrap();
///     // use state...
///     # todo!()
/// }
/// ```
pub struct StreamableHttpService<S, M> {
    pub config: StreamableHttpServerConfig,
    session_manager: Arc<M>,
    service_factory: Arc<dyn Fn() -> Result<S, std::io::Error> + Send + Sync>,
}

impl<S, M> Clone for StreamableHttpService<S, M> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            session_manager: self.session_manager.clone(),
            service_factory: self.service_factory.clone(),
        }
    }
}

impl<RequestBody, S, M> tower_service::Service<Request<RequestBody>> for StreamableHttpService<S, M>
where
    RequestBody: Body + Send + 'static,
    S: crate::ServerHandler + Send + 'static,
    M: SessionManager,
    RequestBody::Error: Display,
    RequestBody::Data: Send + 'static,
{
    type Response = BoxResponse;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;
    fn call(&mut self, req: http::Request<RequestBody>) -> Self::Future {
        let service = self.clone();
        Box::pin(async move {
            let response = service.handle(req).await;
            Ok(response)
        })
    }
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
}

impl<S, M> StreamableHttpService<S, M>
where
    S: crate::ServerHandler + Send + 'static,
    M: SessionManager,
{
    pub fn new(
        service_factory: impl Fn() -> Result<S, std::io::Error> + Send + Sync + 'static,
        session_manager: Arc<M>,
        config: StreamableHttpServerConfig,
    ) -> Self {
        warn_on_portless_allowed_origins(&config.allowed_origins);
        Self {
            config,
            session_manager,
            service_factory: Arc::new(service_factory),
        }
    }
    fn get_service(&self) -> Result<S, std::io::Error> {
        (self.service_factory)()
    }

    fn persisted_stateless_stream(
        &self,
        first: Option<ServerJsonRpcMessage>,
        mut receiver: tokio::sync::mpsc::Receiver<ServerJsonRpcMessage>,
        request_ct: CancellationToken,
        event_store: Arc<dyn EventStore>,
    ) -> ReceiverStream<ServerSseMessage> {
        let (sender, output) = tokio::sync::mpsc::channel(STATELESS_STREAM_CHANNEL_CAPACITY);
        let stream_id = uuid::Uuid::new_v4().to_string();
        let retry = self.config.sse_retry;
        let server_ct = self.config.cancellation_token.child_token();

        tokio::spawn(async move {
            let mut sender = Some(sender);
            if let Some(retry) = retry
                && let Err(error) = persist_and_forward_event(
                    event_store.as_ref(),
                    &stream_id,
                    ServerSseMessage::retry(retry),
                    &mut sender,
                )
                .await
            {
                tracing::error!(%stream_id, %error, "failed to persist SSE priming event");
                request_ct.cancel();
                return;
            }

            let mut first = first;
            loop {
                let message = if let Some(message) = first.take() {
                    Some(message)
                } else {
                    tokio::select! {
                        message = receiver.recv() => message,
                        _ = server_ct.cancelled() => {
                            request_ct.cancel();
                            None
                        }
                    }
                };
                let Some(message) = message else {
                    break;
                };
                tracing::trace!(?message);
                if let Err(error) = persist_and_forward_event(
                    event_store.as_ref(),
                    &stream_id,
                    ServerSseMessage::from_message(message),
                    &mut sender,
                )
                .await
                {
                    tracing::error!(%stream_id, %error, "failed to persist SSE event");
                    request_ct.cancel();
                    break;
                }
            }
        });

        ReceiverStream::new(output)
    }

    fn stateless_sse_response(
        &self,
        first: Option<ServerJsonRpcMessage>,
        receiver: tokio::sync::mpsc::Receiver<ServerJsonRpcMessage>,
        request_ct: CancellationToken,
    ) -> BoxResponse {
        if let Some(event_store) = self.session_manager.event_store() {
            let stream = self.persisted_stateless_stream(first, receiver, request_ct, event_store);
            sse_stream_response(
                stream,
                self.config.sse_keep_alive,
                self.config.cancellation_token.child_token(),
            )
        } else {
            let stream = futures::stream::iter(first)
                .chain(ReceiverStream::new(receiver))
                .map(|message| {
                    tracing::trace!(?message);
                    ServerSseMessage::from_message(message)
                });
            sse_stream_response(
                CancelOnDisconnect::new(stream, request_ct),
                self.config.sse_keep_alive,
                self.config.cancellation_token.child_token(),
            )
        }
    }

    // The HTTP status must be known before opening an SSE stream.
    async fn serve_negotiated_request_directly(
        &self,
        service: S,
        mut request: crate::model::JsonRpcRequest<ClientRequest>,
        parts: http::request::Parts,
    ) -> HttpResult<BoxResponse> {
        let request_meta_malformed = !request
            .request
            .get_meta()
            .missing_required_keys(&ProtocolVersion::V_2026_07_28)
            .is_empty();
        request.request.extensions_mut().insert(parts);
        let (transport, mut receiver) =
            OneshotTransport::<RoleServer>::new(ClientJsonRpcMessage::Request(request));
        // Give this stateless request its own cancellation token so a client
        // disconnect can cancel the in-flight handler (#857), as in the
        // non-negotiated stateless path below.
        let request_ct = CancellationToken::new();
        let service = serve_directly_with_ct(service, transport, None, request_ct.clone());
        tokio::spawn(async move {
            let _ = service.waiting().await;
        });

        let cancel = self.config.cancellation_token.child_token();
        // Cancel the handler if the client disconnects while it is still
        // producing its first message (this future is dropped before
        // `receiver.recv()` completes). Disarmed once the handler emits
        // anything, so a normal response is never cancelled.
        let mut disconnect_guard = Some(request_ct.clone().drop_guard());
        let first = tokio::select! {
            message = receiver.recv() => {
                if let Some(guard) = disconnect_guard.take() {
                    guard.disarm();
                }
                message
            }
            _ = cancel.cancelled() => None,
        }
        .ok_or_else(|| {
            internal_error_response("empty response")(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "no response message received from handler",
            ))
        })?;

        let terminal = matches!(
            &first,
            ServerJsonRpcMessage::Response(_) | ServerJsonRpcMessage::Error(_)
        );
        let status = jsonrpc_http_status(&first, request_meta_malformed);
        if terminal && (self.config.json_response || status != http::StatusCode::OK) {
            // This message is the whole reply, so `receiver` is dropped here and
            // anything the handler emits afterwards is undeliverable. Cancel it so
            // a still-running handler stops instead of running on unobserved: its
            // terminal `send` would otherwise fail before adding the termination
            // permit, leaving the serve loop parked forever. A no-op when the
            // handler already completed.
            request_ct.cancel();
            return jsonrpc_message_response(first, status);
        }

        Ok(self.stateless_sse_response(Some(first), receiver, request_ct))
    }

    pub async fn handle<B>(&self, request: Request<B>) -> Response<BoxBody<Bytes, Infallible>>
    where
        B: Body + Send + 'static,
        B::Error: Display,
    {
        if let Err(response) =
            validate_dns_rebinding_headers(request.uri(), request.headers(), &self.config)
        {
            return response.into_response();
        }
        let method = request.method().clone();
        let supports_stateless_replay = self.session_manager.event_store().is_some();
        let allowed_methods = if supports_stateless_replay {
            "GET, POST"
        } else {
            "POST"
        };
        let result = match method {
            Method::POST => self.handle_post(request).await,
            Method::GET if supports_stateless_replay => self.handle_get(request).await,
            _ => {
                // Handle other methods or return an error
                let response = Response::builder()
                    .status(http::StatusCode::METHOD_NOT_ALLOWED)
                    .header(ALLOW, allowed_methods)
                    .body(Full::new(Bytes::from("Method Not Allowed")).boxed())
                    .expect("valid response");
                return response;
            }
        };
        match result {
            Ok(response) => response,
            Err(response) => response.into_response(),
        }
    }
    async fn handle_get<B>(&self, request: Request<B>) -> HttpResult<BoxResponse>
    where
        B: Body + Send + 'static,
        B::Error: Display,
    {
        // check accept header
        if !request
            .headers()
            .get(http::header::ACCEPT)
            .and_then(|header| header.to_str().ok())
            .is_some_and(|header| header.contains(EVENT_STREAM_MIME_TYPE))
        {
            return Ok(Response::builder()
                .status(http::StatusCode::NOT_ACCEPTABLE)
                .body(
                    Full::new(Bytes::from(
                        "Not Acceptable: Client must accept text/event-stream",
                    ))
                    .boxed(),
                )
                .expect("valid response"));
        }
        validate_protocol_version_header(request.headers(), false)?;

        let Some(last_event_id) = request
            .headers()
            .get(HEADER_LAST_EVENT_ID)
            .and_then(|value| value.to_str().ok())
        else {
            return Ok(method_not_allowed_response());
        };
        let Some(event_store) = self.session_manager.event_store() else {
            return Ok(method_not_allowed_response());
        };
        let stream = match event_store.replay_events_after(last_event_id).await {
            Ok(stream) => stream,
            Err(error) => {
                tracing::warn!(%error, "stateless SSE resume failed, returning empty stream");
                Box::pin(futures::stream::empty())
            }
        };
        Ok(sse_stream_response(
            stream,
            self.config.sse_keep_alive,
            self.config.cancellation_token.child_token(),
        ))
    }

    async fn handle_post<B>(&self, request: Request<B>) -> HttpResult<BoxResponse>
    where
        B: Body + Send + 'static,
        B::Error: Display,
    {
        // check accept header
        if !request
            .headers()
            .get(http::header::ACCEPT)
            .and_then(|header| header.to_str().ok())
            .is_some_and(|header| {
                header.contains(JSON_MIME_TYPE) && header.contains(EVENT_STREAM_MIME_TYPE)
            })
        {
            return Ok(Response::builder()
                .status(http::StatusCode::NOT_ACCEPTABLE)
                .body(Full::new(Bytes::from("Not Acceptable: Client must accept both application/json and text/event-stream")).boxed())
                .expect("valid response"));
        }

        // check content type
        if !request
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|header| header.to_str().ok())
            .is_some_and(|header| header.starts_with(JSON_MIME_TYPE))
        {
            return Ok(Response::builder()
                .status(http::StatusCode::UNSUPPORTED_MEDIA_TYPE)
                .body(
                    Full::new(Bytes::from(
                        "Unsupported Media Type: Content-Type must be application/json",
                    ))
                    .boxed(),
                )
                .expect("valid response"));
        }

        // json deserialize request body
        let (part, body) = request.into_parts();
        let message = match expect_json(body, self.config.max_request_body_bytes).await {
            Ok(message) => message,
            Err(response) => return Ok(response),
        };

        // Stateless mode:
        // - on initialize: the header (if present) must match `params.protocolVersion`
        // - on every other request: the header must name a known version.
        //
        // The opt-in seam applies only here so legacy session routing and
        // its error precedence remain unchanged.
        validate_required_protocol_header(&self.config, &part.headers, &message)?;
        let has_per_request_version = message_has_per_request_protocol_version(&message);
        match &message {
            ClientJsonRpcMessage::Request(req) => {
                if let ClientRequest::InitializeRequest(init_req) = &req.request {
                    validate_header_matches_init_body(
                        &part.headers,
                        init_req.params.protocol_version.as_str(),
                        Some(req.id.clone()),
                    )?;
                } else {
                    validate_protocol_version_header(&part.headers, has_per_request_version)?;
                }
            }
            _ => {
                validate_protocol_version_header(&part.headers, has_per_request_version)?;
            }
        }
        // Resolve exactly one service for schema admission and dispatch. Tool
        // definitions can change between requests (including absent -> present),
        // so retaining schemas by name would enforce a stale routing contract.
        let service = self
            .get_service()
            .map_err(internal_error_response("get service"))?;
        let service = service.request_snapshot().unwrap_or(service);
        validate_standard_headers(&part.headers, &message, |name| {
            service.get_tool(name).map(|tool| tool.input_schema)
        })?;
        validate_request_protocol_version_meta(&part.headers, &message)?;
        validate_required_protocol_meta(&self.config, &message)?;
        match message {
            ClientJsonRpcMessage::Request(request) => {
                self.serve_negotiated_request_directly(service, request, part)
                    .await
            }
            ClientJsonRpcMessage::Notification(_notification) => {
                // ignore
                Ok(accepted_response())
            }
            ClientJsonRpcMessage::Response(_json_rpc_response) => Ok(accepted_response()),
            ClientJsonRpcMessage::Error(_json_rpc_error) => Ok(accepted_response()),
        }
    }
}

pin_project! {
    /// Cancels an unpersisted stateless request when its response is dropped.
    ///
    /// Persisted requests keep running so another connection can resume them.
    /// Without an event store, dropping the stream fires the request's
    /// cancellation token. Natural completion disarms the guard.
    struct CancelOnDisconnect<S> {
        #[pin]
        inner: S,
        ct: Option<CancellationToken>,
    }
    impl<S> PinnedDrop for CancelOnDisconnect<S> {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            if let Some(ct) = this.ct.take() {
                ct.cancel();
            }
        }
    }
}

impl<S> CancelOnDisconnect<S> {
    fn new(inner: S, ct: CancellationToken) -> Self {
        Self {
            inner,
            ct: Some(ct),
        }
    }
}

impl<S: Stream> Stream for CancelOnDisconnect<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let polled = this.inner.poll_next(cx);
        if let Poll::Ready(None) = &polled {
            // Ended naturally: the request completed, so don't cancel on drop.
            *this.ct = None;
        }
        polled
    }
}
