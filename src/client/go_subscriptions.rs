//! Opt-in client for the pinned Go HTTP `subscriptions/listen` dialect.
//!
//! This owns a separate long-lived POST response, not an SDK `Subscription`.
//! Go returns a generated ID but no accepted filter or initial refresh hints.
//! Read current snapshots after opening and refresh on subsequent notifications.
//! Task IDs use Go's explicit filter extension. Reverse server adaptation is not provided.
use crate::{model, transport::sse::Decoder};
use futures_util::{Stream, StreamExt};
use reqwest::{Url, header::HeaderMap};
use serde_json::{Value, json};
use std::{num::NonZeroUsize, pin::Pin, time::Duration};

const META_ID: &str = "io.modelcontextprotocol/subscriptionId";
const REQUEST_ID: &str = "mcp-rs-go-listen";

/// Configuration for a stateless modern Go `/mcp` endpoint.
/// TLS, authentication, redirects, pooling, and any total HTTP timeout are
/// controlled by the supplied client. Avoid a total timeout on long-lived SSE.
#[derive(Clone)]
pub struct Config {
    /// Complete HTTP(S) POST endpoint without URL credentials or a fragment.
    pub endpoint: Url,
    /// Host-owned headers. Protocol framing headers are set by the adapter.
    pub headers: HeaderMap,
    /// Identity carried in every modern listen request.
    pub client_info: model::Implementation,
    /// Capabilities carried in every modern listen request.
    pub capabilities: model::ClientCapabilities,
    /// Maximum encoded request, JSON rejection body, or individual SSE event size.
    pub max_message_bytes: NonZeroUsize,
    /// Combined deadline for response headers and the first acknowledgement.
    pub connect_timeout: Duration,
}
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("max_message_bytes", &self.max_message_bytes)
            .field("connect_timeout", &self.connect_timeout)
            .finish_non_exhaustive()
    }
}
impl Config {
    /// Use a 1 MiB message limit and ten-second opening deadline.
    pub fn new(endpoint: Url) -> Self {
        Self {
            endpoint,
            headers: HeaderMap::new(),
            client_info: model::Implementation::new("mcp-rs", env!("CARGO_PKG_VERSION")),
            capabilities: model::ClientCapabilities::default(),
            max_message_bytes: NonZeroUsize::new(1 << 20).unwrap_or(NonZeroUsize::MIN),
            connect_timeout: Duration::from_secs(10),
        }
    }
}

/// HTTP, framing, validation, or peer rejection of a Go subscription.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// An HTTP operation failed. URL credentials/query data are redacted.
    #[error(transparent)]
    Http(Box<reqwest::Error>),
    /// A peer returned an unsuccessful HTTP status.
    #[error("Go subscription HTTP status {0}")]
    Status(u16),
    /// A peer returned a JSON-RPC error.
    #[error(transparent)]
    Protocol(Box<model::ErrorData>),
    /// A message was not valid JSON or a supported typed notification.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// A message or subscription filter exceeded a bound.
    #[error("Go subscription limit exceeded")]
    Limit,
    /// The combined opening deadline elapsed.
    #[error("Go subscription opening timed out")]
    Timeout,
    /// The peer closed without a completion message; refresh before reconnecting.
    #[error("Go subscription ended abruptly; refresh before reconnecting")]
    Abrupt,
    /// Configuration, acknowledgement, or stream framing was invalid.
    #[error("invalid Go subscription: {0}")]
    Invalid(&'static str),
}
impl From<reqwest::Error> for Error {
    fn from(error: reqwest::Error) -> Self {
        Self::Http(Box::new(error.without_url()))
    }
}
impl From<crate::transport::sse::SseError> for Error {
    fn from(error: crate::transport::sse::SseError) -> Self {
        match error {
            crate::transport::sse::SseError::Limit => Self::Limit,
            crate::transport::sse::SseError::Invalid(message) => Self::Invalid(message),
        }
    }
}

type ByteStream = Pin<Box<dyn Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Send>>;
struct Reader {
    stream: ByteStream,
    chunk: bytes::Bytes,
    offset: usize,
    decoder: Decoder,
}
impl Reader {
    async fn next(&mut self) -> Result<Value, Error> {
        loop {
            while self.offset < self.chunk.len() {
                let byte = self.chunk[self.offset];
                self.offset += 1;
                if let Some(event) = self.decoder.push(byte)? {
                    if !matches!(event.name.as_str(), "" | "message") {
                        return Err(Error::Invalid("unexpected SSE event name"));
                    }
                    return Ok(serde_json::from_str(&event.data)?);
                }
            }
            self.chunk = self.stream.next().await.ok_or(Error::Abrupt)??;
            self.offset = 0;
        }
    }
}

/// An owned Go subscription HTTP response with bounded event decoding.
/// No reader task, replay queue, or automatic reconnection is created. Dropping
/// this value or calling `close` drops the body and disconnects the server stream.
#[must_use]
pub struct Subscription {
    id: String,
    requested: model::SubscriptionFilter,
    task_ids: Vec<String>,
    reader: Option<Reader>,
}
impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GoSubscription")
            .field("closed", &self.reader.is_none())
            .finish_non_exhaustive()
    }
}
impl Subscription {
    /// Open a modern stateless Go HTTP subscription and verify its first frame.
    /// Resource URIs are translated to Go's top-level `uris`; requested list
    /// categories are forwarded. At most 1,000 URI entries of 4,096 bytes each
    /// are allowed. Go does not confirm which filters it accepted; this adapter
    /// locally excludes notifications outside the requested filter.
    ///
    /// # Errors
    /// Returns invalid configuration, size/deadline, HTTP, JSON-RPC rejection,
    /// or malformed acknowledgement errors. Non-streaming success is rejected.
    pub async fn open(
        client: reqwest::Client,
        config: Config,
        requested: model::SubscriptionFilter,
    ) -> Result<Self, Error> {
        Self::open_with_tasks(client, config, requested, std::iter::empty::<String>()).await
    }
    /// Open a subscription including modern Go task status notifications.
    /// Explicit IDs are appended to any IDs in `requested.task_ids`; both count
    /// toward the task limits. `open` also accepts the shared filter field.
    /// The caller must advertise the tasks extension in `Config::capabilities`
    /// (for example, `ClientCapabilities::builder().enable_tasks().build()`).
    /// Task IDs are opaque strings; at most 1,000 entries of 4,096 bytes each
    /// are allowed, independently of the resource limit. Duplicates count toward
    /// that limit. Iteration stops at the first excess entry. The encoded request
    /// also shares `Config::max_message_bytes` with the other filters.
    /// Read `tasks/get` snapshots after opening: Go supplies no initial task hint
    /// or accepted-filter acknowledgement. Native Rust servers use `Client::listen`.
    ///
    /// # Errors
    /// Returns the same errors as `open`, including invalid or excessive task IDs.
    pub async fn open_with_tasks(
        client: reqwest::Client,
        mut config: Config,
        requested: model::SubscriptionFilter,
        task_ids: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, Error> {
        let mut tasks = Vec::new();
        for task_id in requested
            .task_ids
            .iter()
            .flatten()
            .cloned()
            .chain(task_ids.into_iter().map(Into::into))
        {
            if tasks.len() == 1000 {
                return Err(Error::Limit);
            }
            if task_id.is_empty() || task_id.len() > 4096 {
                return Err(Error::Limit);
            }
            tasks.push(task_id);
        }
        if !matches!(config.endpoint.scheme(), "http" | "https")
            || config.endpoint.host_str().is_none()
            || !config.endpoint.username().is_empty()
            || config.endpoint.password().is_some()
            || config.endpoint.fragment().is_some()
            || config.connect_timeout.is_zero()
        {
            return Err(Error::Invalid(
                "HTTP(S) endpoint without credentials/fragment and nonzero deadline required",
            ));
        }
        let uris = requested
            .resource_subscriptions
            .as_deref()
            .unwrap_or_default();
        if uris.len() > 1000 || uris.iter().any(|uri| uri.is_empty() || uri.len() > 4096) {
            return Err(Error::Limit);
        }
        let mut notifications = serde_json::to_value(&requested)?;
        if !tasks.is_empty() {
            notifications["taskIds"] = json!(tasks);
        }
        let body = serde_json::to_vec(&json!({
            "jsonrpc":"2.0", "id":REQUEST_ID, "method":"subscriptions/listen",
            "params": {"notifications":notifications, "uris":uris, "_meta":{
                "io.modelcontextprotocol/protocolVersion":"2026-07-28",
                "io.modelcontextprotocol/clientInfo":config.client_info,
                "io.modelcontextprotocol/clientCapabilities":config.capabilities
            }}
        }))?;
        if body.len() > config.max_message_bytes.get() {
            return Err(Error::Limit);
        }
        // RequestBuilder::header appends; remove caller values so routing and
        // content negotiation cannot become ambiguous through duplicate headers.
        for name in [
            "content-type",
            "accept",
            "mcp-method",
            "mcp-protocol-version",
        ] {
            config.headers.remove(name);
        }
        let opening = async {
            let response = client
                .post(config.endpoint)
                .headers(config.headers)
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .header("mcp-method", "subscriptions/listen")
                .header("mcp-protocol-version", "2026-07-28")
                .body(body)
                .send()
                .await?;
            if response.status() != reqwest::StatusCode::OK {
                return Err(Error::Status(response.status().as_u16()));
            }
            let content_type = response
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .split(';')
                .next()
                .unwrap_or_default()
                .trim();
            if content_type == "application/json" {
                let mut stream = response.bytes_stream();
                let mut bytes = Vec::new();
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk?;
                    if chunk.len() > config.max_message_bytes.get().saturating_sub(bytes.len()) {
                        return Err(Error::Limit);
                    }
                    bytes.extend_from_slice(&chunk);
                }
                let value: Value = serde_json::from_slice(&bytes)?;
                validate_response(&value)?;
                return Err(Error::Invalid("successful listen requires an SSE response"));
            }
            if content_type != "text/event-stream" {
                return Err(Error::Invalid("SSE Content-Type required"));
            }
            let mut reader = Reader {
                stream: Box::pin(response.bytes_stream()),
                chunk: Default::default(),
                offset: 0,
                decoder: Decoder::new(config.max_message_bytes.get()),
            };
            let ack = reader.next().await?;
            validate_response(&ack)?;
            let id = ack
                .pointer("/result/subscriptionId")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty() && id.len() <= 4096)
                .ok_or(Error::Invalid("nonempty generated subscriptionId required"))?
                .to_owned();
            Ok(Self {
                id,
                requested,
                task_ids: tasks,
                reader: Some(reader),
            })
        };
        tokio::time::timeout(config.connect_timeout, opening)
            .await
            .map_err(|_| Error::Timeout)?
    }
    /// The generated Go ID, retained without rewriting its string contents.
    pub fn id(&self) -> &str {
        &self.id
    }
    /// The requested filter. Go has not acknowledged acceptance of this filter.
    pub fn requested_filter(&self) -> &model::SubscriptionFilter {
        &self.requested
    }
    /// Requested opaque task IDs; Go has not confirmed their acceptance.
    pub fn requested_task_ids(&self) -> &[String] {
        &self.task_ids
    }
    /// Drop the response body. Remote disconnect cleanup is asynchronous.
    pub fn close(&mut self) {
        self.reader = None;
    }
    /// Read the next requested notification, retaining the Go subscription metadata.
    /// This wait is cancellation safe; hosts can bound idle waits with a timeout.
    /// Filtered-out notifications are skipped. EOF or a protocol/framing failure
    /// closes the stream and returns one error; subsequent calls return `Ok(None)`.
    /// # Errors
    /// Returns malformed/cross-stream notifications, bounded framing failures,
    /// HTTP read errors, or abrupt stream termination.
    pub async fn next(&mut self) -> Result<Option<model::ServerNotification>, Error> {
        loop {
            let Some(reader) = self.reader.as_mut() else {
                return Ok(None);
            };
            let result = async {
                let value = reader.next().await?;
                if value.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
                    || value.get("id").is_some()
                    || value.get("result").is_some()
                    || value.get("error").is_some()
                    || value
                        .pointer("/params/_meta")
                        .and_then(|meta| meta.get(META_ID))
                        .and_then(Value::as_str)
                        != Some(self.id.as_str())
                {
                    return Err(Error::Invalid(
                        "notification must carry the generated subscription ID",
                    ));
                }
                let method = value
                    .get("method")
                    .and_then(Value::as_str)
                    .ok_or(Error::Invalid("notification method required"))?;
                let wanted = match method {
                    "notifications/tools/list_changed" => {
                        self.requested.tools_list_changed == Some(true)
                    }
                    "notifications/resources/list_changed" => {
                        self.requested.resources_list_changed == Some(true)
                    }
                    "notifications/prompts/list_changed" => {
                        self.requested.prompts_list_changed == Some(true)
                    }
                    "notifications/resources/updated" => {
                        let uri = value
                            .pointer("/params/uri")
                            .and_then(Value::as_str)
                            .ok_or(Error::Invalid("resource update URI required"))?;
                        self.requested
                            .resource_subscriptions
                            .as_ref()
                            .is_some_and(|uris| uris.iter().any(|requested| requested == uri))
                    }
                    "notifications/tasks" => {
                        // Decode explicitly: the SDK union's custom-notification fallback
                        // must not hide malformed known task messages.
                        let id = value
                            .pointer("/params/taskId")
                            .and_then(Value::as_str)
                            .filter(|id| !id.is_empty())
                            .ok_or(Error::Invalid("task ID required"))?;
                        let wanted = self.task_ids.iter().any(|requested| requested == id);
                        let notification: model::TaskStatusNotification =
                            serde_json::from_value(value)?;
                        return Ok(wanted.then_some(
                            model::ServerNotification::TaskStatusNotification(notification),
                        ));
                    }
                    _ => return Err(Error::Invalid("unsupported notification method")),
                };
                let notification = serde_json::from_value(value)?;
                Ok(wanted.then_some(notification))
            }
            .await;
            match result {
                Ok(Some(notification)) => return Ok(Some(notification)),
                Ok(None) => {}
                Err(error) => {
                    self.close();
                    return Err(error);
                }
            }
        }
    }
}
fn validate_response(value: &Value) -> Result<(), Error> {
    if value.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || value.get("id").and_then(Value::as_str) != Some(REQUEST_ID)
        || value.get("method").is_some()
        || value.get("result").is_some() == value.get("error").is_some()
    {
        return Err(Error::Invalid(
            "listen response envelope or request ID differs",
        ));
    }
    if let Some(error) = value.get("error") {
        return Err(Error::Protocol(Box::new(serde_json::from_value(
            error.clone(),
        )?)));
    }
    Ok(())
}
