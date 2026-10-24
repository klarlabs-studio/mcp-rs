//! Explicit modern HTTP prompt/resource MRTR for the pinned Go wire dialect.
//!
//! Go uses array input requests and reserved `_meta` retry fields. This adapter
//! drives that dialect directly; it is separate from the SDK `Client` and does
//! not enable initialize, legacy sessions, sampling, roots, or task execution.
use crate::{model::*, transport::sse::Decoder};
use futures_util::StreamExt;
use reqwest::{Url, header::HeaderMap};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    num::NonZeroUsize,
    time::Duration,
};

const ID: &str = "mcp-rs-go-mrtr";
const RESPONSES: &str = "io.modelcontextprotocol/inputResponses";
const STATE: &str = "io.modelcontextprotocol/requestState";

/// Configuration for one modern Go HTTP endpoint.
/// The supplied HTTP client owns TLS, authentication, redirects and pooling.
#[derive(Clone)]
pub struct Config {
    /// HTTP(S) POST URL without credentials or a fragment.
    pub endpoint: Url,
    /// Host headers. Protocol framing and routing headers are reserved.
    pub headers: HeaderMap,
    /// Client identity sent on every round.
    pub client_info: Implementation,
    /// Declared capabilities; explicitly enable the elicitation modes you handle.
    pub capabilities: ClientCapabilities,
    /// Maximum encoded request or total response bytes per round.
    pub max_message_bytes: NonZeroUsize,
    /// Maximum pending-input rounds, excluding the final completed response.
    pub max_input_rounds: NonZeroUsize,
    /// Deadline for the entire operation, including HTTP and input callbacks.
    pub timeout: Duration,
}
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("max_message_bytes", &self.max_message_bytes)
            .field("max_input_rounds", &self.max_input_rounds)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
impl Config {
    /// Default to 1 MiB per round, ten input rounds and a 30-second deadline.
    pub fn new(endpoint: Url) -> Self {
        Self {
            endpoint,
            headers: HeaderMap::new(),
            client_info: Implementation::new("mcp-rs", env!("CARGO_PKG_VERSION")),
            capabilities: ClientCapabilities::default(),
            max_message_bytes: NonZeroUsize::new(1 << 20).unwrap_or(NonZeroUsize::MIN),
            max_input_rounds: NonZeroUsize::new(10).unwrap_or(NonZeroUsize::MIN),
            timeout: Duration::from_secs(30),
        }
    }
}
/// A bounded HTTP, protocol, decoding or callback failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// HTTP failure, with endpoint URL data removed.
    #[error(transparent)]
    Http(Box<reqwest::Error>),
    /// Unsuccessful HTTP response.
    #[error("Go MRTR HTTP status {0}")]
    Status(u16),
    /// Peer JSON-RPC rejection or application input callback rejection.
    #[error(transparent)]
    Protocol(Box<ErrorData>),
    /// Malformed JSON or native result/input type.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// Encoded bytes or input rounds exceeded a configured bound.
    #[error("Go MRTR limit exceeded")]
    Limit,
    /// The whole-operation deadline elapsed.
    #[error("Go MRTR operation timed out")]
    Timeout,
    /// Invalid configuration, envelope, unsupported input, or replay sequence.
    #[error("invalid Go MRTR: {0}")]
    Invalid(&'static str),
}
impl From<reqwest::Error> for Error {
    fn from(error: reqwest::Error) -> Self {
        Self::Http(Box::new(error.without_url()))
    }
}

/// A reusable, opt-in Go MRTR HTTP adapter with request-local replay state.
/// No background work, reconnect loop, cache or legacy negotiation is created.
#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    config: Config,
}
impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GoMrtrClient")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}
impl Client {
    /// Validate endpoint, framing headers and a nonzero operation deadline.
    /// # Errors
    /// Returns invalid configuration without making a network request.
    pub fn new(http: reqwest::Client, config: Config) -> Result<Self, Error> {
        if !matches!(config.endpoint.scheme(), "http" | "https")
            || config.endpoint.host_str().is_none()
            || !config.endpoint.username().is_empty()
            || config.endpoint.password().is_some()
            || config.endpoint.fragment().is_some()
            || config.timeout.is_zero()
            || config.headers.keys().any(|key| {
                let name = key.as_str();
                name.starts_with("mcp-")
                    || matches!(
                        name,
                        "content-type" | "accept" | "content-length" | "transfer-encoding" | "host"
                    )
            })
        {
            return Err(Error::Invalid("endpoint, deadline or reserved headers"));
        }
        Ok(Self { http, config })
    }
    /// Read a resource, fulfilling form/URL inputs through the supplied callback.
    /// Previous answers are retained for Go's sequential handler replay. Dropping
    /// this future cancels HTTP and callback work; no background task survives it.
    /// # Errors
    /// Returns HTTP/protocol, unsupported input, framing, size, round or deadline errors.
    pub async fn read_resource<F, Fut>(
        &self,
        uri: impl Into<String>,
        fulfill: F,
    ) -> Result<ReadResourceResult, Error>
    where
        F: FnMut(ElicitRequestParams) -> Fut,
        Fut: Future<Output = Result<ElicitResult, ErrorData>>,
    {
        let uri = uri.into();
        self.execute("resources/read", &uri, json!({"uri":uri}), fulfill)
            .await
    }
    /// Get a prompt, fulfilling form/URL inputs through the supplied callback.
    /// # Errors
    /// Returns the same errors as [`Self::read_resource`].
    pub async fn get_prompt<F, Fut>(
        &self,
        name: impl Into<String>,
        arguments: Option<JsonObject>,
        fulfill: F,
    ) -> Result<GetPromptResult, Error>
    where
        F: FnMut(ElicitRequestParams) -> Fut,
        Fut: Future<Output = Result<ElicitResult, ErrorData>>,
    {
        let name = name.into();
        self.execute(
            "prompts/get",
            &name,
            json!({"name":name,"arguments":arguments.unwrap_or_default()}),
            fulfill,
        )
        .await
    }
    async fn execute<T, F, Fut>(
        &self,
        method: &str,
        name: &str,
        params: Value,
        mut fulfill: F,
    ) -> Result<T, Error>
    where
        T: DeserializeOwned,
        F: FnMut(ElicitRequestParams) -> Fut,
        Fut: Future<Output = Result<ElicitResult, ErrorData>>,
    {
        let operation = async {
            let mut answers = BTreeMap::<String, Value>::new();
            let mut state: Option<Value> = None;
            let caps = serde_json::to_value(&self.config.capabilities)?;
            for round in 0..=self.config.max_input_rounds.get() {
                let mut request = params.clone();
                let mut meta = json!({
                    "io.modelcontextprotocol/protocolVersion":"2026-07-28",
                    "io.modelcontextprotocol/clientInfo":self.config.client_info,
                    "io.modelcontextprotocol/clientCapabilities":caps,
                });
                if !answers.is_empty() {
                    meta[RESPONSES] = Value::Array(
                        answers
                            .iter()
                            .map(|(id, payload)| json!({"id":id,"payload":payload}))
                            .collect(),
                    );
                }
                if let Some(state) = &state {
                    meta[STATE] = state.clone();
                }
                request["_meta"] = meta;
                let result = self.send(method, name, request).await?;
                match result.get("resultType").and_then(Value::as_str) {
                    Some("complete") => return Ok(serde_json::from_value(result)?),
                    Some("input_required") => {}
                    _ => return Err(Error::Invalid("unsupported result type")),
                }
                if round == self.config.max_input_rounds.get() {
                    return Err(Error::Limit);
                }
                let inputs = result
                    .get("inputRequests")
                    .and_then(Value::as_array)
                    .filter(|inputs| !inputs.is_empty())
                    .ok_or(Error::Invalid("missing input requests"))?;
                // Validate the entire batch before invoking any host callback.
                let mut pending = Vec::new();
                let mut pending_ids = BTreeSet::new();
                for input in inputs {
                    let id = input
                        .get("id")
                        .and_then(Value::as_str)
                        .filter(|id| !id.is_empty())
                        .ok_or(Error::Invalid("missing input ID"))?;
                    if answers.contains_key(id) || !pending_ids.insert(id.to_owned()) {
                        return Err(Error::Invalid("duplicate or replayed input ID"));
                    }
                    if input.get("kind").and_then(Value::as_str) != Some("elicitation") {
                        return Err(Error::Invalid("unsupported input kind"));
                    }
                    let payload = input
                        .get("payload")
                        .ok_or(Error::Invalid("missing input payload"))?;
                    let mode = payload
                        .get("mode")
                        .and_then(Value::as_str)
                        .unwrap_or("form");
                    if !matches!(mode, "form" | "url")
                        || caps
                            .get("elicitation")
                            .and_then(|cap| cap.get(mode))
                            .is_none()
                    {
                        return Err(Error::Invalid("undeclared elicitation mode"));
                    }
                    let typed: ElicitRequestParams = serde_json::from_value(payload.clone())?;
                    pending.push((id.to_owned(), typed));
                }
                state = result.get("requestState").cloned();
                for (id, input) in pending {
                    let answer = fulfill(input)
                        .await
                        .map_err(|error| Error::Protocol(Box::new(error)))?;
                    answers.insert(id, serde_json::to_value(answer)?);
                }
            }
            Err(Error::Limit)
        };
        tokio::time::timeout(self.config.timeout, operation)
            .await
            .map_err(|_| Error::Timeout)?
    }
    async fn send(&self, method: &str, name: &str, params: Value) -> Result<Value, Error> {
        let body =
            serde_json::to_vec(&json!({"jsonrpc":"2.0","id":ID,"method":method,"params":params}))?;
        let limit = self.config.max_message_bytes.get();
        if body.len() > limit {
            return Err(Error::Limit);
        }
        let response = self
            .http
            .post(self.config.endpoint.clone())
            .headers(self.config.headers.clone())
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", "2026-07-28")
            .header("mcp-method", method)
            .header("mcp-name", name)
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
            .unwrap_or("")
            .split(';')
            .next()
            .unwrap_or("")
            .trim();
        let sse = match content_type {
            "application/json" => false,
            "text/event-stream" => true,
            _ => return Err(Error::Invalid("unsupported content type")),
        };
        let mut decoder = Decoder::new(limit);
        let mut bytes = Vec::new();
        let mut total = 0usize;
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            total = total
                .checked_add(chunk.len())
                .filter(|total| *total <= limit)
                .ok_or(Error::Limit)?;
            if sse {
                for byte in chunk {
                    if let Some(event) = decoder.push(byte).map_err(|error| match error {
                        crate::transport::sse::SseError::Limit => Error::Limit,
                        crate::transport::sse::SseError::Invalid(message) => {
                            Error::Invalid(message)
                        }
                    })? {
                        if !matches!(event.name.as_str(), "" | "message") {
                            return Err(Error::Invalid("unsupported SSE event"));
                        }
                        return decode(&event.data);
                    }
                }
            } else {
                bytes.extend_from_slice(&chunk);
            }
        }
        if sse {
            return Err(Error::Invalid("SSE ended before response"));
        }
        decode(std::str::from_utf8(&bytes).map_err(|_| Error::Invalid("JSON is not UTF-8"))?)
    }
}
fn decode(data: &str) -> Result<Value, Error> {
    let value: Value = serde_json::from_str(data)?;
    if value.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || value.get("id").and_then(Value::as_str) != Some(ID)
        || value.get("method").is_some()
        || value.get("result").is_some() == value.get("error").is_some()
    {
        return Err(Error::Invalid("response envelope or request ID differs"));
    }
    if let Some(error) = value.get("error") {
        return Err(Error::Protocol(Box::new(serde_json::from_value(
            error.clone(),
        )?)));
    }
    Ok(value["result"].clone())
}
