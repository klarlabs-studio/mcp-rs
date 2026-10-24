// Derived from rmcp 3.5.1; maintained and modified by mcp-rs. See ../../../UPSTREAM.md.
//! Multi Round-Trip Request (MRTR) types for SEP-2322.
//!
//! Provides [`InputRequiredResult`], [`InputRequests`], and [`InputResponses`]
//! for the stateless multi round-trip request pattern defined in the MCP spec.
//! [`ResultType`] lives in the parent [`super`] module alongside other base result types.
//!
//! # Overview
//!
//! A server may respond to `tools/call`, `prompts/get`, or `resources/read` with an
//! [`InputRequiredResult`] instead of the normal result. The client fulfills the
//! [`InputRequests`], then retries the original request with [`InputResponses`] and
//! the echoed `requestState`.
//!
//! # Using MRTR
//!
//! **Server:** return an [`InputRequiredResult`] from a tool/prompt/resource
//! handler via the matching outcome enum ([`CallToolResponse`],
//! [`GetPromptResponse`], [`ReadResourceResponse`]). The SDK only lets an
//! `InputRequiredResult` reach a peer that negotiated protocol version
//! `2026-07-28` or newer; older peers get a protocol error instead.
//!
//! **Client:** the high-level `RunningService` helpers — `call_tool`,
//! `get_prompt`, and `read_resource` — automatically fulfil each
//! [`InputRequest`] through the local `ClientHandler` and retry, up to
//! [`DEFAULT_MRTR_MAX_ROUNDS`]. Use the `*_once` variants (e.g.
//! `call_tool_once`) to receive an [`InputRequiredResult`] directly and drive
//! the rounds yourself.
//!
//! # `requestState` is untrusted
//!
//! The client echoes `requestState` back verbatim, so a stateless server that
//! stores meaningful data in it MUST verify integrity before trusting the echoed
//! value. Enable the `request-state` feature and use `RequestStateCodec` to seal
//! and open it, or keep the state server-side and use `requestState` only as an
//! opaque handle.
//!
//! A complete runnable walkthrough lives in the `servers_mrtr` example.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    CallToolResult, CreateMessageRequest, CreateTaskResult, ElicitRequest, GetPromptResult,
    ListRootsRequest, MetaObject, ReadResourceResult, ResultType, ServerResult,
};

/// Default maximum number of MRTR rounds a high-level client call will drive.
///
/// This matches the default used by other Tier 1 SDKs and prevents a
/// misbehaving peer from keeping a request alive indefinitely.
pub const DEFAULT_MRTR_MAX_ROUNDS: usize = 10;

/// A server-initiated request that can appear inside [`InputRequests`].
///
/// Per the MCP spec, only `CreateMessageRequest` (sampling),
/// `ElicitRequest` (elicitation), and `ListRootsRequest` (roots)
/// are allowed. This is modeled as an untagged enum rather than a
/// `ServerRequest` alias to prevent `PingRequest` or `CustomRequest` from
/// being included.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[non_exhaustive]
pub enum InputRequest {
    /// A `sampling/createMessage` request.
    CreateMessage(CreateMessageRequest),
    /// An `elicitation/create` request.
    Elicitation(ElicitRequest),
    /// A `roots/list` request.
    ListRoots(ListRootsRequest),
}

// Wire-level equality: two `InputRequest`s are equal if they serialize to the
// same JSON. The wrapped request envelopes do not implement `PartialEq`
// structurally (they carry `Extensions`).
impl PartialEq for InputRequest {
    fn eq(&self, other: &Self) -> bool {
        match (serde_json::to_value(self), serde_json::to_value(other)) {
            (Ok(a), Ok(b)) => a == b,
            _ => false,
        }
    }
}

/// A map of server-initiated requests that the client must fulfill.
///
/// Keys are server-assigned string identifiers; values are request objects
/// (`ElicitRequest`, `CreateMessageRequest`, or `ListRootsRequest`).
pub type InputRequests = BTreeMap<String, InputRequest>;

/// A map of client responses to server-initiated requests.
///
/// Keys correspond to the keys in the [`InputRequests`] map; values are the
/// client's result for each request (`ElicitResult`, `CreateMessageResult`,
/// or `ListRootsResult`), represented as opaque JSON because the
/// heterogeneous `ClientResult` union does not derive the traits required
/// for use as a `BTreeMap` value.
pub type InputResponses = BTreeMap<String, Value>;

/// Result of a `tools/call` request, including the MRTR intermediate result.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum CallToolResponse {
    /// The server completed the tool call.
    Complete(CallToolResult),
    /// The server requires client-side input before the tool call can complete.
    InputRequired(InputRequiredResult),
    /// The server materialized a task for this call (SEP-2663 Tasks extension,
    /// `resultType: "task"`). The client polls `tasks/get` for the result.
    Task(CreateTaskResult),
}

impl From<CallToolResult> for CallToolResponse {
    fn from(result: CallToolResult) -> Self {
        Self::Complete(result)
    }
}

impl From<InputRequiredResult> for CallToolResponse {
    fn from(result: InputRequiredResult) -> Self {
        Self::InputRequired(result)
    }
}

impl From<CallToolResponse> for ServerResult {
    fn from(response: CallToolResponse) -> Self {
        match response {
            CallToolResponse::Complete(result) => ServerResult::CallToolResult(result),
            CallToolResponse::InputRequired(result) => ServerResult::InputRequiredResult(result),
            CallToolResponse::Task(result) => ServerResult::CreateTaskResult(result),
        }
    }
}

impl From<CreateTaskResult> for CallToolResponse {
    fn from(result: CreateTaskResult) -> Self {
        Self::Task(result)
    }
}

/// Result of a `prompts/get` request, including the MRTR intermediate result.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum GetPromptResponse {
    /// The server completed the prompt request.
    Complete(GetPromptResult),
    /// The server requires client-side input before the prompt can be returned.
    InputRequired(InputRequiredResult),
}

impl From<GetPromptResult> for GetPromptResponse {
    fn from(result: GetPromptResult) -> Self {
        Self::Complete(result)
    }
}

impl From<InputRequiredResult> for GetPromptResponse {
    fn from(result: InputRequiredResult) -> Self {
        Self::InputRequired(result)
    }
}

impl From<GetPromptResponse> for ServerResult {
    fn from(response: GetPromptResponse) -> Self {
        match response {
            GetPromptResponse::Complete(result) => ServerResult::GetPromptResult(result),
            GetPromptResponse::InputRequired(result) => ServerResult::InputRequiredResult(result),
        }
    }
}

/// Result of a `resources/read` request, including the MRTR intermediate result.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ReadResourceResponse {
    /// The server completed the resource read.
    Complete(ReadResourceResult),
    /// The server requires client-side input before the resource can be returned.
    InputRequired(InputRequiredResult),
}

impl From<ReadResourceResult> for ReadResourceResponse {
    fn from(result: ReadResourceResult) -> Self {
        Self::Complete(result)
    }
}

impl From<InputRequiredResult> for ReadResourceResponse {
    fn from(result: InputRequiredResult) -> Self {
        Self::InputRequired(result)
    }
}

impl From<ReadResourceResponse> for ServerResult {
    fn from(response: ReadResourceResponse) -> Self {
        match response {
            ReadResourceResponse::Complete(result) => ServerResult::ReadResourceResult(result),
            ReadResourceResponse::InputRequired(result) => {
                ServerResult::InputRequiredResult(result)
            }
        }
    }
}

/// A result indicating that additional input is needed before the request
/// can be completed.
///
/// At least one of [`input_requests`](Self::input_requests) or
/// [`request_state`](Self::request_state) MUST be present.
///
/// Servers MAY send this in response to `tools/call`, `prompts/get`, or
/// `resources/read`. Servers MUST NOT send this for any other request.
///
/// # Examples
///
/// ```
/// use mcp_protocol::model::InputRequiredResult;
///
/// let result = InputRequiredResult::from_request_state("opaque-server-state");
/// assert!(result.input_requests.is_none());
/// assert_eq!(result.request_state.as_deref(), Some("opaque-server-state"));
/// ```
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[non_exhaustive]
pub struct InputRequiredResult {
    /// Always `"input_required"` for this result type.
    pub result_type: ResultType,

    /// Server-initiated requests that the client must fulfill before retrying.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_requests: Option<InputRequests>,

    /// Opaque request state to be echoed back by the client on retry.
    /// Clients MUST NOT inspect, parse, modify, or make any assumptions
    /// about the contents.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_state: Option<String>,

    /// Optional protocol-level metadata.
    #[serde(rename = "_meta", skip_serializing_if = "Option::is_none")]
    pub meta: Option<MetaObject>,
}

/// Custom deserializer that requires `resultType: "input_required"` to prevent
/// greedy matching in the untagged `ServerResult` enum (which would otherwise
/// swallow empty objects or unknown shapes).
impl<'de> Deserialize<'de> for InputRequiredResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Helper {
            result_type: Option<ResultType>,
            input_requests: Option<InputRequests>,
            request_state: Option<String>,
            #[serde(rename = "_meta")]
            meta: Option<MetaObject>,
        }

        let helper = Helper::deserialize(deserializer)?;

        match &helper.result_type {
            Some(rt) if rt.is_input_required() => {}
            _ => {
                return Err(serde::de::Error::custom(
                    "InputRequiredResult requires resultType to be \"input_required\"",
                ));
            }
        }

        if helper.input_requests.is_none() && helper.request_state.is_none() {
            return Err(serde::de::Error::custom(
                "InputRequiredResult requires at least one of inputRequests or requestState",
            ));
        }

        Ok(InputRequiredResult {
            result_type: ResultType::INPUT_REQUIRED,
            input_requests: helper.input_requests,
            request_state: helper.request_state,
            meta: helper.meta,
        })
    }
}

impl InputRequiredResult {
    /// Creates a new `InputRequiredResult` with both input requests and request state.
    pub fn new(input_requests: Option<InputRequests>, request_state: Option<String>) -> Self {
        Self {
            result_type: ResultType::INPUT_REQUIRED,
            input_requests,
            request_state,
            meta: None,
        }
    }

    /// Creates from input requests only.
    pub fn from_input_requests(input_requests: InputRequests) -> Self {
        Self::new(Some(input_requests), None)
    }

    /// Creates from request state only (e.g. for load shedding).
    pub fn from_request_state(request_state: impl Into<String>) -> Self {
        Self::new(None, Some(request_state.into()))
    }

    /// Sets optional metadata.
    pub fn with_meta(mut self, meta: MetaObject) -> Self {
        self.meta = Some(meta);
        self
    }
}
