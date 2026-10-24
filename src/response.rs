//! Typed application results passed through middleware.
use crate::model::{
    CallToolResponse, CallToolResult, CompleteResult, GetPromptResponse, GetPromptResult,
    ReadResourceResponse, ReadResourceResult,
};

/// The result of executing a registered application handler.
///
/// Middleware can inspect results without serializing them or erasing their type.
/// It must preserve the variant corresponding to the requested method.
#[derive(Debug)]
#[non_exhaustive]
pub enum Response {
    /// Policy admission for a modern subscription, before registering its watcher.
    SubscriptionAdmission,
    /// A tool's content and optional structured output.
    Tool(CallToolResult),
    /// The contents of a resource.
    Resource(ReadResourceResult),
    /// A native advanced tool response, such as an MRTR request or task handle.
    Protocol(Box<CallToolResponse>),
    /// A native intermediate resource response passed through middleware.
    ResourceProtocol(Box<ReadResourceResponse>),
    /// A native intermediate prompt response passed through middleware.
    PromptProtocol(Box<GetPromptResponse>),
    /// Parameter completion values.
    Completion(CompleteResult),
    /// A prompt's messages.
    Prompt(GetPromptResult),
}
