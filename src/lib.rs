//! Typed application framework ported from klarlabs-studio/mcp-go.
//!
//! The fluent API and its in-repository protocol engine form an independent MCP
//! implementation. The attributed engine source fork is maintained in this
//! workspace and supports only the 2026-07-28 discovery lifecycle. Optional
//! adapters provide WebSocket and gRPC framing. See UPSTREAM.md for provenance.
//!
//! ```
//! use mcp_rs::{Server, ToolError};
//! use schemars::JsonSchema;
//! use serde::Deserialize;
//!
//! #[derive(Deserialize, JsonSchema)]
//! #[serde(deny_unknown_fields)]
//! struct Hello { name: String }
//!
//! let mut server = Server::new("example", "0.1.0");
//! server.tool("hello").description("Say hello")
//!     .handler(|_ctx, input: Hello| async move {
//!         Ok::<_, ToolError>(format!("Hello {}", input.name))
//!     })?;
//! # Ok::<(), mcp_rs::RegistrationError>(())
//! ```
#![forbid(unsafe_code)]
#![deny(missing_docs)]

#[cfg(feature = "client")]
pub mod client;
pub mod discovery;
mod error;
pub mod live;
pub mod metadata;
pub mod middleware;
mod prompt;
mod resource;
mod response;
mod server;
pub mod server_card;
pub mod session;
pub mod skills;
pub mod subscriptions;
mod tool;
pub mod transport;
pub mod visibility;

#[cfg(feature = "client")]
pub use client::{Client, ClientError, MrtrLimits, TypedTool};
pub use error::RegistrationError;
pub use live::LiveServer;
pub use mcp_protocol as protocol;
pub use mcp_protocol::model;
pub use metadata::{BuildInfo, ServerMetadata};
pub use prompt::PromptBuilder;
pub use resource::{ResourceBuilder, UriParameters};
pub use response::Response;
pub use schemars;
pub use serde;
pub use serde_json;
pub use server::Server;
pub use session::Session;
pub use subscriptions::{
    DeliveryReport, SubscriptionConfig, SubscriptionError, SubscriptionHub, SubscriptionStats,
};
pub use tool::ToolBuilder;
pub use visibility::{VisibilityPolicy, VisibilityTarget};

use futures_util::future::BoxFuture;
use mcp_protocol::{ErrorData, RoleServer, service::RequestContext};
use serde_json::Value;
use std::sync::Arc;

/// Protocol request context, including cancellation, metadata, and peer access.
pub type Context = RequestContext<RoleServer>;
/// A typed handler result shared by protocol methods.
pub type HandlerResponse = Result<Response, ErrorData>;
/// The next application handler in a middleware chain.
pub type Next = Arc<dyn Fn(Context, Value) -> BoxFuture<'static, HandlerResponse> + Send + Sync>;
/// A thread-safe asynchronous wrapper around application handlers.
pub type Middleware =
    Arc<dyn Fn(Context, Value, Next) -> BoxFuture<'static, HandlerResponse> + Send + Sync>;
/// An intentional public message returned to the model as a tool execution error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct ToolError {
    message: String,
}
impl ToolError {
    /// Create an execution error whose message is safe to show to the caller.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}
