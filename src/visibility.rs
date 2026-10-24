//! Request-aware discovery and access policy.
use crate::Context;
use mcp_protocol::ErrorData;
use std::sync::Arc;

/// The registered object being listed, accessed, completed, or subscribed to.
/// Context metadata supplied by a client is untrusted; host authentication should
/// place verified identity in request extensions before dispatching to the server.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum VisibilityTarget<'a> {
    /// A tool name, shared by listing and invocation.
    Tool {
        /// Registered tool name.
        name: &'a str,
    },
    /// A static resource or a URI-template resource.
    Resource {
        /// Concrete URI on reads/subscriptions; registered URI on lists/completion references.
        uri: &'a str,
        /// Registered static URI or URI template. Exact matches retain priority.
        registered_uri: &'a str,
        /// Human-readable registered resource name.
        name: &'a str,
    },
    /// A prompt name, shared by listing, retrieval, and completion.
    Prompt {
        /// Registered prompt name.
        name: &'a str,
    },
}
/// Shared synchronous policy. Return false to hide an object and deny access.
/// Predicates must be fast, nonblocking, and free of side effects; they can run
/// multiple times per request. Panics fail closed with an opaque protocol error.
pub type VisibilityPolicy = Arc<dyn Fn(&Context, VisibilityTarget<'_>) -> bool + Send + Sync>;

pub(crate) fn allows(
    policy: Option<&VisibilityPolicy>,
    context: &Context,
    target: VisibilityTarget<'_>,
) -> Result<bool, ErrorData> {
    let Some(policy) = policy else {
        return Ok(true);
    };
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| policy(context, target)))
        .map_err(|_| ErrorData::internal_error("Visibility policy panicked", None))
}
