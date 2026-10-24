// Derived from rmcp 3.5.1; maintained and modified by mcp-rs. See ../../../UPSTREAM.md.
pub mod session;
#[cfg(all(feature = "transport-streamable-http-server", not(feature = "local")))]
pub mod tower;
pub use session::{RestoreOutcome, SessionId, SessionManager, SessionRestoreMarker};
#[cfg(all(feature = "transport-streamable-http-server", not(feature = "local")))]
pub use tower::{StreamableHttpServerConfig, StreamableHttpService};
