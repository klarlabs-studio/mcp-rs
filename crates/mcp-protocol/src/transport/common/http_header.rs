// Derived from rmcp 3.5.1; maintained and modified by mcp-rs. See ../../../UPSTREAM.md.
pub const HEADER_SESSION_ID: &str = "Mcp-Session-Id";
pub const HEADER_LAST_EVENT_ID: &str = "Last-Event-Id";
pub const HEADER_MCP_PROTOCOL_VERSION: &str = "MCP-Protocol-Version";
pub const EVENT_STREAM_MIME_TYPE: &str = "text/event-stream";
pub const JSON_MIME_TYPE: &str = "application/json";

// SEP-2243 standard headers, gated on protocol version >= 2026-07-28.
pub const HEADER_MCP_METHOD: &str = "Mcp-Method";
pub const HEADER_MCP_NAME: &str = "Mcp-Name";
pub const HEADER_MCP_PARAM_PREFIX: &str = "Mcp-Param-";

/// Sentinel wrapping a Base64-encoded SEP-2243 header value (`=?base64?<b64>?=`).
pub const BASE64_HEADER_PREFIX: &str = "=?base64?";
pub const BASE64_HEADER_SUFFIX: &str = "?=";

/// Reserved headers that must not be overridden by user-supplied custom headers.
/// `MCP-Protocol-Version` is in this list but is allowed through because the worker
/// injects it after initialization.
#[allow(dead_code)]
pub(crate) const RESERVED_HEADERS: &[&str] = &[
    "accept",
    HEADER_SESSION_ID,
    HEADER_MCP_PROTOCOL_VERSION, // allowed through by validate_custom_header; worker injects it post-init
    HEADER_LAST_EVENT_ID,
];

/// Checks whether a custom header name is allowed.
/// Returns `Ok(())` if allowed, `Err(name)` if rejected as reserved.
/// `MCP-Protocol-Version` is reserved but allowed through (the worker injects it post-init).
#[cfg(feature = "client-side-sse")]
pub(crate) fn validate_custom_header(name: &http::HeaderName) -> Result<(), String> {
    if RESERVED_HEADERS
        .iter()
        .any(|&r| name.as_str().eq_ignore_ascii_case(r))
    {
        if name
            .as_str()
            .eq_ignore_ascii_case(HEADER_MCP_PROTOCOL_VERSION)
        {
            return Ok(());
        }
        return Err(name.to_string());
    }
    Ok(())
}

/// Extracts the `scope=` parameter from a `WWW-Authenticate` header value.
/// Handles both quoted (`scope="files:read files:write"`) and unquoted (`scope=read:data`) forms.
#[cfg(feature = "client-side-sse")]
pub(crate) fn extract_scope_from_header(header: &str) -> Option<String> {
    let header_lowercase = header.to_ascii_lowercase();
    let scope_key = "scope=";

    if let Some(pos) = header_lowercase.find(scope_key) {
        let start = pos + scope_key.len();
        let value_slice = &header[start..];

        if let Some(stripped) = value_slice.strip_prefix('"') {
            if let Some(end_quote) = stripped.find('"') {
                return Some(stripped[..end_quote].to_string());
            }
        } else {
            let end = value_slice
                .find(|c: char| c == ',' || c == ';' || c.is_whitespace())
                .unwrap_or(value_slice.len());
            if end > 0 {
                return Some(value_slice[..end].to_string());
            }
        }
    }

    None
}
