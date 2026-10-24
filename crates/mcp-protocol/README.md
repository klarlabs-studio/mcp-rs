# Klarlabs MCP protocol engine

The repository-owned modern MCP protocol engine used by
[klarlabs-studio/mcp-rs](https://github.com/klarlabs-studio/mcp-rs).

Package: `klarlabs-mcp-protocol`. Rust import: `mcp_protocol`.
For application development, prefer the `klarlabs-mcp` framework package, whose
Rust import remains `mcp_rs`. It re-exports this engine through `mcp_rs::protocol`.

MCP 2026-07-28 discovery only. Legacy protocol negotiation is not supported.

This is an attributed source fork of rmcp 3.5.1, maintained independently;
it does not depend on rmcp or rmcp-macros. The original authors retain their
copyrights. See [provenance](UPSTREAM.md) and the exact upstream license,
including its documentation terms, in [UPSTREAM-LICENSE](UPSTREAM-LICENSE).
