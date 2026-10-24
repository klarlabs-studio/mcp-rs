# Upstream provenance

- Framework: https://github.com/klarlabs-studio/mcp-go
- Inspected commit: `52fa5746759d3284a6950e2667915d05ae2c6682`
- Snapshot inspected: 2026-10-17
- License: MIT, Copyright (c) 2025 Felix Geelhaar; notice preserved in LICENSE.

This independent Rust implementation ports modern application concepts using
Rust generics, Serde, Schemars, and the repository-owned protocol engine. Scope is MCP
2026-07-28 with discovery only. Older revisions and compatibility shims are
intentionally excluded, regardless of upstream or SDK availability.

`tests/build_go_reference.py` requires the exact clean checkout above. Reference
fixtures use actual upstream handlers/transports and modern request metadata.
`tests/differential.py` compares selected semantic projections, not whole messages.

`proto/mcp.proto` is copied verbatim from upstream `transport/grpc/mcp.proto`
under MIT. `src/transport/grpc_proto.rs` is generated with tonic-prost-build
0.14.6. Normal builds require no protoc. Transport envelope types are retained
for modern WebSocket/gRPC interoperability, without older protocol negotiation.

Modern subscriptions follow pinned `server/subscriptions.go` and
`server/resource_subscriptions.go`. Go and SDK negotiation shapes differ;
`tests/subscription_wire.py` preserves that distinction. The optional modern Go
HTTP subscription client follows its `uris` filter and generated-ID stream.
Negotiation and actual transport notification publication have separate checks.

Request-aware visibility follows pinned `WithToolFilter`, `WithResourceFilter`,
and `WithPromptFilter`, with additional Rust completion/subscription admission
checks and panic containment. Metadata builders follow `server/server.go` title,
description, website, icons, and instructions. Go buildInfo and modern icon-size
encoding remain explicit gaps in `docs/compatibility.md`.

## Protocol engine provenance

`crates/mcp-protocol` is a source fork of `rmcp` 3.5.1 from
https://github.com/modelcontextprotocol/rust-sdk at commit
`79437f291b2c44053d00dcd5db969fd0cca7c887`, `crates/rmcp/src`.
The original authors retain their copyrights. The exact upstream license,
including its Apache-2.0/MIT transition and documentation attribution terms, is
preserved in `crates/mcp-protocol/UPSTREAM-LICENSE`.

This is an independently maintained source fork, not a clean-room rewrite or
a registry SDK wrapper. All protocol implementation source is in this repository.
There is no runtime or development dependency on `rmcp` or `rmcp-macros`.
Changed Rust files carry a modification notice. Changes include modern-only
lifecycle policy, repository-owned names and feature selection. Upstream build
hooks, integration tests, proc macros and unused authentication/platform adapters
are not imported. Selected upstream unit tests are retained alongside the mcp-rs
regression suite and new modern-lifecycle tests.

The framework's own code remains MIT licensed. Original protocol documentation
retains upstream attribution and licensing; the source fork does not change those
terms. No endorsement by the upstream project is implied.
