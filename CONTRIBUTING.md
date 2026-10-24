# Contributing

Support MCP 2026-07-28 through discovery only. Do not restore initialize fallback,
older revisions, retired task APIs, roots/sampling, or split HTTP/SSE. Keep
`tests/modern_only.rs` negative checks and modern Go interoperability fixtures.

Build idiomatic Rust equivalents of the upstream application concepts. Preserve
observable behavior where supported; record intentional gaps in the parity
roadmap. Avoid translating Go's reflection, nil values, or error strings into the
public Rust API.

## Design rules

- Model known outcomes with types and enums. Configuration errors implement
  `std::error::Error` and remain separate from client-visible protocol errors.
- Keep fields private; use documented constructors and builders. Mark unfinished
  builders `must_use`. Prefer compatible dependency requirements and a locked,
  tested dependency graph.
- Preserve ownership and cancellation semantics. Share immutable configurations
  with `Arc`; copy on configuration changes. Do not add locks around immutable
  registries or hold synchronous mutex guards across `.await`.
- Async handlers must yield. Move blocking I/O or CPU work off runtime threads;
  document that cancelling a future cannot stop already running blocking work.
- Await lifecycle cleanup. Avoid detached background tasks in transport wrappers
  and examples. Tokio stdin's blocking read requires a bounded owned-runtime
  teardown when EOF is unavailable; retain the cancellation regression test.
  Process stdout belongs exclusively to the stdio protocol.
- Do not use `unwrap`, `expect`, or intentional panics in library paths. Keep
  panic containment at the application callback boundary. Test assertions may
  unwrap values whose failure should fail the test.
- Unsafe code is forbidden in this crate. Keep transport dependencies optional,
  request only needed dependency features, and let applications own the runtime.
- Document public APIs, errors, and limitations. Include compiling examples and
  test observable protocol behavior, not private implementation details.

## Checks

```sh
cargo fmt --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo clippy --locked --lib --no-default-features -- -D warnings
cargo test --locked --all-features
cargo +1.89.0 check --locked --all-targets --all-features
cargo check --locked --lib --no-default-features
cargo check --locked --lib --no-default-features --features stdio
cargo check --locked --lib --no-default-features --features http
RUSTDOCFLAGS='-D warnings' cargo doc --locked --no-deps --all-features
cargo build --locked --all-features --examples
python3 tests/transport_smoke.py
```

The transport smoke test binds an ephemeral loopback port and checks graceful
Ctrl-C exit. CI tests stable and the minimum compiler, checks transport feature
combinations, and treats documentation and Clippy warnings as failures. Any
increase in the minimum compiler needs an explicit Cargo.toml and documentation
update backed by a build on that version.

## Go compatibility checks

Use a clean checkout of the commit in UPSTREAM.md. The reference builder refuses
any other revision. This explicit command may download Go dependencies:

```sh
python3 tests/build_go_reference.py --upstream /path/to/mcp-go --output /tmp/mcp-go-reference
python3 tests/differential.py --go-reference /tmp/mcp-go-reference
target/debug/examples/client_probe --stdio /tmp/mcp-go-reference
```

Add shared cases for observable compatibility changes. Do not silently normalize
wire differences; each permitted mismatch belongs in the fixture with a reason.
Keep the roadmap distinct from SDK-provided capabilities. A passing bounded
fixture is not certification of every extension or protocol revision.

Network transport checks (all features):

```sh
python3 tests/network_interop.py --go-reference /tmp/mcp-go-reference
```

`proto/mcp.proto` and generated bindings preserve the pinned upstream wire
contract. To intentionally regenerate bindings, install protoc and run
`python3 tests/generate_grpc_bindings.py`; regular crate builds must not require
protoc. Keep socket/TLS/authentication policy explicit, outbound queues bounded,
and shutdown awaited. Do not claim TLS or load certification from loopback tests.

Modern subscriptions use `SubscriptionHub` with bounded queues and registration
guards. SDK acknowledgment precedes admission; keep initial refresh hints so
changes during negotiation cannot leave a client stale. Do not hold hub locks
across awaits or silently drop updates for a still-open stream. Preserve the
explicit Go wire mismatch check (`tests/subscription_wire.py`) rather than
reporting it as cross-language subscription parity.

The optional `client-go-subscriptions` API is deliberately separate from native
SDK subscriptions. Verify it with `python3 tests/go_subscription_interop.py
--go-reference /tmp/mcp-go-reference` (one shell command). Keep application
negotiation evidence separate from transport fixture push evidence. Go provides
no accepted-filter acknowledgement or initial hints: do not synthesize those
promises. Keep generated IDs scoped and HTTP response bodies owned and bounded.

Visibility must filter before pagination and gate direct access before validation.
Keep hidden/missing errors equivalent within each framework path, exact-resource
priority intact, and post-middleware URI/reference rewrites checked. Policy
callbacks must fail closed on panic and run on request context before tool
middleware. Do not authenticate callers from self-reported metadata. Subscription
acknowledgements run without request context; keep their treatment of hidden and
unknown URI requests uniform. Existing grants require explicit hub revocation.
Use `python3 tests/visibility_interop.py --go-reference /tmp/mcp-go-reference` for
the selected actual pinned Go filter comparison.

Server metadata stays in the native SDK Implementation/instructions fields;
method capabilities remain derived from registrations. Preserve copy-on-write
and live discovery behavior. Keep the pinned Go icon/buildInfo/discovery placement
gaps visible in `tests/server_metadata_interop.py`; do not silently discard or
normalize them into a claim of complete metadata parity. The native identity
setter preserves caller values, while individual empty-value setters clear their
optional field. Metadata configuration must not fetch icon/website URIs.


## Complete acceptance gate

Use [the first-release contract](docs/release-readiness.md) and
`python3 tests/conformance.py --offline --go-reference /tmp/mcp-go-reference`
to run every local gate against one source fingerprint. This includes discovery,
webhook and TLS interoperability checks as well as the other pinned fixtures.
The Go reference is validation tooling only. Partial runs explicitly record their
skipped groups. Do not treat a locally passing gate as a hosted CI or publishing
result.

## GitHub workflow

Open changes against `main`. The required aggregate check includes Rust quality,
stable/MSRV tests, all configured feature builds and the pinned Go fixtures.
Issues, PR templates and security reporting follow the Klarlabs repository setup.
API documentation is built from Rust source and deployed through GitHub Pages.

## Releases

`.relicta.yaml` follows mcp-go's conventional versioning and changelog conventions.
AI release notes are disabled. Use `relicta plan --analyze` to inspect a release;
defaults keep execution as a dry run with Git pushing disabled and draft GitHub
releases. Review version changes for both crate manifests before creating a tag.
Do not run two independent GitHub release publishers for the same tag: the tag
workflow owns source release publication.

A matching `v<version>` tag runs the complete acceptance gate, then publishes a
GitHub source archive, its acceptance report and checksums. Neither crate is
automatically published to a registry; both retain `publish = false`.
