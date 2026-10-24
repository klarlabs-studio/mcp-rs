# First-release acceptance contract

Target: the independent `mcp-rs` 0.1 Rust framework, pinned to the modern
MCP 2026-07-28 surface in [the roadmap](port-roadmap.md). This contract is finite:
passing it establishes the supported release scope, not exhaustive protocol,
experimental extension, production load or PKI certification.

## Required implementation and evidence

| Gate | Evidence |
|---|---|
| Independent modern engine; discovery only; no registry SDK | Resolved Cargo metadata, owned engine source and `modern_only` negative tests |
| Idiomatic typed server/client APIs and valid inputs/outputs | Framework, core parity, schema, visibility, metadata and client suites |
| Native tasks, MRTR and subscriptions | Task lifecycle/input/retention/concurrency tests, MRTR budget/cancellation tests, scoped subscription tests |
| Routing, caching and progressive discovery | Routing/schema/header/cache suites, bounded pagination, filtered schema detail and concurrent page collection |
| Skills, server cards and webhook helpers | Bundle/directory/archive/template/card/webhook suites and pinned Go fixtures |
| Every native transport family | One shared workflow over framed JSON streams, HTTP, WebSocket and gRPC: concurrent filtered pagination, MRTR, cache invalidation, task status streams, live replacement and graceful completion |
| Encrypted cross-language transports | 36 loopback TLS/mTLS cases across WebSocket/gRPC and both Go/Rust directions; valid calls after trust, hostname and client-certificate rejections |
| Rust quality and supported feature combinations | Formatting, Clippy warnings denied, documentation warnings denied, Rust 1.89 checks, 18 framework and four protocol feature configurations |
| Runtime and differential gate | Workspace tests, all examples, stdio/HTTP smoke tests, typed stdio clients and all 18 pinned Go fixture runners |
| Intentional differences stay visible | Explicit assertions and explanations in [compatibility.md](compatibility.md), rather than silently normalizing them |

## Reproduce

The Go reference is a test dependency only. Build it from a clean checkout of
`52fa5746759d3284a6950e2667915d05ae2c6682`; the builder rejects other revisions:

```sh
python3 tests/build_go_reference.py --upstream /path/to/pinned/mcp-go --output /tmp/mcp-go-reference
python3 tests/conformance.py --offline --go-reference /tmp/mcp-go-reference
```

Omit `--offline` if the locked Rust dependencies are not cached. Install stable
Rust with Clippy/rustfmt, Rust 1.89.0, Python 3 and Go compatible with the pinned
reference. The TLS fixtures are checked-in loopback test identities. No paid API,
external service or certificate download is used by the acceptance gate.

The runner writes `target/conformance/report.json` and individual logs. It stops
on failure, records skipped groups, rejects source changes during a run and hashes
implementation/configuration/fixtures. A group can be run independently with
`--only quality`, `--only features`, `--only tests` or `--only interop`; a partial
run does not establish the full contract. Use `--jobs` to control resource use.
Interop-only runs require all-feature examples to have been built first.

## Supported differences and ownership

The release preserves Rust's native MRTR/subscription envelopes, task retention
and error semantics, schema generation and metadata encoding. Optional Go HTTP
adapters cover the documented pinned Go contracts. Reverse adapters and a claim
of identical Go/Rust wire output are outside this release. These differences are
not unresolved implementation promises.

Legacy revisions, initialize fallback, split HTTP/SSE, retired tasks, roots and
sampling are excluded. Hosts own authentication policy, application identity,
MCP Apps rendering, persistence/event replay, durable webhook scheduling,
distributed limits and observability exporters. Production proxy/load behavior,
certificate rotation/revocation and exhaustive experimental schemas are separate
deployment qualifications, not silently claimed by loopback tests.

## Local acceptance result

The final unchanged-source run on 2026-10-24 passed all 51 acceptance checks,
including 412 workspace tests across 46 suites, all 22 feature configurations,
and 36 TLS/mTLS interoperability cases. No tests were ignored. See the durable
[acceptance-report.json](acceptance-report.json) snapshot and
[validation.md](validation.md) for the run and regression details. This establishes
local completion of the supported implementation contract above.

## Distribution boundary

Local acceptance and hosted CI are separate evidence. The repository is
[klarlabs-studio/mcp-rs](https://github.com/klarlabs-studio/mcp-rs). Current hosted
results are recorded in [Actions](https://github.com/klarlabs-studio/mcp-rs/actions).
CI runs the full pinned interoperability group and preserves its report/logs.
Protected main requires the aggregate check covering Rust quality, stable/MSRV,
feature configurations, package dry runs and pinned Go interoperability. A maintainer must review
hosted results before creating a release tag. Tag releases rerun the complete
acceptance gate before publishing a source archive and checksums on GitHub.
Registry package identities are `klarlabs-mcp` and `klarlabs-mcp-protocol`, both
restricted to crates.io. The release workflow publishes both packages in dependency
order after acceptance. A scoped `CRATES_IO_TOKEN` secret is required before
creating a tag; see [publishing](publishing.md). No credentials are required for
local development or CI package dry runs, and the acceptance gate itself never
publishes anything.
