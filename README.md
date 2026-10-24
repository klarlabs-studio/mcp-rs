# mcp-rs

[![CI](https://github.com/klarlabs-studio/mcp-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/klarlabs-studio/mcp-rs/actions/workflows/ci.yml) · [API documentation](https://klarlabs-studio.github.io/mcp-rs/)

A Rust application-framework port of [klarlabs-studio/mcp-go](https://github.com/klarlabs-studio/mcp-go).

An independent Rust implementation with a typed fluent server API and an
in-repository protocol engine in `crates/mcp-protocol`. There is no dependency
on the `rmcp` or `rmcp-macros` packages. The engine is an attributed source fork
of rmcp 3.5.1, maintained here; see [provenance and licensing](UPSTREAM.md).
This is a tested core parity milestone, **not full mcp-go feature parity**.

**Protocol policy: MCP 2026-07-28 only, using `server/discover`.**
The protocol engine rejects `initialize`; clients never fall back to it.
Legacy task APIs, split HTTP/SSE, roots, and sampling are outside this port.
Some inherited model types remain for decoding and rejecting retired messages;
they do not imply support for those protocols.

`mcp-protocol` owns JSON-RPC, discovery, lifecycle, task management, and the
stdio/HTTP transports. Optional WebSocket and gRPC adapters use the same engine.
Advanced applications access it through `mcp_rs::protocol` and `mcp_rs::model`.

```rust
use mcp_rs::{Server, ToolError, schemars::JsonSchema};
use serde::Deserialize;

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Hello { name: String }

let mut server = Server::new("example", "0.1.0");
server.tool("hello")
    .description("Say hello")
    .handler(|_ctx, input: Hello| async move {
        Ok::<_, ToolError>(format!("Hello {}", input.name))
    })?;
Ok::<(), mcp_rs::RegistrationError>(())
```

## Run

```sh
cargo run --locked --example basic           # stdio: stdout is protocol only
cargo run --locked --example basic -- --http # http://127.0.0.1:8080/mcp
cargo test --locked --workspace --all-features
cargo build --locked --all-features --examples
python3 tests/transport_smoke.py
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
```

The repository is [klarlabs-studio/mcp-rs](https://github.com/klarlabs-studio/mcp-rs).
The crates.io package names are `klarlabs-mcp` and `klarlabs-mcp-protocol`;
Rust imports remain `mcp_rs` and `mcp_protocol`. For a published release:

```toml
[dependencies]
mcp-rs = { package = "klarlabs-mcp", version = "0.1" }
serde = { version = "1", features = ["derive"] }
schemars = "1"
```

Check [crates.io](https://crates.io/crates/klarlabs-mcp) for publication status.
For development, clone the repository and use a local path dependency:

```toml
[dependencies]
mcp-rs = { path = "../mcp-rs" }
serde = { version = "1", features = ["derive"] }
schemars = "1"
```

## Application API

- `server.tool(name).description(...).handler(...)`: async typed input, serializable
  output, generated JSON Schema. Compiled JSON Schema validation and Serde decoding run before the handler.
  String outputs become text; objects populate `structuredContent` when an output
  schema is declared with `.output_schema::<Output>()?`. Use `.rich_handler` for
  native content blocks and `.protocol_handler` for advanced protocol engine responses.
- `server.resource(uri).name(...).mime_type(...).handler(...)`: static and named URI-template resources. `.template_handler` receives raw
  parameters; `.content_handler` supports text and binary content vectors,
  including `text/html;profile=mcp-app` resources.
- `server.prompt(name).argument(name, description, required).handler(...)`:
  parameterized prompts with required string arguments checked before execution.
- `server.with_middleware(...)`: request wrappers in registration order around
  tool/resource/prompt/completion execution. `Next` returns a typed `Response` enum; middleware can inspect content
  without JSON serialization and must preserve the response variant. Listing/lifecycle methods bypass this
  application middleware. Use a transport layer to protect the entire endpoint.
- `server.with_result_cache(ttl_ms, model::CacheScope)`: advertise freshness and
  sharing hints on discovery, registry lists, and complete resource reads. Defaults
  remain `0`/private. Per-result resource hints take precedence. These are client
  hints; responses are recomputed. Choose public scope only for results that can
  be shared across authorization contexts.
  The protocol engine client honors freshness hints; changing the server does not force a
  client refresh. Hosts switching users on one client connection must update the
  protocol engine cache's private partition through `client.peer()`.
- `server.with_timeout(Duration)`: 30-second async handler deadline by default.
  Panics are contained and explicit cancellation drops handler futures. These
  controls cannot preempt blocking CPU work or detached tasks.
- `.ui_resource("ui://app/view")`: MCP App metadata on tool discovery and success
  results. Host UI rendering and an HTML application are separate concerns.

Use `#[serde(deny_unknown_fields)]` for strict field checking. JSON Schema constraints are validated before execution; keep semantic and
domain checks in your handler. `.skip_validation()` opts out of the schema check
but retains Serde decoding. `ToolError` messages are deliberately public and returned as `isError`;
never put credentials or private diagnostics in them. Panics are replaced with
an opaque protocol error (the process panic hook may still log to stderr).

`transport::serve_stdio`, `transport::http_router`, and `transport::serve_http`
use protocol engine transports and accept a cancellation token. Stdio shutdown awaits protocol engine
cleanup before returning. Tokio's stdin reader can remain blocked until EOF;
the example bounds owned-runtime teardown with `shutdown_timeout` so Ctrl-C
also exits while a client keeps stdin open. HTTP explicitly disables legacy
sessions and uses the protocol engine stateless manager, retaining host/origin checks and
request limits; the example listens on loopback. Requests need no transport
session store or sticky routing. Shared application state, tasks and durable
event replay require separate host-owned persistence. Transport-level
TLS, authentication, and deployment policy belong to the host.

Stdio and async byte-stream transports cap each incoming line at 16 MiB before
JSON parsing. The limit excludes LF and includes an optional CR; zero permits
only empty lines. Oversized input ends reception without draining the line.
Use `transport::serve_stdio_with_max_message_size` or
`Client::stdio_with_max_message_size` to choose a different limit. Custom streams
can use `protocol::transport::async_rw::AsyncRwTransport::new_with_max_message_size`;
child-process builders expose `max_message_size`. Partial input survives cancelled
receives, and the budget resets for each line. Outgoing messages, parsed JSON
allocations and process memory are outside this incoming payload limit.

Use `http_router_with_config` or `serve_http_with_config` with
`transport::HttpServerConfig` to configure modern HTTP admission and responses:

```rust
use mcp_rs::{Server, transport::{HttpServerConfig, http_router_with_config}};
use tokio_util::sync::CancellationToken;
let mut policy = HttpServerConfig::default();
policy.allowed_hosts = vec!["mcp.example.com:443".into()];
policy.allowed_origins = Some(vec!["https://app.example.com:443".into()]);
policy.max_request_body_bytes = 1024 * 1024;
policy.json_response = true;
let router = http_router_with_config(Server::new("research", "1"), CancellationToken::new(), policy);
```

Host entries accept hostnames or exact `host:port` authorities. Origin entries use
scheme, host and port; use explicit ports or `:*`. `allowed_origins: None` leaves
Origin validation disabled (the protocol engine default); `Some(vec![])` rejects every present
Origin. Requests without Origin can still connect. Host and Origin policies are
admission controls; endpoint authentication remains host-owned. Body limits cover
streamed uploads without Content-Length and return HTTP 413 before tool dispatch.
JSON preference falls back to SSE if a handler sends progress or another message
before its final response. Both helpers keep transport sessions disabled and use
the supplied cancellation token. The default helpers retain their existing policy.

## Port status

See [the parity roadmap](docs/port-roadmap.md), [first-release acceptance contract](docs/release-readiness.md), and [upstream provenance](UPSTREAM.md).
No crate or GitHub repository has been published.

## Rust support and crate features

The minimum supported compiler is **Rust 1.89** for the locked dependency graph.
The library forbids unsafe code, requires public API documentation, and separates
configuration errors (`RegistrationError`) from wire errors (`ErrorData`).

Default features enable `stdio`, `http`, and `client`. For an embedded framework without
transport dependencies:

```toml
mcp-rs = { path = "../mcp-rs", default-features = false }
```

Enable only `features = ["stdio"]` or `["http"]` as needed. The `client` feature enables typed clients and is included in the defaults.
`client-stdio`, `client-http`, `client-go-mrtr`, and `elicitation` opt into additional APIs.
`websocket` and `grpc` independently enable server adapters; combine either with
`client` to enable `Client::websocket` or `Client::grpc`. `grpc-tls` enables gRPC
TLS through Rustls with the AWS-LC provider. `websocket-tls` adds native WebSocket
TLS with the same provider. These transports are disabled by default. Applications own their Tokio runtime;
the core library requests specific runtime capabilities rather than `full`.

`Server::clone` shares configuration through `Arc`; registration on a clone uses
copy-on-write and preserves existing configurations. Captured handler state is
shared intentionally. Builders are `must_use`, and configuration types implement
`Debug` without inspecting handler captures.

See [contributing](CONTRIBUTING.md) for design rules and validation commands.

## Additional application APIs

- `.output_schema::<Output>()?` validates successful structured results.
  `.rich_handler` preserves native content and absent/empty structured objects.
- `Server::into_live()` creates an atomic registry. Edit `snapshot()`, call
  `replace(updated)`, then `notify_changed(peer)` for each affected connection.
  `remove_tool`, `remove_resource`, and `remove_prompt` edit configurations.
- `Client::connect` accepts protocol engine transports and requires modern discovery within ten
  seconds. `Client::connect_with_timeout` overrides that discovery deadline; zero
  rejects immediately. `.call::<Input, Output>` and
  `.tool::<Input, Output>` provide typed calls. Optional `Client::stdio` launches
  a configured command directly; `Client::http` accepts a caller-owned HTTP
  client/config. `.call_once` preserves native task/MRTR responses.
  Engine `RequestHandle::await_response` deadlines finish without waiting for
  cancellation queue capacity or transport acknowledgment. Cancellation on timeout
  is best effort; request queue admission and MRTR callbacks need separate deadlines.
  Dropped request handles/response futures release local progress and subscription
  routing registrations and pending responses, even after the runtime stops.
  Abandoned queued requests are skipped. Cancellation-safe transports can opt in
  to stopping in-flight sends. Native HTTP also closes abandoned request response
  streams after the POST finishes. SSE cancellation also interrupts blocked handler
  delivery and response draining. Final-response drains stop at EOF, the first
  stream error, 32 trailing messages or 50 ms. Completed request streams cannot
  reconnect during cleanup. The SSE decoder also yields during runs of ignored
  frames. Built-in byte parsing uses bounded slices and yields during continuous
  comments or partial events. Reconnection honors disabled retry policies and
  waits for both local delays and persistent server retry hints. Retry counters and
  exponential delays saturate at numeric limits; remote cancellation
  is still best effort.
- `Session::from_context(&ctx)` provides ordered progress and peer callbacks.
  `Session::cancelled()` lets cooperative helpers stop when their request or task
  ends. Task scopes outlive the creation response and end on completion, failure,
  cancellation, protocol engine expiry or manager shutdown; the host still owns joining helpers.
  `elicit` requires the `elicitation` feature and an explicit form capability
  on the current request; it does not use connection-wide handshake state. `Client::from_service` wraps custom
  protocol engine client connections only after validating the negotiated modern revision;
  it returns `Result` and cancels unsupported connections.
- `.with_task_manager(manager)` plus `.task_handler(options, callback)` enables
  modern protocol engine tasks, input requests, polling, updates, and cancellation. Native
  tool, prompt, and resource `.protocol_handler` callbacks can return native MRTR
  input requests or completed results. `Session::tool_request()`,
  `Session::prompt_request()`, and `Session::resource_request()` expose the original
  request state and input responses. Resource callbacks also receive raw URI-template
  parameters. protocol engine `get_prompt_once` / `read_resource_once` expose each round;
  `get_prompt` / `read_resource` fulfill input requests through the client handler
  and retry with the returned state. Automatic loops default to ten outbound rounds,
  64 inputs per round and four concurrent input callbacks per operation.
  `service::MrtrLimits` and `*_with_mrtr_limits` configure these budgets.
  Typed tool calls accept per-call overrides through `Client::call_with_mrtr_limits`,
  `Client::call_raw_with_mrtr_limits` and `TypedTool::call_with_mrtr_limits`;
  `MrtrLimits` is also re-exported at the crate root. Overrides affect only that
  operation, and ordinary calls retain the defaults. `Client::get_prompt` and
  `Client::read_resource` drive the same automatic loops; both expose
  `*_with_mrtr_limits` overrides and `*_once` native response methods. Skill index
  and content reads also drive resource MRTR through configured client callbacks.
  Oversized
  or retired sampling/roots input batches fail before any callback; the final
  allowed round never starts callbacks for a retry it cannot send. Zero rounds
  sends nothing; zero input budget still permits state-only retries. These are
  count limits, independent of transport byte limits and host callback deadlines.
  Completed resource responses retain cache hints;
  intermediate input requests pass through typed middleware without cache hints.
  The pinned Go implementation uses a different modern MRTR envelope; native
  protocol engine-client/Go-server MRTR interoperability remains open (see
  [wire differences](docs/compatibility.md#promptresource-mrtr-wire-differences)).
- Opt-in `client-go-mrtr` exposes `client::go_mrtr::{Client, Config}` for modern
  Go HTTP prompt/resource input loops. Supply an HTTP client, explicitly declare
  form/URL elicitation capabilities, and pass an async input callback to
  `get_prompt` or `read_resource`. It preserves opaque JSON state and cumulative
  answers with byte, round and whole-operation limits. See the
  [working example](examples/go_mrtr_probe.rs) and
  [verified scope](docs/go-mrtr-interop-report.json).
- `.with_completion_handler(callback)` handles typed prompt/template completions.
- Middleware includes `size_limit`, `rate_limit`, `observe`, and `logging`.
  Observations contain request IDs and outcomes rather than payloads.

See [compatibility](docs/compatibility.md) for deliberate differences and remaining
limits. [The shared wire fixture](tests/fixtures/core-wire.json) and
[validation report](docs/validation.md) describe what has actually been checked.

Task-update wire admission is checked against pinned Go and Rust HTTP servers;
malformed parameters return invalid-params errors when tasks are installed.
See [the compatibility notes](docs/compatibility.md#task-update-wire-validation)
for strict Rust input shapes and the explicitly tested Go differences.
[Task input and expiry checks](docs/compatibility.md#task-input-and-expiry-checks)
cover form/URL responses and host-configured TTLs, including differing Go/Rust
expiry behavior. [Concurrent-response checks](docs/compatibility.md#concurrent-task-responses-and-payload-validation)
cover conflicting answers, duplicates, independent task IDs, and handler rejection
of invalid payloads. [Cancellation/update races](docs/compatibility.md#cancellationupdate-races)
check valid terminal outcomes, repeated cancellation, late responses, and task isolation.

## WebSocket and gRPC

```sh
cargo run --locked --all-features --example parity -- --websocket 127.0.0.1:8081
cargo run --locked --all-features --example parity -- --grpc 127.0.0.1:50051
cargo run --locked --all-features --example client_probe -- --websocket ws://127.0.0.1:8081
cargo run --locked --all-features --example client_probe -- --grpc http://127.0.0.1:50051
```

`transport::websocket::serve_websocket` accepts a listener, cancellation token,
and `WebSocketServerConfig`. It accepts non-browser clients without Origin;
browser origins require an exact allowlist match. Defaults limit inbound frames
and messages to 4 MiB, concurrent connections to 1024, and initialization to ten
seconds. `WebSocketTransport::new` wraps a socket with caller-controlled TLS or
handshake authentication. The convenience client supports `wss` with WebPKI roots
and caller-supplied HTTP request headers.

Enable `websocket-tls` for `transport::websocket::serve_websocket_tls` and
`Client::websocket_tls`. Supply an `Arc<rustls::ServerConfig>` with a server
certificate and optional client-certificate verifier; the client accepts an
`Arc<rustls::ClientConfig>` with explicit trust roots and optional client identity.
The TLS client requires a `wss` URL and preserves request headers. Rustls is
available through `transport::websocket::tokio_rustls::rustls`. Connection limits
include TLS handshakes, and the server's handshake deadline covers TLS, HTTP
upgrade and MCP discovery together. Origin policy, frame limits and awaited
shutdown use the same path as the plaintext server. See the
[native TLS examples in tests](tests/websocket_tls.rs) for trust and mutual TLS setup.

`transport::grpc::serve_grpc` implements `/mcp.v1.MCP/Connect`, using the pinned
Go schema in `proto/mcp.proto`. `GrpcService` composes into a Tonic server so a
host can add TLS, authentication interceptors, and per-message limits. The
adapter defaults to 4 MiB protobuf messages, 1024 active streams, a 32-message
outbound queue per stream, and a ten-second MCP handshake deadline.
`GrpcService::with_config` accepts `GrpcServerConfig` to override these limits;
invalid runtime capacities and a zero deadline return `GrpcConfigError`. Limits
are shared by adapter clones. The handshake includes discovery and the first
ordinary MCP request; it does not limit established-session lifetime. For example:

```rust
let mut limits = mcp_rs::transport::grpc::GrpcServerConfig::default();
limits.max_streams = std::num::NonZeroUsize::new(64).unwrap();
limits.handshake_timeout = std::time::Duration::from_secs(5);
let adapter = mcp_rs::transport::grpc::GrpcService::with_config(server, stop, limits)?;
let service = adapter.service(); // Add this to your Tonic server.
```

Stop the host and call `GrpcService::shutdown` to
await tracked sessions. `Client::grpc` accepts a configured generated client and
RPC metadata. Credentials belong to RPC metadata/interceptors; the protobuf
envelope's `metadata` map is not installed as application context.

Enable `grpc-tls` for `transport::grpc::serve_grpc_tls`. Pass a Tonic `Identity`
containing the server certificate/private key and a `ServerTlsConfig`. Adding
`.client_ca_root(ca_certificate)` requires trusted client certificates for mutual
TLS. Configure the client channel with `ClientTlsConfig`, a trusted CA and the
expected server DNS name; add `.identity(client_identity)` for mutual TLS, then
pass the generated client to `Client::grpc` as usual. Tonic is re-exported under
`transport::grpc::tonic`. The helper shares the plaintext server's bounded session
lifecycle and rejects invalid TLS configuration before serving. Hosts own certificate
loading, trust policy and rotation. See [the native TLS tests](tests/grpc_tls.rs)
for complete server/client wiring and rejection cases.

Both transports support bidirectional protocol engine requests. Library shutdown awaits
session cleanup; applications own runtime teardown. Defaults do not configure
endpoint authentication. See [transport compatibility](docs/compatibility.md)
for deliberate differences from Go, including string IDs on the gRPC wire.

Bindings are checked in; building the library does not require `protoc`.
Maintainers can regenerate them with `python3 tests/generate_grpc_bindings.py`.

## Modern subscriptions

Install a shared hub to enable protocol engine 2026-07-28 resource, task, and registry-list streams:

```rust
use mcp_rs::{Server, SubscriptionHub};
let updates = SubscriptionHub::default();
let mut server = Server::new("research", "1").with_subscription_hub(updates.clone());
server.resource("topic://ai").handler(|_, _| async { Ok("Current AI digest".into()) })?;
// After updating the underlying digest:
let queued = updates.notify_resource_updated("topic://ai")?;
Ok::<(), Box<dyn std::error::Error>>(())
```

The typed client's `listen(SubscriptionFilter)` returns the protocol engine subscription;
consume `next()` and call `cancel()` when done. Setup has a ten-second deadline;
`listen_with_timeout(filter, duration)` overrides it and zero sends nothing.
The limit covers outgoing queue admission through acknowledgment and ends when
the stream is returned. Expiry removes local routing and attempts best-effort
peer cancellation; queue saturation can prevent cancellation delivery. Resource filters accept exact
registered URIs and concrete URI-template matches. Unknown URIs are declined.
List categories have separate `notify_tools_changed`, `notify_resources_changed`,
and `notify_prompts_changed` publisher methods. These publish only to matching
subscriptions, with the originating request ID tagged on every notification.
`DeliveryReport` counts queue admission, not delivery acknowledgment.

Each accepted category and resource gets an initial refresh hint. Clients should
read a current snapshot on that hint and on later updates; duplicate hints are
safe. This covers the protocol engine's acknowledgment-before-admission window. Middleware
runs once for admission, including authorization and normal handler deadlines;
the stream itself stays open until cancellation, shutdown, or delivery failure.
An acknowledgment alone is not an authorization grant. Admission errors after
acknowledgment appear through the subscription's next operation.

Defaults allow 1024 streams, 1000 requested entries per stream and 10,000 total
entries for each of resources and tasks, 4096 bytes per URI or task ID, and
five-second protocol engine sends. Resource/list updates and matching task updates
each have a separate 32-update queue per stream.
Full queues close the affected stream with an error so clients can resynchronize.
Requested entries, including duplicates and declined entries, count toward limits.
The typed client sizes its receive buffer for initial hints plus 64 updates.
`hub.shutdown().await` closes admission and waits for registered watchers to
unwind. Cancel subscriptions before closing clients for prompt cleanup; an abrupt
EOF may retain registrations through the protocol engine's five-second handler drain.

Hub state survives server clones and edits made from `LiveServer::snapshot()`.
Subscriptions retain their admission-time URI grants; replacing a registry does
not revoke them. Close the old hub when changing authorization grants or replacing
it with a different hub. Existing direct per-peer notification APIs remain
separate from the hub's scoped streams.

This uses the Rust protocol engine's current acknowledgment-and-stream schema. The pinned Go
revision returns a generated subscriptionId in a result instead; modern Go/Rust
subscription interoperability uses the explicit HTTP client below. The executable
[wire comparison](docs/subscription-wire-report.json) records that difference.
Retired `resources/subscribe` is excluded. Replay and distributed subscription
storage remain separate roadmap items.

### Native task subscriptions

Install both `.with_task_manager(tasks)` and `.with_subscription_hub(hub)` on the
server. Connect a protocol client configured with
`ClientCapabilities::builder().enable_tasks().build()`, then wrap its running
service with `Client::from_service`. Subscribe with
`SubscriptionFilter::builder().task_ids([task_id]).build()` and consume `next()`.
Both peers must advertise the tasks extension; otherwise task IDs are declined.
Task filters are exact opaque IDs, with no wildcard or task-list subscription.

After middleware admission, each distinct accepted ID receives one complete
`notifications/tasks` snapshot, followed by matching status/message updates.
Snapshots and registration share the task write lock, so no admission changes
are missed or replayed out of order. Input requests, resumed working state, and
terminal result/error payloads are included. Unrelated tasks do not fill the
queue. Unknown or expired IDs fail admission and release the entire registration;
the acknowledgment precedes admission and is not proof the IDs exist.

The manager is process-local and does not infer per-user task ownership. Hosts
must authorize requested IDs in subscription middleware or isolate task managers.
Accepted streams retain their admission-time grant until cancelled or closed.
TTL failure notifications occur on the manager's next opportunistic sweep; there
is no background expiry worker. Queue overflow requires reopening the stream for
fresh snapshots. Manager shutdown ends task receivers; hub shutdown closes streams.

### Pinned Go HTTP subscription compatibility

Enable `client-go-subscriptions` to consume the pinned Go server's stateless modern
HTTP stream. This is a separate client API; native `Client::listen` keeps the protocol engine
contract above.

```rust,ignore
use mcp_rs::{client::go_subscriptions::{Config, Subscription}, model::SubscriptionFilter};
let config = Config::new("http://127.0.0.1:8080/mcp".parse()?);
let filter = SubscriptionFilter::builder().resource_subscription("users://Felix").build();
let mut stream = Subscription::open(reqwest::Client::new(), config, filter).await?;
// Read the current resource snapshot after opening; Go sends no initial hint.
while let Some(notification) = stream.next().await? {
    // Refresh the affected resource/list using your regular client.
}
stream.close();
```

The adapter translates resource filters to top-level `uris`, sends modern routing
headers and per-request metadata, verifies Go's generated ID, and locally filters
scoped notifications. `requested_filter()` reports the request; Go supplies no
accepted-filter acknowledgement. Opening is bounded to ten seconds; requests,
JSON error bodies, and individual SSE events default to 1 MiB. There are no
background tasks or replay queues. `close`/drop disconnects the response body;
remote cleanup is asynchronous. EOF is an explicit abrupt-end error, followed by
`Ok(None)` on subsequent reads. Refresh before reconnecting.

TLS, credentials, redirects, and pooling remain caller-owned; use a supplied HTTP
client without a total response timeout for long-lived streams. Reverse Go-client/Rust-server wire adaptation and automatic Go resource publisher
integration remain outside this adapter. Native Rust task subscriptions use the
owned engine API above. The [pinned Go checks](docs/go-subscription-interop-report.json)
verify actual application negotiation, task completion/failure/cancellation, input
requests resumed through `tasks/update`, capability rejection, and upstream
transport push/cleanup.

Use `Subscription::open_with_tasks(client, config, filter, task_ids)` to include
Go's `notifications.taskIds` filter. Set `config.capabilities` with
`ClientCapabilities::builder().enable_tasks().build()` (or add the extension to
your existing capabilities); the Go server requires it. `task_ids` accepts an iterator of strings;
IDs stay opaque. Task and resource filters each allow at most 1,000 entries of
4,096 bytes, counting duplicates, and share the encoded request budget. An
unbounded task iterator is rejected after its 1,001st entry. `requested_task_ids()`
reports the requested IDs. Read `tasks/get` snapshots after opening, then consume
typed `ServerNotification::TaskStatusNotification` events with their full
status-specific payload and subscription metadata. Malformed task messages close
the stream instead of becoming custom notifications.

The pinned Go `ServeHTTP` wires task notifications only when the server enables
`Capabilities.ResourceSubscribe`; the real task fixture enables that setting.
A task listen acknowledgement alone does not prove the server has a publisher.

## Request-aware visibility

`with_visibility_policy` gates registered tools, resources, and prompts using the
current request context. The same predicate filters lists before pagination and
blocks direct access. Hidden entries return the same framework error as missing
entries, before tool inputs or required prompt arguments are validated.

```rust
use mcp_rs::{Server, VisibilityTarget};
#[derive(Clone)]
struct Principal { staff: bool }
let server = Server::new("research", "1").with_visibility_policy(|context, target| {
    let staff = context.extensions.get::<Principal>().is_some_and(|p| p.staff);
    match target {
        VisibilityTarget::Tool { name } | VisibilityTarget::Prompt { name } =>
            staff || name == "public",
        VisibilityTarget::Resource { uri, registered_uri, .. } =>
            staff || (registered_uri == "public://{id}" && !uri.ends_with("/private")),
        _ => false,
    }
});
```

The host must insert a verified `Principal` before framework dispatch. Client
metadata and clientInfo are self-reported and are not authentication. For HTTP,
protocol engine contexts carry `axum::http::request::Parts`; authenticated state placed in
Axum request extensions is available through `parts.extensions`. Framework
application middleware runs after the initial visibility check, so insert identity
in the host transport/service layer rather than tool middleware.

For resource lists, `uri` is the registered static URI/template. Reads and
subscription admission receive the concrete URI plus `registered_uri` and the
registered name. A hidden exact resource never falls back to a visible template.
Completion references are checked too; enabling visibility requires completion
references to resolve to registered prompts/resources. Completion callbacks still
authorize their individual suggestions; the policy gates the reference. Resource URI and completion
reference rewrites by middleware are checked again before callbacks execute.

Predicates replace the previous policy and must be fast, nonblocking, and free of
side effects. They may run more than once per request. Panics fail closed with an
opaque error. Without a policy, existing behavior is preserved. LiveServer loads
the current policy with each registry snapshot; in-flight work retains its snapshot.
Pagination does not pin an identity/policy snapshot across requests: an old cursor
may become invalid after permissions change. Notify affected peers when their
visible lists change.

Subscription admission checks concrete resource visibility before allocating
watchers. When a policy is installed, the protocol engine's preliminary context-free
acknowledgement treats valid requested URIs uniformly; hidden and missing URIs
then produce the same admission error. The acknowledgement is not an access grant.
Existing subscriptions retain admission-time grants; close their hub to revoke
those grants. List-change invalidations carry no object names. Capabilities and
host-side `get_tool` schema lookups remain configuration-level. Task administration,
identity verification, transport authorization, and revocation remain host-owned.


## Server identity and instructions

Configure native server metadata with typed builders:

```rust
use mcp_rs::{BuildInfo, Server, model::{Icon, IconTheme}};
let server = Server::new("research", "1.0.0")
    .with_build_info(BuildInfo::new("abc123", "2026-10-17T10:00:00Z"))
    .with_title("Research updates")
    .with_description("Papers, code, videos, and podcasts")
    .with_website_url("https://example.test/research")
    .with_icons([Icon::new("https://example.test/icon.svg")
        .with_mime_type("image/svg+xml")
        .with_sizes(vec!["any".into()])
        .with_theme(IconTheme::Dark)])
    .with_instructions("Search first, read sources next, and cite the originals.");
```

`with_server_info(model::Implementation)` replaces the complete native identity,
including name and version, while preserving registered methods, capabilities,
instructions, and separately configured build information. The individual title/description/website/instructions setters
clear their optional field when given an empty string. `with_icons` replaces the
collection; an empty collection clears it. Native `Implementation` option values
are preserved as supplied. URIs are advertised unchanged and are never fetched
or validated by these configuration methods.

Metadata uses the existing copy-on-write server configuration. Live snapshots
retain their metadata; replacing a live configuration changes future discovery
responses. Already received discovery data is not pushed to clients
automatically. Method capabilities continue to derive from the registered handlers.

Modern protocol engine discovery carries identity in result metadata under
`io.modelcontextprotocol/serverInfo`, with instructions alongside it. The protocol engine
keeps icons in its native `src`/array-valued `sizes` schema.
`with_build_info(BuildInfo::new(commit, build_date))` advertises Go's existing
`buildInfo` extension inside that identity; `with_build_info(None)` clears it.
Values are host supplied, with empty commit/date fields omitted. No provenance
is inferred from Git or the environment.

`client.server_metadata()?` reads the handshake snapshot, including build info.
For refreshed metadata, call `ServerMetadata::from_discovery(&discovery)?` on a
fresh protocol engine discovery result. Both readers accept native array-valued icon sizes
and modern Go's whitespace-separated size string, normalizing only the owned
view. Malformed advertised fields return an error; missing identity returns
`None`. Raw discovery metadata is never rewritten. The native protocol engine's
`server_info()` accessor alone omits build info and cannot decode Go size strings.

Outgoing icon arrays and Go's duplicate top-level discovery identity remain
explicit wire differences. Rust retains the protocol engine format and namespaced identity;
no older protocol lifecycle or fallback is added. See the
[metadata comparison](docs/server-metadata-interop-report.json).

### Server cards and catalogs

`server_card::ServerCard::from_identity(&identity)` builds an advisory static card.
`Remote::streamable_http(url)` advertises only MCP 2026-07-28. Repository metadata,
remote header/variable descriptions, and namespaced metadata are typed. The card
constructor normalizes names and truncates descriptions safely at 100 Unicode
characters. Keep actual credentials out of public card values.

With the `http` feature, merge the discovery routes into the MCP router:

```rust,no_run
use mcp_rs::{Server, model::Implementation, server_card::{ServerCard, Remote}, transport};
use tokio_util::sync::CancellationToken;

let identity = Implementation::new("org.example/research", "1");
let card = ServerCard::from_identity(&identity)
    .with_remote(Remote::streamable_http("https://example.test/mcp"));
let server = Server::new("org.example/research", "1").with_server_info(identity);
let router = transport::http_router(server, CancellationToken::new()).merge(
    transport::server_card::router(
        &card, "https://example.test/mcp/server-card",
        "urn:air:example.test:mcp:research",
    )?
);
# Ok::<(), serde_json::Error>(())
```

The routes serve `/mcp/server-card` and both `/.well-known/ai-catalog.json`
and `/.well-known/mcp/catalog.json`. They support GET/HEAD, public cache hints,
SHA256 ETags, conditional requests, and browser-readable CORS headers. They are
immutable snapshots; rebuild the router to change them. The host supplies the
public card URL and catalog identifier explicitly. Clients must reconcile cards
with live `server/discover`. Card schema validation, automatic live updates,
OAuth policy, and browser preflight handling are not provided by these helpers.

### In-memory skills

Skills (experimental SEP-2640) use ordinary resources. Register immutable bundles
with a required `SKILL.md` entry document and optional text or binary files:

```rust
use mcp_rs::{Server, skills::SkillBundle};
let mut server = Server::new("research", "1");
let skill = SkillBundle::new("research/review", "---\nname: review\ndescription: Review research papers\n---\nRead the paper and assess the evidence.\n")?
    .with_text_file("examples/check.py", "text/x-python", "print('review')\n")?;
server.register_skill(skill)?;
# Ok::<(), mcp_rs::skills::SkillError>(())
```

Read `skill://index.json` to discover entrypoints, then
`skill://research/review/SKILL.md` or supporting files. With the client feature,
`Client::list_skills()` decodes the catalog and `Client::read_skill_uri()` reads
its resources. Missing catalogs return the server's ordinary resource error.
The skills extension is advertised while bundles, archives or templates exist. Resource middleware,
timeouts, cache hints and visibility apply; the index omits hidden entrypoints.

Registration is atomic on conflicts. `remove_skill("research/review")` removes
all owned files or the archive resource. Removing any skill resource through
`remove_resource` removes its owning skill; removing the generated index removes
all bundles, archives and templates. Other resources and
existing server snapshots are preserved. Live hosts publish changes through
snapshot replacement and their existing explicit list-change notifications.

Bundles allow 256 files, 1 MiB per file and 8 MiB total before base64 encoding.
Paths use a restricted ASCII subset and reject traversal, URI encoding and
query/fragment syntax. MIME types are host supplied. Frontmatter supports only
required unique top-level `name`/`description`, simple quotes and `|`/`>` blocks
(joined with newlines), rather than general YAML. Progressive discovery and
webhook delivery helpers are described below.

### Loading skill directories

Load a single skill with `SkillBundle::from_directory("research/review", path)`,
then pass it to `Server::register_skill`. To load every immediate child directory
containing `SKILL.md` as a separate skill:

```rust,no_run
use mcp_rs::Server;
let mut server = Server::new("research", "1");
let count = server.register_skills_from_directory("./skills")?;
# Ok::<(), mcp_rs::skills::SkillError>(())
```

Loading is synchronous: use it at startup or run it in `spawn_blocking` from
async code. The result is an immutable snapshot; changing files on disk requires
loading again and replacing the live server snapshot. Collection registration
is atomic, including validation and resource conflicts. Root files and child
directories without `SKILL.md` are ignored.

The loader uses the pinned Go extension-to-MIME mapping; unknown extensions
are binary. Text files must be UTF-8. It rejects symlinks, special files and
invalid resource paths, and expects a stable host-owned directory tree during
loading. It does not provide a filesystem sandbox against concurrent changes.
The existing bundle limits apply. Traversal additionally allows at most 1,024
entries per bundle or collection root and 32 nested directory levels; a single
collection allows 64 skills and 32 MiB of combined file contents. These limits
are exported from `mcp_rs::skills`.


### Packed skill archives

Serve an existing archive as one binary resource and an `archive` index entry:

```rust,no_run
use mcp_rs::{Server, skills::SkillArchive};
let mut server = Server::new("research", "1");
let archive = SkillArchive::from_file("research/review", "./review.tgz")?
    .with_description("Review research papers")?;
server.register_skill_archive(archive)?;
# Ok::<(), mcp_rs::skills::SkillError>(())
```

`.zip`, `.tar.gz` and `.tgz` suffixes are case-insensitive; `.tgz` is advertised
as `skill://research/review.tar.gz` with `application/gzip`. ZIP uses `.zip` and
`application/zip`. `SkillArchive::new` accepts bytes with an explicit
`SkillArchiveFormat`. Packed snapshots allow at most 8 MiB before base64 encoding.
The default description is `Archived skill {name}`; sibling metadata is not read.

Archives are opaque: the framework does not unpack them, validate their headers,
or inspect embedded frontmatter. Supply trusted archives containing a root
`SKILL.md` whose name matches the final namespace segment. The size limit covers
packed bytes only. File loading is blocking, rejects symlinks and special files,
and has the same stable-filesystem contract as directory loading. Later file
changes do not affect reads. Registration is atomic; bundles and archives cannot
share a namespace. Ordinary resource visibility, middleware and removal apply.


### Parameterized skill templates

Register a dynamic entrypoint and its catalog entry together:

```rust
use mcp_rs::Server;
let mut server = Server::new("research", "1");
server.skill_template("skill://docs/{product}/SKILL.md", "Product documentation")?
    .handler(|_, _, parameters| async move {
        Ok(format!("# {} documentation\n", parameters["product"]))
    })?;
# Ok::<(), mcp_rs::skills::SkillError>(())
```

Templates appear in `resources/templates/list` and the skill index as
`mcp-resource-template` entries without a name. Concrete reads use ordinary
resource routing. Parameters are raw strings: `a%2Fb` stays `a%2Fb`. Static
resources take priority over matching templates. `.content_handler` accepts
native content blocks when text alone is insufficient; handlers own the validity
and metadata of their returned documents.

The grammar supports literal ASCII letters, digits, `_` and `-` in path segments,
plus unique whole-segment `{variable}` placeholders. Variable names use ASCII
letters, digits and `_` and cannot start with a digit. At least one variable and
a final `/SKILL.md` are required. Operators, partial-segment variables, escapes,
queries and fragments are rejected. This is a restricted subset of RFC 6570.

Registration is atomic and starts only when a handler is supplied. Dropping an
unused builder has no effect. `remove_skill_template(uri)` removes its owned
resource and index entry; `remove_resource(uri)` also removes its catalog entry.
Visibility checks use the template's own registration in the catalog and concrete
requested URIs during reads. Use `registered_uri` in the visibility policy to hide
an entire template namespace. Existing middleware, timeouts and live replacement
apply. Completion and subscription helpers use the same ordinary resource templates.


### Progressive tool discovery

Standard `tools/list` supports `group`, AND-matched `tags`, and `detail` through
our owned engine. Register discovery labels and optionally defer schemas:

```rust
use mcp_rs::Server;
use schemars::JsonSchema;
use serde::Deserialize;
#[derive(Deserialize, JsonSchema)]
struct Search { query: String }
let mut server = Server::new("research", "1");
server.tool("search_papers").group("research").tags(["read", "papers"])
    .defer_schema()
    .handler(|_, input: Search| async move { Ok(input.query) })?;
# Ok::<(), mcp_rs::RegistrationError>(())
```

A Rust client uses the ordinary list operation:

```rust,ignore
use mcp_rs::model::{ListToolsRequestParams, ToolListDetail};
let page = client.peer().list_tools(Some(ListToolsRequestParams {
    group: "research".into(),
    tags: vec!["read".into()],
    detail: Some(ToolListDetail::Full),
    ..Default::default()
})).await?;
```

`detail: names` omits input and output schemas; `full` restores them. Omitted detail
respects each tool's `defer_schema` setting. `ListToolsResult.tools` contains
`ListedTool` entries with optional schemas, group and tags. Registration definitions
remain `Tool` values with complete input schemas, so execution validation is unchanged.
For HTTP calls with `x-mcp-header` parameters, fetch full discovery first so the
client can emit the required routing headers. Lean listings preserve previously
learned full routing schemas. Invalid routing annotations fail registration with
`RegistrationError::InvalidInputSchema`, even with `skip_validation`: header names
must be nonempty HTTP tokens, case-insensitively unique, and attached directly to
top-level properties with an explicit string/integer/boolean type. Annotations
inside definitions, nested objects, arrays or combinators are unsupported. Argument
validation opt-out leaves this routing-schema check enabled. The HTTP client
rebuilds `Mcp-Method`, `Mcp-Name` and `Mcp-Param-*` for each message; custom
transport headers cannot supply or override these reserved routing values.
Other custom headers are preserved.
The native peer response cache defaults to 512 entries and a 16 MiB aggregate
payload budget. Configure it with `peer().set_response_cache_config(...)` and
`service::ClientCacheConfig::with_max_total_bytes(bytes)`. The byte budget counts
logical-key UTF-8 bytes plus each response's compact JSON representation;
container overhead, private-partition identifiers, parsed-value memory and
transport buffers are outside this budget. Zero bytes disables retention; zero
`max_entries` removes only the entry-count limit. Oldest entries are evicted first
and successful refreshes renew their position. Oversized responses are delivered
without retention and retire an older value for that key, preserving unrelated
entries. Reducing the budget trims existing entries immediately. Budget changes
also prevent requests already in flight from writing into the updated cache.
Counting stops at the budget without allocating a serialized response copy.

Routing retention is separate from the response cache: the HTTP transport keeps
only annotated parameter names, types and header names for at most 1,024 tools,
with a 4 MiB aggregate routing payload budget. Set
`StreamableHttpClientTransportConfig::max_routing_schemas` and
`max_routing_schema_bytes` to choose the limits; zero for either disables retention.
The byte budget counts each UTF-8 tool name plus its compact JSON projection;
it does not measure allocator/container overhead, in-flight responses or the
separate peer response cache. Oversized schemas remain in discovery results but
are not retained; an oversized refresh retires that tool's old entry without
evicting unrelated tools. The oldest full-discovery entry is evicted first, and
full refreshes renew its position. Lean listings and calls do not renew it.
After eviction, clear `peer().clear_response_cache().await` and fetch the tool's
`detail: full` discovery again before calling it with promoted arguments. A
cached discovery result does not restore the transport's routing entry. Use
filtered discovery or larger limits for catalogs that exceed the budget.
A `notifications/tools/list_changed` notification clears retained routing schemas.
Discovery requests started before that notification cannot repopulate or retire
current entries when their responses arrive later; fresh full discovery is needed.
Unsolicited and duplicate list responses cannot change routing state. Schema
validation still applies to all delivered full list responses.

The built-in reqwest HTTP client limits each buffered JSON response or HTTP error
body to 16 MiB of decoded bytes before parsing. Configure
`StreamableHttpClientTransportConfig::max_response_body_size` (or its builder);
zero rejects nonempty buffered bodies. Oversized responses return
`StreamableHttpError::ResponseBodyTooLarge` and leave later requests usable.
Successful SSE responses use the independent `max_sse_event_size` limit instead
of a total stream-body cap. Authentication remains host-owned.
Custom HTTP backends must override `StreamableHttpClient::post_message_with_limits`
to enforce buffered-body limits; the default trait implementation only forwards
the SSE limit. This bounds collected payloads, not total process memory or
allocations made by the network stack or JSON parser.

Visibility applies before filtering and 100-entry pagination. Pass `nextCursor`
with the same filters, identity and configuration snapshot; use
`peer().list_all_tools_with(params)` to collect every page while preserving the
query. Discovery cache keys include cursor, group, tags and detail. Labels are
hints; use visibility and authorization for access control.

Native `list_all_*` helpers stop at 1,024 pages or 100,000 items by default and
reject repeated cursors, including a supplied starting cursor. Configure
`service::PaginationLimits` with `list_all_tools_with_limits(params, limits)` or
`list_all_{prompts,resources,resource_templates}_with_limits(limits)`. Tool
filters, detail and the initial cursor are preserved. Empty and cached pages
count toward the page budget; zero pages performs no requests, and zero items
allows only empty pages. A failure returns a typed `ServiceError` without a
partial collection. These limits bound collection counts, not response bytes or
elapsed time; each page is parsed before checking its item count. Use single-page
APIs to process larger catalogs incrementally.

The host helper `Server::discover_tools(&ctx, &ToolDiscoveryParams)` uses the same
selection logic. `LiveServer` uses one current atomic snapshot per request;
retain `live.snapshot()` to page through a fixed configuration. Host catalog
results omit protocol cache hints. See [compatibility](docs/compatibility.md) for
query differences and the limits of Go comparisons.


### Out-of-band webhook delivery

Enable the optional `webhooks` feature to POST notifications to a receiver you
configure. This is a delivery helper matching the experimental Go API; it does
not subscribe to server events or register an inbound route.

```rust
use mcp_rs::transport::webhook::WebhookNotifier;
use serde_json::json;
# async fn deliver() -> Result<(), Box<dyn std::error::Error>> {
let notifier = WebhookNotifier::new("https://receiver.example/hook".parse()?)?;
notifier.send_channel_message("research", &json!({"type":"text", "text":"New paper"})).await?;
notifier.send_notification("notifications/resources/updated", &json!({"uri":"papers://latest"})).await?;
# Ok(())
# }
```

Each POST contains a JSON-RPC 2.0 notification with no ID, `Content-Type:
application/json`, and a matching `Mcp-Method` header. Any 2xx status succeeds;
other statuses return typed errors. `WebhookConfig` supplies authentication headers,
a per-send deadline (ten seconds by default), and a serialized notification limit
(1 MiB by default). Framing headers cannot be overridden. Endpoint admission and
any signing/token policy belong to the host.

The default client verifies HTTPS certificates using Rustls/platform trust and
disables redirects and retries. `with_client` accepts your own TLS, proxy, redirect
and retry policy. The configured deadline/size limit still applies. Response bodies
are dropped without buffering; dropping a pending send future cancels local work.
A receiver may already have accepted a canceled request.

`MultiNotifier` accepts ordered `Arc<dyn NotificationSender>` targets, attempts them
sequentially, and returns all typed failures with their target indices. An empty
set succeeds. No background worker, durable queue, automatic event wiring or
delivery guarantee is installed. See [webhook interop](docs/webhook-interop-report.json)
for the bounded pinned Go comparison.
