> Current architecture: `mcp-rs` owns its protocol engine in
> `crates/mcp-protocol`, an attributed source fork of rmcp 3.5.1. There is no
> external rmcp dependency. References below to rmcp/SDK wire shapes describe the
> inherited implementation and historical comparisons. Native task-ID subscriptions
> and progressive `tools/list` are implemented in the owned engine.
> See [provenance](../UPSTREAM.md) and the [roadmap](port-roadmap.md).

# Modern compatibility contract

Only MCP **2026-07-28** is supported. `Server` and `LiveServer` advertise that
revision alone and reject `initialize`. `Client::connect` uses discovery without
fallback and a ten-second discovery deadline. `Client::connect_with_timeout`
overrides that deadline; zero rejects before sending discovery. Expiry returns
`ClientError::DiscoveryTimeout` and drops the discovery future and owned transport.
Transport background cleanup can finish asynchronously. The deadline covers
discovery sends/responses, not earlier transport construction or later requests.
HTTP/stdio/WebSocket/gRPC convenience clients inherit this default; WebSocket
TCP/TLS/upgrade retains its separate ten-second limit.
`Client::from_service` validates the negotiated revision, returns an
error, and cancels an unsupported SDK connection. Retired task augmentation,
`tasks/list`, `tasks/result`, split HTTP/SSE, roots, and sampling are excluded.
The SDK re-export remains available for native types; it is not a promise to
support its older protocols. The framework transport helpers enforce the same boundary for custom SDK handlers.
Hosts that directly use the re-exported SDK own their protocol policy.


Engine `RequestHandle::await_response` deadlines return without waiting for
cancellation queue capacity or transport acknowledgment. Timeout cancellation is
best effort and can be discarded when the queue is full or closed; it does not
guarantee remote termination. `PeerRequestOptions` deadlines start when response
waiting begins and exclude outgoing request queue admission and MRTR callbacks.
Explicit `RequestHandle::cancel` still awaits notification delivery.

Dropping a request handle or its response future synchronously removes its local
progress watcher and subscription routing entry, without spawning cleanup work or
requiring a running runtime. Transport-error exits have the same cleanup. The guard
retains the original registration keys even if public handle IDs/tokens are changed.
Drop cleanup also retires the service's pending response sender, including its
request-association signal. A request dropped before service-loop dispatch is not
sent. Admission and cleanup share a short map lock so cleanup cannot race a late
insertion. Pending responses are cleared on service exit, abort and unwind even
when callers retain peer clones. Existing numeric-string response ID handling is
preserved. No map lock is held across awaits or response delivery.

`Transport::supports_request_send_cancellation` defaults to false. Transports may
opt in only if dropping a request send preserves connection/framing integrity and
releases request-owned work. Worker transports forward their worker's existing
request-cancellation support, including native HTTP. Opted-in sends end when their
request handle is dropped, cancelled or times out. Other transports keep in-flight
sends running until completion or service shutdown while still retiring pending
responses. Drop cleanup does not itself enqueue a cancellation notification or
guarantee remote termination; explicit cancellation sends a notification. Existing
subscription establishment/drop cancellation remains in place.

Native HTTP request response streams also observe the caller's local lifetime after
POST acknowledgment. Handle/future drops, explicit cancellation and response timeout
cleanup release that stream even when its handler queue is full; sibling requests
and common notification streams remain independent. The lifetime is carried in
non-serialized request extensions and introduces no background watcher task. Stream
completion recognizes this local cancellation instead of synthesizing a premature
close error. Direct transport sends without a service-owned lifetime keep their
existing behavior. Explicit Go HTTP adapters retain their existing stream lifetime
so their cancellation POST can finish; this does not add legacy native support.
The shared HTTP SSE reader observes its stream token across reading, handler queue
delivery and the bounded 50 ms final-response drain, with cancellation taking
priority when ready. This also covers common notification streams and direct
transport sends without a service request token. Cancelling a stream leaves sibling
and parent tokens active. Final-response draining is best effort for connection
reuse: it stops at EOF, the first stream error, 32 trailing messages or the existing
50 ms timeout. It yields after every trailing message so ready streams give timers
and cancellation a chance to run. Reaching the count/time budget drops the stream;
connection reuse is not guaranteed. Trailing messages are discarded and errors do
not replace the final response already delivered. This work budget is independent
of the existing per-event byte limit; custom backends must still avoid blocking
inside stream polling.
Request-scoped SSE resumption stops after a final JSON-RPC response or error, even
if the stream has an event ID and advertises immediate retry. Cleanup can still
consume the current response body within the drain budget, but EOF/body errors
cannot open another GET. Incomplete request streams still resume with their last
event ID, and common notification streams retain their independent retry policy.
The JSON-RPC SSE decoder performs at most 32 internal steps per poll before
yielding and waking itself. Continuous ready control frames, data-less events,
and malformed JSON therefore do not keep this decoder in one poll indefinitely.
Messages and last-event-ID/retry state survive the yield. This limits decoder
work independently of raw byte parsing. Built-in byte parsing separately splits
body chunks into at most 16 KiB zero-copy slices and returns pending with a self-wake
after 32 slices, including empty chunks. Continuous comments and partial events
therefore yield even when they produce no event. Split UTF-8 and CRLF state, event
order, source errors and the aggregate event-size limit are preserved. A retained
`Bytes` tail can still share the original backend allocation; this is a work bound,
not a bound on the size of backend allocations. Custom backends must not block
inside their own poll implementations.
Both EOF and recoverable stream errors consult the local retry policy before
opening a new GET. `NeverRetry` and zero-attempt policies cannot be overridden by
a server retry hint. Permitted retries wait for the greater of the local delay
and latest server hint; the hint persists across successful reconnections until
a later event replaces it. Dropping a waiting stream cancels its timer without
opening the GET. Policies still count consecutive connection failures per outage;
a successful reconnect starts a fresh outage budget, not a lifetime attempt cap.
The exponential backoff exponent conversion and failure counter saturate at their
numeric bounds. Large caller-supplied counts cannot wrap back to short delays,
and an exhausted counter cannot wrap around to reopen its retry budget. Duration
saturation, optional delay caps, zero delays and attempt limits remain supported.

## Differential reference

`tests/go-reference/main.go` and `examples/parity.rs` register the same application.
`tests/fixtures/core-wire.json` supplies the exact same requests and expectations
for both. The Go build script requires a clean checkout at the pinned commit.

The suite checks explicit semantic projections: returned values, required fields,
error categories, static/template separation, binary data, URI parameters, stable
ordering, pagination, and absent versus empty structured content. It does not
compare full schemas or diagnostic wording, nor certify every protocol method.
Both peers are separately checked against the fixture, not just against each
other. Every permitted mismatch needs an explicit fixture entry and reason.

The shared cases and task/MRTR tests use 2026-07-28 through modern discovery.
Negative tests verify that older clients and retired methods are rejected.

## Intentional differences

- A missing required prompt argument returns -32602 in Rust. The pinned Go
  dispatcher sanitizes that failure into -32603. The suite requires the exact
  documented result for each language and reports this mismatch separately.
- Rust validates successful output against a declared output schema before
  delivery. The Go dispatcher primarily promotes structured output for strict
  clients to validate. Rust refuses invalid output with an opaque protocol error.
- Rust rejects empty/duplicate registrations and malformed or duplicate-name URI
  placeholders. These are explicit typed configuration errors rather than relying
  on overwrites or ambiguous template matching.
- Application errors use ToolError for deliberately public execution failures.
  Native protocol handlers and SDK types provide advanced response control.

URI templates support named `{parameter}` segments, matching Go's basic matcher;
parameters retain their raw percent-encoded representation. This is not a full
RFC 6570 expression implementation. Exact resources take priority; overlapping
templates are tried in lexicographic order for deterministic behavior.

## Scope boundaries

`Session::elicit` validates the current request's explicit form capability and
uses a typed schema/callback. An empty legacy elicitation capability object does
not enable it. No connection-wide client identity or capability state is synthesized.


- Static Server clones use copy-on-write. LiveServer shares an atomic registry;
  replacement preserves in-flight snapshots and is last-writer-wins. Notify each
  affected peer explicitly. The registry does not retain every connection.
  Pagination across separate requests does not pin a registry revision.
- Application middleware wraps tool/resource/prompt/completion execution.
  Request-aware visibility gates registry lists and matching access paths before
  execution. Lifecycle and task administration still require endpoint-level policy.
  The host controls identity verification, transport credentials, and authorization.
- Session progress updates from cloned reporters share ordering. Repeated or
  decreasing values normally advance by 0.1; when precision requires a larger
  step they advance to the next representable float. Once the greatest finite
  value has been sent, further updates fail with invalid-params and emit no
  notification. This strict ordering at extreme magnitudes is Rust behavior,
  not a newly certified Go parity case.
- Task handlers use the installed process-local SDK manager. The application owns
  admission policy, manager lifetime, retention, and shutdown. Task cancellation
  drops async handler futures; blocking work still requires application care.
  Each task owns an independent context cancellation scope. A drop guard signals
  `Session::cancelled()` on completion, failure (including panic/output validation),
  cancellation, SDK expiry or manager shutdown, so cooperative helper work can
  stop. Returning the creation handle and ending other requests/tasks do not cancel
  it. The SDK signals ordinary request scopes; the host owns joining helper tasks.
- Modern task state/input/cancellation and native tool/prompt/resource MRTR are covered by local
  tests. Additional wire tests cover unknown-ID errors, ignored input keys,
  declined/cancelled form responses, terminal updates, and retention. The task
  handler decides how an elicitation response affects the result: a `cancel`
  response does not automatically mean task cancellation. Terminal tasks remain
  queryable for one configured TTL window after reaching a terminal state, then
  SDK access sweeps them; `ttl_ms: None` retains them until manager shutdown.
  Native static/live tests check completed, failed, cancelled, expired and
  unlimited tasks: stable terminal payloads, cleared input requests, dropped
  pending operations, eviction errors and shutdown cleanup. Expiry is observed
  on SDK access and starts its own terminal retention window. This is tested SDK
  behavior; selected Go/Rust expiry checks are described below.
- Prompt/resource `.protocol_handler` callbacks return native SDK response enums.
  Each retry receives the original typed request in context extensions and through
  `Session::prompt_request()` / `resource_request()`. Visibility and required prompt
  arguments are checked on retries. Middleware sees ordinary completed results as
  `Response::Prompt` / `Resource`, and intermediate native responses as
  `PromptProtocol` / `ResourceProtocol`. Complete resource reads preserve explicit
  cache hints and inherit missing defaults; input requests receive no cache hints.
  Static/live round trips and SDK automatic client loops are tested locally.
  Native automatic loops default to ten outbound rounds, 64 inputs per round and
  four concurrent callbacks per operation. `service::MrtrLimits` configures all
  three through `*_with_mrtr_limits`; round-only helpers retain the other defaults.
  The typed `Client::call_with_mrtr_limits`, `call_raw_with_mrtr_limits` and
  `TypedTool::call_with_mrtr_limits` forward per-operation budgets to the same
  native loop. `mcp_rs::MrtrLimits` re-exports the engine's type. Overrides do not
  change connection or handle defaults; typed decoding and native execution status
  follow their ordinary call methods. Inputs are validated before zero-round
  rejection, and `call_once` continues to return a single native response.
  `Client::get_prompt` / `read_resource` also drive automatic input rounds and
  preserve native results. Their `*_with_mrtr_limits` methods set per-call budgets;
  `get_prompt_once` / `read_resource_once` expose native complete/input-required
  response enums without invoking callbacks. The original request parameters,
  including opaque retry state and answers, pass through unchanged. Hosts can use
  these single-round methods for explicit resumption. `list_skills` and
  `read_skill_uri` use the automatic resource path with default limits. Access
  checks remain server-controlled; custom callbacks must be configured through
  `Client::from_service`. These helpers do not fetch elicitation consent URLs.
  A batch over the input cap returns `InputRequiredRequestsExceeded` before any
  callback. Retired sampling/roots inputs reject the entire batch before callbacks.
  No callback runs on an input-required final allowed round because no retry can
  be sent. Callback failure or caller cancellation drops inline active/queued
  callbacks without sending partial responses. Each started callback's context
  cancellation token is signalled when its scope exits, including success, error,
  unwinding and operation drop. A sibling failure also ends active callback scopes;
  independent operations retain separate scopes even for identical input keys.
  Hosts must cooperate with cancellation and own joining any spawned helper tasks.
  Effects already performed by host callbacks are not rolled back. These count limits do not bound response parsing,
  callback duration or host-spawned background work.
  Malformed native `inputResponses` / `requestState` fields return invalid parameters
  (-32602), including when SDK typed decoding falls back to a custom request.

## Prompt/resource MRTR wire differences

The bounded [HTTP comparison](prompt-resource-interop-report.json) checks 22 cases
against each actual server at MCP 2026-07-28. Matching application outcomes do not
mean native wire compatibility. Both are modern implementations, with different
MRTR representations:

| Field | Pinned Go | Rust SDK 3.5.1 |
|---|---|---|
| Input requests | Array of `{id, kind, payload}` | Object keyed by ID, values `{method, params}` |
| Retry responses | `_meta["io.modelcontextprotocol/inputResponses"]`: array of `{id, payload}` | Top-level `inputResponses`: object keyed by ID |
| Retry state | `_meta["io.modelcontextprotocol/requestState"]`: opaque JSON | Top-level `requestState`: opaque string |
| Malformed string responses | Ignored; input is requested again | Invalid parameters (-32602), HTTP 200 |

The runner sends each implementation's own format and checks its actual returned
shape. Supplying the other retry envelope leaves the request pending and does not
echo the foreign state; no implicit conversion is provided. A native Rust SDK client
cannot directly drive the pinned Go MRTR loop. The separate opt-in
`client-go-mrtr` adapter handles this Go dialect for HTTP prompt/resource
elicitation; it leaves the framework's SDK-native public protocol unchanged.

Cases cover prompt and resource form/URL accept, decline and cancel; two sequential
form inputs; empty and unknown response IDs; malformed response types; opaque string
state echo; and complete-only resource cache hints. URL consent data is inspected
without fetching a URL. The sequential manual probe carries prior answers cumulatively
because Go replays the handler. These differential checks do not exercise an
automatic client; the separate adapter checks below cover automatic replay.

The explicit `client::go_mrtr::{Client, Config}` API is gated by `client-go-mrtr`.
It accepts a caller-owned `reqwest::Client`, endpoint, host headers, identity and
capabilities. Declare the handled form/URL modes explicitly: an empty SDK
elicitation capability does not declare a mode. `get_prompt` and `read_resource`
accept an async callback returning native `ElicitResult`; callback errors abort
locally. The adapter validates an entire input batch before callbacks, preserves
wire callback order, rejects duplicate/replayed IDs and unsupported input kinds,
and retains cumulative answers for Go's handler replay. State remains opaque JSON
and is isolated to each operation. Completed responses decode to native prompt or
resource result types, including resource cache hints.

The [adapter report](go-mrtr-interop-report.json) records 14 automatic operations
against the pinned Go broker: form/URL accept, decline and cancel, plus two-round
form input for both methods. Local tests cover structured JSON state, malformed
frames, capability admission, byte/round limits and a whole-operation deadline
including callbacks. JSON and bounded SSE responses are accepted. Dropping the
operation cancels inline HTTP/callback work. URLs for consent are passed to the
host callback and are never fetched by the adapter.

This API targets a configured modern Go endpoint directly. It performs no discovery
negotiation, initialize, automatic fallback, reconnect or background processing.
TLS, authentication, redirects and HTTP retry policy belong to the supplied client;
see [the probe](../examples/go_mrtr_probe.rs) for a client with redirects and retries
disabled. Reserved framing/routing headers cannot be supplied in `Config.headers`.
Tool calls, tasks, sampling, roots, a reverse Go-client/Rust-server adapter, and
native Go/SDK wire alignment are outside this API.

Go injects `ElicitFromContext` only for tools. The prompt/resource reference fixture
constructs the exported `NewElicitor(SessionFromContext(ctx))` facade explicitly,
then exercises the actual broker and dispatcher. No Go protocol implementation is
replaced in the fixture. The Rust fixture uses the new native protocol builders.

Opaque MRTR state is application-controlled. Seal or otherwise verify any state
that contains trusted data; the SDK's request-state feature is available through
the re-exported SDK, but this framework does not sign arbitrary state itself.

## Task-update wire validation

`tests/task_update_interop.py` checks 11 update cases against each real HTTP
server, including a pending-input snapshot after every update and a final valid
answer that completes as `hello Ada`. See
[task-update-interop-report.json](task-update-interop-report.json).

Rust requires a string `taskId` and object-valued `inputResponses`. Missing/null
responses, arrays and scalar response containers are rejected. When the task
manager is installed, malformed SDK task requests that fall through typed decoding
return JSON-RPC invalid params (`-32602`, HTTP 200), on both static and live
servers. Without the task extension those methods remain unavailable. Retired
methods remain method-not-found. Validation does not echo submitted parameter data.

Pinned Go accepts missing/null responses and an empty response array as no-ops.
Rust does not adopt those alternate shapes. For the tested invalid scalar/array
elements and missing/numeric task IDs, Go returns HTTP 200 with error `-32603`;
Rust returns HTTP 200 with `-32602`. These differences are asserted explicitly,
not normalized. Both acknowledge empty objects and unknown response keys without
answering the outstanding input request. Every tested update preserves that
pending input, and a subsequent valid object response resumes the task.

The suite does not certify response-payload schema validation, every input kind,
concurrent/duplicate updates, task authorization, or durable tasks.
Task input response payloads remain opaque to the framework and must be validated
by the receiving handler.

## Task input and expiry checks

`tests/task_input_interop.py` checks six form/URL elicitation responses and two
host-configured TTL cases against each real HTTP server. See
[task-input-interop-report.json](task-input-interop-report.json). Both servers
publish keyed `elicitation/create` input requests and resume on `accept`,
`decline`, and `cancel`. The fixture handler completes successfully for all three
actions: cancelling an input interaction does not itself cancel a task. Form
acceptance validates the fixture's expected content; payloads remain opaque to
the framework. The URL check exchanges consent metadata without fetching the URL.

The host sets a 1,500 ms TTL using its manager API; requests do not include the
retired client-side task option. A completed result remains queryable before
expiry and eventually becomes unknown on both servers. An unfinished Go task
becomes unknown at its creation-based TTL; the Rust SDK first exposes `failed`
with an error, then retains that terminal state for another TTL window before
removal. This uses opportunistic registry access, not a background expiry service.
Unknown/expired IDs return `-32603` in pinned Go and `-32602` in Rust (HTTP 200).
These differences are asserted explicitly rather than hidden by an adapter.

The tests cover these short, process-local cases, not exact eviction timing,
executor cancellation in Go, unlimited or durable retention, every input kind,
concurrent updates, or general response-schema validation. Go's fixture bounds
its unfinished executor independently because registry expiry does not cancel it.

## Concurrent task responses and payload validation

`tests/task_concurrency_interop.py` runs eight conflicting-response races, one
two-task isolation case, and nine invalid-payload cases against each real HTTP
server. See [task-concurrency-interop-report.json](task-concurrency-interop-report.json).
Each race sends two independent requests together; either answer may win. The
fixture then asks for confirmation so duplicate responses are tested while the
task is still waiting. Matching and conflicting duplicates leave that second
input unchanged, and its completion retains the originally chosen answer.
Separate task IDs keep concurrent answers scoped to their respective tasks;
this does not establish user/tenant authorization.

Both managers acknowledge object-valued `inputResponses` containing arbitrary
response values. The nine invalid cases cover null/scalars/arrays, missing or
unknown actions, absent accepted content, and a wrongly typed field. The Go
elicitation decoder or fixture handler subsequently rejects them; the Rust
fixture handler returns invalid params. These tasks become `failed`, with error
`-32603` in Go and `-32602` in Rust. These codes describe the fixture's execution
failures, not admission-time validation or generic schema enforcement. A correct
answer sent after failure does not revive the task.

On a terminal task, Go rejects updates with `-32603` over HTTP 200. Rust
acknowledges and ignores them. Both preserve the completed result or failure.
These differences are asserted explicitly. Payload validation and any retry
interaction remain the receiving handler's responsibility; `tasks/update` does
not automatically reissue an invalid input request. The race checks establish
selected observable outcomes, not exhaustive schedule coverage, execution-once
side effects, cancellation races, or distributed consistency. Go replays its
handler between input rounds, so application side effects must account for replay.

## Cancellation/update races

`tests/task_cancel_interop.py` checks six ordered/isolation cases and eight
rounds each of cancellation versus an input response, final completion, and
handler failure against each real HTTP server. See
[task-cancel-interop-report.json](task-cancel-interop-report.json). Ordered checks
cover cancellation before an answer, after the first of two answers, after
completion/failure, duplicate cancellation, and isolation from another task.

When another input is still required, cancellation settles these fixtures as
`cancelled`. A cancellation raced against the final answer may settle as either
`cancelled` or `completed`; against an invalid answer it may settle as either
`cancelled` or `failed`. The suite accepts either valid ordering and checks the
result/error when present. It does not require both outcomes to appear in a run.
Late responses and repeated cancellation preserve each observed terminal outcome.

Pinned Go makes an active task terminal immediately on cancellation and rejects
subsequent cancel/update requests with `-32603` (HTTP 200). Exactly one of two
concurrent cancellations is acknowledged. Rust records cancellation intent;
framework task handlers observe it and drop their async handler futures. Both
concurrent cancellations and late terminal requests are acknowledged by Rust,
while existing completed/failed results remain intact. These differences are
asserted explicitly rather than normalized.

This is bounded HTTP behavior coverage. It does not prove every schedule,
termination of blocking work, rollback of side effects, distributed consistency,
or task authorization. Cancellation remains an application concern for blocking
or externally spawned work; a terminal snapshot alone does not prove it stopped.

## Result cache hints and HTTP routing

`Server::with_result_cache(ttl_ms, CacheScope)` sets client freshness/sharing hints
on discovery, all four registry lists, and complete resource reads. This is hint
advertisement; the application server recomputes responses. The typed client
uses the SDK's per-peer response cache. Explicit per-result resource hints survive the default
policy. Tool calls, prompt execution, errors, and incomplete resource results are
not given cache hints by the policy. Configuration follows normal clone/live
snapshot semantics.

Rust keeps the SDK's conservative `ttlMs: 0`, `cacheScope: "private"` default.
Pinned Go defaults to 60,000 ms/private; this deliberate difference is checked
rather than normalized away. Public scope requires the host to ensure results
can be shared across authorization contexts, especially when using visibility
policies. Per-result hints can override the configured policy.

The owned client engine's cache reuse, expiry, stale-on-error policy, recovery,
private-partition changes, bounded oldest-entry eviction and disabling the cache
are checked over real duplex connections. A successful refresh with an effective
zero TTL removes any previous entry, so a later failure cannot resurrect the
superseded value. Notifications invalidate every cached tool query/page, prompt
lists, resource lists/templates, or the exact updated resource URI as appropriate.
List changes preserve independent resource-read entries and unrelated families.
In-flight responses cannot repopulate entries after notification invalidation,
explicit clearing, private-partition changes or disabling the cache.

Hosts that change users on a connection must update
`client.peer().set_response_cache_config(...)` with
`ClientCacheConfig::with_private_partition`. Invalidation suppresses cache writes;
it does not cancel an existing request or change the response already owed to its
caller. Server configuration replacement alone does not force clients to discard
still-fresh cached responses. The live-policy test disables client caching to
check current server responses. Shared intermediary caches, automatic host
identity detection and exhaustive cache certification remain outside these checks.

`tests/cache_interop.py` checks all six cacheable methods, configured private and
public policies (including zero TTL), and execution/error exclusions against real
pinned Go and Rust processes. See [cache-interop-report.json](cache-interop-report.json).
This does not certify HTTP intermediary caching or client freshness behavior.

Modern HTTP routing-header validation is provided by the owned protocol engine
through the framework HTTP router. Each request uses one service factory result
for schema admission and dispatch, without retaining tool schemas or absent-tool
lookups across requests. `tests/http_routing.rs` and `tests/http_routing_refresh.rs`
check actual HTTP admission and handler execution, sequential live annotation
replacement/removal and registration, alternating factories, malformed optional
headers, and typed-client encoded names/strings, integers and booleans.

Routing annotations share the feature-independent
`model::validate_tool_header_annotations` contract. Typed registration rejects
invalid annotations as `RegistrationError::InvalidInputSchema` before installing
a tool, including task/rich/protocol handlers and `skip_validation` registrations.
Only direct top-level properties with an explicit string/integer/boolean type may
carry a nonempty RFC token name; names must be case-insensitively unique.
Nested schema annotations, definitions, array items and combinators are rejected.
Examples/defaults/constants/custom metadata remain application data. This helper
does not validate all JSON Schema constraints or promote annotations through refs.
HTTP admission enforces the same contract for custom handlers. HTTP clients filter
invalid discovery entries and discard any previously learned routing schema for
those names; unrelated and omitted-schema entries retain their learned schemas.

Stateless HTTP calls `ServerHandler::request_snapshot` once before header
admission. `LiveServer` captures an isolated registry configuration, so concurrent
replacement cannot switch the tool definition between admission and dispatch.
The framework's modern-policy wrapper and the engine's `Arc`/`Box` handler wrappers
forward the hook. Deterministically ordered replacement tests verify the admitted
handler executes and the next request sees the new annotations, through all three
handler forms.

Custom handlers default to no snapshot and must override/forward the hook if they
need this guarantee for their own mutable registries. Shared application state,
task managers and event hubs remain shared; only registry configuration is pinned.
Stream-oriented transports retain their existing per-operation snapshot behavior.
Clients must refresh full tool discovery after annotation changes; no schema
revision protocol is provided. These are bounded Rust HTTP checks, not complete Go
routing parity or verification of every transport/client path.

## WebSocket and gRPC adapters

The optional `websocket` feature sends one JSON-RPC object per text frame and
accepts text or binary JSON frames, as Go does. Unlike Go's default origin policy,
Rust requires an exact configured Origin match for every browser request; an
absent Origin remains accepted. The listener closes excess TCP connections
before upgrading, rather than returning Go's HTTP 503. There is no default idle
read deadline; HTTP upgrade/MCP initialization and writes are bounded. Incoming
malformed JSON or oversized frames terminate that peer, while the listener
continues serving. Rust provides full SDK bidirectional requests; the pinned Go
WebSocket server only provides a notification sender and explicitly lacks a
bidirectional request sender. Native TLS termination is available through
`websocket-tls`; caller-upgraded TLS sockets can also use `WebSocketTransport`.
The standard convenience client supports wss with WebPKI roots.

The optional `grpc` feature uses the exact pinned Go protobuf schema and service
path. JSON payloads stay JSON inside protobuf bytes. Request IDs are opaque
strings on that wire: Rust tags outgoing numeric and string IDs separately,
records at most 1024 unanswered requests, restores original types on replies,
and maps cancellation references consistently. Incoming peer IDs remain opaque
strings, including quotes, slashes, and newlines. Invalid envelopes and unknown
response IDs close the session; this is stricter than Go ignoring unknown types.
Progress token types are preserved in JSON params rather than coerced from the
envelope's string-only meta field. The envelope metadata map is not an
authentication mechanism and is not copied to application context. Use Tonic RPC
metadata and host interceptors. Tonic channels/TLS lifetime remains caller-owned.

Client stream opening is lazy and polled alongside the SDK's first send. This
matters for the pinned Go server, which withholds gRPC response headers until its
first MCP response; eagerly awaiting headers would deadlock before initialization.
There is no detached opening task. RPC opening errors close receive and surface
as SDK initialization failures, with the underlying status available to tracing.

Server adapters await SDK session cleanup on cancellation. WebSocket close awaits
the close-frame write, bounded by ten seconds, without waiting indefinitely for
peer acknowledgement. gRPC close drops the request sender and response body.
By default the gRPC service bounds admission to 1024 streams, each with a
32-message outbound queue and ten-second writes. `GrpcServerConfig` and
`GrpcService::with_config` let hosts set stream admission, per-stream queue
capacity, encoded/decoded protobuf message limits and the handshake deadline.
Capacities above Tokio limits and zero deadlines return typed `GrpcConfigError`
values. Limits and admission permits are shared across clones. The handshake
deadline starts at Connect admission and covers discovery plus the first ordinary
MCP request, because SDK discovery is a bootstrap probe; it ends when the SDK
establishes the modern session. Idle peers release their permits on timeout.
Tonic defaults are capped to 4 MiB in both directions by `GrpcService::service`;
the configured message limit replaces that default. Callers may further configure
the returned service or generated client. `serve_grpc` and `serve_grpc_tls` keep
default limits; host composition supports custom limits with TLS/interceptors. Both adapters use modern discovery for framework
Server/LiveServer handlers. Native Rust WebSocket and gRPC TLS/mTLS checks are
described below; bidirectional loopback TLS/mTLS checks are included. Proxy authentication,
reconnection, compression and production load behavior remain outside certification.

### Native WebSocket TLS and mutual TLS

The optional `websocket-tls` feature includes `websocket` and Tokio Rustls with
AWS-LC, matching the HTTP/gRPC TLS provider. `serve_websocket_tls` accepts the
ordinary listener, cancellation token and `WebSocketServerConfig`, plus an
`Arc<rustls::ServerConfig>`. Configure the certificate identity and a client
certificate verifier before calling the helper. The library does not load files,
choose trust roots, assign application identity or install a global crypto provider.
The Rustls/Tokio Rustls types are re-exported through
`transport::websocket::tokio_rustls` for host configuration.

TLS and plaintext listeners share connection admission, Origin policy, inbound
message/frame limits, modern discovery and session cleanup. Permits are acquired
before TLS acceptance. `handshake_timeout` covers TLS, HTTP upgrade and MCP discovery
as one operation; cancellation interrupts stalled handshakes and awaits active
sessions. TLS peer failures are contained to that connection.

`Client::websocket_tls(request, Arc<rustls::ClientConfig>)` uses explicit trust
and optional client certificates, preserves host request headers, and rejects
non-wss URLs before opening a socket. It shares the ordinary client's 4 MiB inbound
limit and ten-second TCP/TLS/WebSocket connect deadline. MCP discovery follows
through `Client::connect`. The host controls certificate verification and lifetime
through the supplied configuration; no trust bypass or plaintext fallback is added.

Five Rust-only loopback tests verify modern discovery and a typed tool call over
TLS/mTLS; reject unknown server trust, wrong hostname, plaintext, missing/untrusted
client identity and forbidden Origin; accept an allowed Origin; enforce inbound
frame/message limits, pre-TLS connection admission and TLS handshake deadline;
and check listener survival, active-session shutdown and interruption of a stalled
TLS handshake. They reuse the checked-in localhost test identities. The separate `tests/tls_interop.py` runner checks Go/Rust TLS and mTLS in both
directions. Proxies, revocation, rotation and production PKI remain outside these checks.

### Native gRPC TLS and mutual TLS

The opt-in `grpc-tls` feature includes `grpc` and Tonic's Rustls AWS-LC provider.
This matches the existing HTTP TLS provider; the library does not install a global
crypto provider. Hosts enabling multiple Rustls providers must select their process
provider before creating TLS clients or servers.
It exposes `serve_grpc_tls(server, listener, shutdown, identity, tls_config)` alongside
the plaintext convenience server. Both share tracked session admission and cleanup.
The identity is a required argument, avoiding Tonic's panic when a TLS configuration
omits it. Invalid PEM returns a configuration error before serving. For
host-composed servers, `GrpcService` remains available for Tonic TLS, interceptors
and message-limit configuration. `Client::grpc` accepts a TLS-configured Tonic
channel through the generated client, without a separate MCP TLS implementation.

`tests/grpc_tls.rs` uses only Rust clients and servers and checked-in test identities.
It verifies modern discovery and a typed tool call with server TLS and required
mutual TLS; rejects an unknown server CA, incorrect DNS name, plaintext connection,
missing client certificate and an untrusted client certificate; verifies the
listener still serves after rejections; and awaits active encrypted session cleanup
on shutdown. Malformed server identity fails before serving. The test identities are
valid from 2020 to 2120 and use localhost SAN with explicit server/client key usages.

Trust roots and identities are explicit caller inputs. The TLS helper does not
install a client CA, enable optional client authentication, bypass name verification,
load system roots or manage certificate rotation. Transport authentication does
not itself assign application identity or replace tool authorization. The separate cross-language runner now covers pinned Go/Rust loopback TLS/mTLS;
these checks do not certify proxies, revocation or production PKI.

`tests/network_interop.py` checks Rust clients against actual pinned Go servers,
and Go wire probes (Gorilla WebSocket and upstream-generated protobuf bindings)
against Rust servers. These probes are not a Go fluent-client certification.
They verify selected operations rather than full-message equivalence.

## Modern notification hub

`Server::with_subscription_hub` opts into the SDK 2026-07-28
`subscriptions/listen` contract. `SubscriptionHub` shares retained state across
server clones and LiveServer snapshots. It supports resource updates and the
three registry-list categories. Exact URI/template registration determines the
accepted URI subset; duplicates are deduplicated for initial hints and delivery.
Requested entries still count toward per-stream and aggregate memory limits.
Filtering does not execute a resource handler or imply permission to read it.

The SDK emits `notifications/subscriptions/acknowledged` before invoking the
framework listener. Framework middleware authorizes admission before allocating
watchers; requests/accepted filters are available via native context extensions
and decoded middleware arguments. An acknowledgment is preliminary and can be
followed by a policy/capacity error. Each successful admission sends initial
invalidations for accepted URIs/list categories. Clients read current snapshots
on these hints, closing the acknowledgment/registration gap without a replay log.
Applications needing endpoint authentication must still enforce it at transport
level, including discovery/lifecycle methods which bypass execution middleware.

Update publishing uses bounded try-send queues while holding only a short state
lock. A full queue marks and cancels that stream; it ends with an explicit
resynchronization error. SDK sends also have deadlines. Reports count accepted
queue entries rather than successful delivery; already in-flight events can
arrive during cancellation. Native client buffers can also lag: the framework
client reserves space for its initial hint set plus 64 events, while callers
using `peer.listen` directly own that SDK buffer choice.

Admission has the server's configured handler timeout, recovery, and cancellation
policy. Stream lifetime is not truncated by that timeout. Registration guards
release quotas on normal return, request cancellation, dropped futures, and peer
closure. The SDK intentionally drains an abrupt EOF for up to five seconds before
dropping its service guard; raw connections can retain subscriptions until then.
Explicit subscription cancellation and hub shutdown avoid that drain delay.
`hub.shutdown` waits for registrations; transport/runtime shutdown stays owned
by the host. No hub reader/writer background tasks or durable replay log exist.

Configuration snapshots keep existing subscriptions alive with their original
accepted resource grants. Changes to registry contents or authorization do not
automatically revoke those grants. To revoke access, close the old hub and
renegotiate. A registry change during negotiation is checked again before
admission. Existing direct per-peer notification methods are separate from
scoped hub publication.

The pinned modern Go handler accepts its `uris` filter shape and returns a JSON-RPC
result containing a generated subscriptionId. The Rust SDK expects a scoped
acknowledgment notification, followed by updates and a final completion on graceful
teardown, using the originating request ID. `tests/subscription_wire.py` sends
identical params containing both forms to record the deliberate incompatibility;
this is not an interoperability pass. SDK modern clients cannot consume the
pinned Go listen shape through the native SDK API.

The opt-in `client-go-subscriptions` feature supplies
`client::go_subscriptions::Subscription` for stateless Go HTTP endpoints. It
translates resource filters to `uris`, supplies required modern headers/metadata,
consumes the generated-ID response without pretending it is an SDK filter
acknowledgement, and returns typed notifications retaining Go metadata. Matching
IDs are mandatory; local filtering excludes unrequested categories, URIs, and task IDs.
`open_with_tasks` sends Go's `notifications.taskIds` filter; callers must advertise
the tasks extension in `Config::capabilities`. The adapter explicitly decodes
known task events into typed task notifications; malformed payloads cannot fall
back to custom notifications. Task IDs and resource URIs each permit 1,000 entries
of up to 4,096 bytes; duplicates count and all filters share the request-size bound.
Opening, encoded requests, JSON rejection bodies, and SSE events are bounded.
Malformed events and EOF terminate the stream with one error. No detached reader,
automatic reconnect, or replay is created; dropping/closing releases the body.

Go provides no initial refresh hint or accepted filter; read snapshots after
opening. The pinned application is checked for real negotiation, and its actual
HTTP transport is separately checked with a deterministic handler for publication
via `NotifySubscription`, ID isolation, and disconnect hooks. This does not certify
automatic resource publication by the Go application, stateful Go
HTTP mode, or reverse Go-client/Rust-server subscription adaptation. See
[go-subscription-interop-report.json](go-subscription-interop-report.json).

The real Go application task manager/HTTP transport is additionally checked for
post-open working snapshots with matching task IDs, selected completion and failure
payloads, cancellation, rejection when the tasks capability is absent, and exclusion
of an unrequested task. A real task elicitation produces `input_required` with a
keyed `inputRequests` map; the probe supplies the named response via `tasks/update`
and verifies the resumed completion payload. Deterministic gates keep these
transitions after stream admission. Local stream fixtures additionally cover all
five typed task statuses, malformed known payloads, opaque IDs, cross-stream
rejection, and bounds. This is selected form-input coverage, not certification of
every input kind, transition, malformed update, or Go retention policy.
The probe also verifies that empty and unknown-key updates leave pending input
unchanged. Unknown IDs for get/update/cancel produce an HTTP JSON-RPC `-32603`
error in pinned Go, with `-32602` embedded in the message; the Rust SDK returns
`-32602` directly. This difference is retained explicitly, not claimed as parity.
Retired subscribe/unsubscribe methods are rejected. Native Rust task-ID
subscriptions now use the owned engine's `SubscriptionFilter::task_ids` builders
and scoped task sink. Both peers must advertise the tasks extension. After
middleware admission, the shared manager atomically registers a bounded receiver
and captures initial detailed snapshots for distinct requested IDs. Matching
message/status/input/result/error changes are ordered after those snapshots.
Unknown IDs fail the entire registration; oversized filters, missing task support,
queue overflow, sink/client filter rejection, cancellation, and manager/hub
shutdown are covered by local checks. Task and resource queues are separately
bounded. Task managers and authorization remain host-owned; no per-user ownership
is inferred. TTL expiry remains opportunistic. Reverse Go-client/Rust-server
adaptation and cross-language task-subscription certification remain separate work.

The pinned Go `ServeHTTP` wires task notifications only when the server enables
`Capabilities.ResourceSubscribe`; the real task fixture enables that setting.
A task listen acknowledgement alone does not prove the server has a publisher.

## Request-aware visibility contract

`Server::with_visibility_policy` installs one shared, synchronous predicate over
`Context` and `VisibilityTarget::{Tool, Resource, Prompt}`. It replaces any prior
predicate using the existing copy-on-write configuration. No extra feature or
runtime dependency is required. SDK method capabilities and host-side schema
lookups remain configuration-level; they are not user-facing registry lists.

Filtering precedes pagination for tools, static resources, resource templates,
and prompts. Hidden direct calls/reads/gets return the same framework error as
missing entries, before input/required-argument validation. Static exact matches
retain priority even when denied. Resource targets expose the concrete requested
URI and registered URI/name separately, allowing both registration-level and
per-URI policy. Lists use the registered URI as their target. Completion gates
registered prompt/resource references when a policy is installed; no-policy
completion retains the previous generic-reference behavior. Completion callbacks
remain responsible for authorizing individual suggestions for allowed references;
the framework does not infer concrete URIs from arbitrary completion values.

Resource reads and completion revalidate policy after middleware rewrites their
URI/reference. Policy panics yield opaque internal errors and no user callback is
run. Predicates must be quick/nonblocking; synchronous callbacks cannot be
preempted by asynchronous deadlines. Verified identity must be present before
framework dispatch, not injected in downstream application middleware. SDK HTTP
request Parts retain host-added Axum request extensions. Client metadata and
clientInfo remain untrusted.

Resource subscription policy runs during admission before hub registration.
The SDK accepted-filter callback has no request context: with a policy installed,
it retains all valid requested URIs rather than exposing registry existence.
Hidden and unknown URIs then fail with the same admission error. Existing grants
survive policy/registry changes; close the hub and renegotiate to revoke them.
Capabilities, preliminary acknowledgements, and list-change invalidations are not
proof that an object can be accessed. Task polling/update/cancel and other SDK
administrative methods remain host-authorized separately.

Live registry snapshots include policy; in-flight work keeps the old snapshot.
Mutable state captured by a policy is host-owned. Pagination/cursors do not pin a
policy or caller across requests. Visibility changes require host notifications
for clients to refresh lists. This implementation provides no identity provider,
policy cache, automatic revocation, or tenant-aware task manager.

The pinned Go fixture compares filtered lists/pagination, hidden direct access,
and visible tool/resource success via its actual WithToolFilter/WithResourceFilter/
WithPromptFilter options. It retains each peer's existing unknown-object error
codes and does not certify completion or subscription parity. See
[visibility-interop-report.json](visibility-interop-report.json).


## Server metadata

Server configuration stores one native SDK `Implementation` plus optional
instructions. `with_server_info` replaces identity without replacing method
registrations or derived capabilities. Individual fluent setters support title,
description, website URI, icons, and instructions. Empty strings clear the
individual optional string fields; an empty icon iterator clears the collection.
The native identity setter retains its optional values as supplied. Defaults
continue to emit only name/version. No URI fetching or validation is performed.

Discovery uses `_meta.io.modelcontextprotocol/serverInfo`
and preserves instructions. Live replacement changes future discovery replies;
it does not rewrite prior replies or automatically refresh client-side peer info.
Existing static clones/snapshots retain their identity/instructions, while
capabilities still reflect registered methods.

Build information is configured separately with `with_build_info(BuildInfo)` and
cleared with `with_build_info(None)`. Discovery extends the actual identity object
at `_meta["io.modelcontextprotocol/serverInfo"]["buildInfo"]`, using upstream's
`commit` and `buildDate` field names. Empty fields are omitted, and no details are
inferred. Replacing native identity preserves this separate extension. The SDK's
closed `Implementation` view still omits it; `ServerMetadata` retains it.

The pinned Go comparison checks title, description, website, multiline
instructions, and identical build information at 2026-07-28. It also runs the
actual typed Rust client against both server processes, checking the handshake
snapshot and fresh discovery. `Client::server_metadata`,
`ServerMetadata::from_peer_info`, and `ServerMetadata::from_discovery` decode
both modern size encodings: SDK arrays and Go whitespace-separated strings.
Normalization affects only an owned view, preserving raw discovery JSON. Unknown
extensions are ignored; malformed known fields return errors instead of silently
losing the whole identity. Missing identity is distinct from malformed identity.

Outgoing icons retain the native SDK schema (`src`, array-valued `sizes`, optional
theme). Go emits string-valued sizes and additionally duplicates the identity at
discovery's top level. Rust emits the namespaced metadata identity only. These
outbound differences remain; this is not full bidirectional icon certification.
Direct native SDK identity accessors cannot parse Go string sizes; use the
framework metadata reader. Already connected clients retain their handshake
snapshot until the caller explicitly requests fresh discovery. No initialize or
older protocol path is added. See
[server-metadata-interop-report.json](server-metadata-interop-report.json).

## Static server cards and catalogs

The typed card builder and opt-in HTTP routes port a bounded subset of pinned
Go's experimental SEP-2127 helper. Remote advertisements are restricted to
Streamable HTTP / MCP 2026-07-28; older remote types and discovery versions are
excluded. The card constructor copies native identity metadata, normalizes the
name, and truncates the description by Unicode characters rather than Go's byte
slice. Public fields remain host-editable; this is not a schema validator.
Outgoing icons retain SDK array-valued sizes, as described for server metadata.
Optional Rust values explicitly set to empty/false may remain present, whereas
Go's `omitempty` removes them.

`transport::server_card::router` returns routes to merge with the normal MCP
router. Card and catalog bodies are serialized once into immutable snapshots.
Their ETag uses the first 16 SHA256 bytes of that document's exact JSON encoding;
Go/Rust validators need not match when encoding differs. GET/HEAD and exact
conditional requests are checked against both real processes. Local HTTP tests
also check changed snapshots, weak/list/wildcard validators, aliases, static
catalog URLs under hostile request headers, and successful modern discovery
through the merged router. Rust accepts weak/list/wildcard If-None-Match and
advertises GET, HEAD for CORS; pinned Go uses exact ETag matching and advertises
GET. Neither helper installs an OPTIONS preflight handler.

Rust requires an explicit card URL and catalog identifier; it does not derive
these from Host or forwarded headers. The fixture comparison normalizes Go's
derived absolute URL to the configured relative Rust URL, and compares the
remaining JSON directly. This does not certify the complete extension schema,
proxy/TLS behavior, browser behavior, or icon equivalence. Public documents are
advisory; applications own metadata accuracy and access policy. Live MCP
capabilities remain authoritative. Skills helpers, progressive discovery,
and webhooks are described separately below.

## Skills extension

Rust serves validated immutable bundles, optionally loaded from directories, through ordinary `skill://`
resources and the SEP-2640 index. The extension is experimental. Unlike pinned
Go directory loaders, resource contents do not change with the filesystem.
Rust validates restricted ASCII paths, exact frontmatter fences, duplicate
metadata and bounded file counts/sizes. Go accepts a broader path set and its
minimal frontmatter parser differs on malformed input. Both use a limited
frontmatter subset, not general YAML. MIME types are explicit for manually
constructed Rust bundles; directory loading uses the pinned Go extension mapping.
Text MIME types require UTF-8.

The Rust index applies entrypoint visibility; Go's index handler does not filter
entries per request. Supporting files still require their own resource visibility
checks. Removing any bundle resource removes that entire bundle, preventing
dangling index entries. Resource ownership is tracked explicitly so unrelated
resources under the same URI prefix survive removal. Archive resources and
parameterized skill templates are supported.

Directory loading snapshots file contents before registration; Go reads registered
files on each request. Rust rejects symlinks and special entries, limits traversal
and byte/count totals, and performs multi-skill registration atomically. Go's
`SkillsFromDir` may leave earlier skills registered when a later skill fails.
Rust ignores ordinary root files and directories without `SKILL.md`, but rejects
symlinks/special entries even at the collection root. Directories must remain
stable and host-owned while loading; metadata checks are not a race-proof sandbox.


Packed skill archives use the Go URI suffixes and MIME types: `.zip` and
`.tar.gz`, with `.tgz` normalized to `.tar.gz`. Both implementations serve opaque
binary bytes and rely on callers for archive contents and matching root
`SKILL.md`; neither loader validates or extracts archive members. Rust snapshots
at most 8 MiB of packed bytes and rejects symlinks/special files, while Go reads
from disk on each request. Rust does not read a sibling `SKILL.md` implicitly;
use `SkillArchive::with_description` to supply metadata. Go optionally takes a
sibling frontmatter description and rejects a successfully parsed mismatched name.
Rust defaults to the same `Archived skill {name}` description. Namespace ownership
is unique across bundles and archives; a duplicate fails atomically instead of
replacing index metadata or leaving multiple representations behind. No decompressed
size or extraction guarantees are provided because archives are never opened.

The Rust catalog uses deterministic namespace ordering; Go preserves registration
order. The archive/directory comparison fixture deliberately registers Go entries
in Rust's order before comparing the full catalog. This does not establish
equivalent ordering for arbitrary registration sequences.


Skill templates register a dynamic Markdown resource and a nameless
`mcp-resource-template` catalog entry atomically. Rust accepts only a
`skill://.../SKILL.md` URI with unique, whole-segment named variables and literal
ASCII path segments; operators and query/fragment/escape syntax are unsupported.
The Go builder checks only the scheme, a variable opener and a nonempty description
before ordinary resource compilation, accepting a broader template set. Rust
requires a handler before catalog mutation; Go adds index metadata when acquiring
the builder. Both preserve raw parameters without percent decoding. Concrete
content is handler-owned and not parsed as frontmatter. Rust's Markdown convenience
handler fills the MIME type; native content handlers preserve individual block types.

Template catalog visibility uses the exact registered template, avoiding accidental
selection of an overlapping template matcher. Reads retain the ordinary exact-first,
then deterministic-template routing and do not fall through after a visibility
denial. Catalog visibility does not certify access to all concrete instantiations;
use `registered_uri` to deny a whole template, and `uri` for per-instance policy.
Removing the registered template URI removes its catalog entry and resource, while
unrelated static overrides survive. The bounded pinned Go comparison covers template
listing, nameless index metadata and two concrete text reads; malformed syntax,
completion, subscription and policy parity are not certified by that comparison.


Progressive discovery uses standard `tools/list` in the owned engine and the
host helper `Server::discover_tools`. Registration supports `group`, replacing
`tags`, and `defer_schema`. Selection follows the previously checked pinned Go
semantics: exact nonempty group, AND tags, name ordering, filtering before
100-entry pagination, absent detail respecting per-tool deferral, `names`
omitting both schemas and `full` restoring them. Title, annotations, icons and
metadata are preserved. Visibility evaluates the caller's context and fails
closed; filters are not authorization. Stored `Tool` definitions retain complete
input schemas and call validation remains authoritative.

The typed wire query is `ListToolsRequestParams`, with ordinary `_meta` support.
Unlike Go's raw parser, empty-string detail and unknown fields are rejected.
Malformed standard list parameters return invalid-params errors rather than
falling through to custom handlers. The optional-envelope decoder now rejects
malformed parameter objects instead of treating them as absent. Listed entries
use `ListedTool` (also exported as `DiscoveredTool`) with optional schemas;
`ToolListDetail` is also exported as the host helper's `ToolDiscoveryDetail`.
`PaginatedRequestParams` still serves resources and prompts; callers migrating
explicit tool pagination should use `ListToolsRequestParams` or `.into()`.

The typed client cache includes cursor, group, tags and detail, excluding request
metadata. `list_all_tools_with` preserves the query across pages. Full HTTP
listings teach the client `x-mcp-header` schemas; names-only/deferred entries are
retained and do not replace a previously learned schema. Fetch full detail before
calling an annotated tool whose schema has not been learned. Discovery does not
provide a per-tool schema revision or a dedicated schema-fetch method.

The host helper shares selection but returns catalog data without protocol cache
hints. If exposed as a tool, ordinary tool middleware applies. A captured `Server`
retains its configuration snapshot. `LiveServer` takes one atomic current snapshot
per request; retain `live.snapshot()` for stable pagination across replacements.
Cursors are names from the visible filtered set, not signed tokens. Keep snapshot,
identity and filters stable across pages.

Local standard-wire checks cover filtering, schema omission/restoration, metadata,
filtered pagination, typed page collection, live replacement, visibility denial
and panic, malformed queries, continued call validation, query-isolated cache hits,
and deferred HTTP discovery with annotated routing. The pinned Go/Rust stdio
runner additionally verifies 13 catalog queries, seven invalid queries/cursors,
filtered 100-entry pagination, schema presence and deferred call validation.
It confirms Rust's stricter empty-detail/unknown-field rejection. Generated
schema content, exact error wording, HTTP cache parity and concurrent mutation
are outside that comparison; see [discovery-interop-report.json](discovery-interop-report.json).
These checks are not exhaustive cross-language certification.


The optional `webhooks` feature provides async `WebhookNotifier`, a channel-message
wrapper, and object-safe `NotificationSender`/`MultiNotifier` fan-out. Outbound
JSON-RPC envelopes, explicit null parameters, `Mcp-Method`, content type, custom
headers and success/error status classification match selected pinned Go checks.
Channel messages use Go's `channel`/`message` keys. These helpers do not register
subscriptions, alter live transport sinks or advertise standardized webhook support.

Rust defaults to a ten-second response-header deadline and bounded 1 MiB streaming
serialization. Parameters/envelopes that exceed the limit fail before sending.
Go has no payload bound and drains response bodies; Rust drops bodies without
buffering or waiting for EOF. Rust's default client disables redirects and retries;
Go's standard client follows redirects. Rust accepts HTTP(S) endpoints without URL
credentials/fragments and rejects reserved framing headers, instead of Go's broader
URL/header acceptance. Authentication headers are caller-owned. Host-selected
endpoints can include local addresses; the host must enforce its own destination
admission policy. TLS certificate verification uses Rustls/platform trust by
default; custom clients control TLS, proxy, redirect and internal retry policy.
The helper adds no retries, signing, token validation or durable storage. HTTP
error URLs and configuration debug output omit endpoint/query/header details.

Fan-out is sequential and continues after returned errors. Rust retains typed
failures with zero-based target indices rather than joining error strings. Targets
cannot be null; an empty collection succeeds. Dropping the future cancels pending
work and skips remaining targets without undoing any already accepted delivery.
Panics from custom senders are not converted into delivery errors.

The local regression suite checks framing, custom headers, 2xx/non-2xx statuses,
redirect suppression, payload boundaries, serialization failures, deadlines,
unbounded response bodies, URL redaction, fan-out failure aggregation and future
cancellation. The differential fixture compares real Go and Rust POSTs for channel,
resource-update and null-parameter notifications, 202/204 acceptance and 503
rejection. It does not certify TLS, endpoint authentication, fault-induced retries,
complete notification vocabulary or an eventual webhook SEP.


## HTTP session-store scope

The framework HTTP router explicitly sets `legacy_session_mode = false` and uses
rmcp 3.5.1's `NeverSessionManager`. Modern requests carry their own protocol/client
metadata and do not allocate or restore transport sessions. GET and DELETE on
`/mcp` return 405 with `Allow: POST`; the router has no event store for resumable
GET streams. Request-scoped SSE responses to POST remain supported.

The SDK `SessionStore` persists `SessionState.initialize_params` and restores a
session by replaying `initialize` and `notifications/initialized`. That is a legacy
protocol path, excluded from this port. The pinned Go `transport.SessionStore`
and `examples/stores/redis.go` provide opaque session bytes; they do not establish
modern shared task or application-state persistence. No Redis session adapter is
added for the retired handshake.

`tests/http_routing.rs` sends self-contained tool requests alternately to two
independent routers and verifies successful results, absent session-ID response
headers and disabled GET/DELETE session endpoints. This proves request routing
without session affinity for that handler. It does not prove failover of in-flight
streams, tasks, subscriptions, rate limits, caches or other application state.
These remain process-local unless the host supplies a shared implementation;
durable SSE event replay is a separate SDK `EventStore` extension point.

## Native HTTP host policy

`HttpServerConfig`, `http_router_with_config` and `serve_http_with_config` expose
host/Origin admission, streaming POST size limits and JSON response preference.
These native Rust helpers configure the existing SDK stateless service. The
framework configuration excludes legacy session and session-store settings;
`NeverSessionManager`, modern protocol enforcement and shutdown wiring remain in
effect. Default helpers delegate with the existing SDK defaults.

An absent Origin policy disables Origin validation, while an explicit empty list
rejects present Origin headers. Missing Origin headers remain permitted. Explicit
ports preserve SDK authority/origin matching; TLS termination, endpoint identity
and authentication belong to the host. JSON preference retains the SDK SSE
fallback when the handler emits a notification or request before its final result.

Native loopback checks cover configured hostname/port and scheme/origin rejection
before dispatch, default HTTPS ports, absent Origin, empty versus disabled Origin
validation, fixed-length and chunked oversized requests, a zero-byte limit,
continued service after rejection, modern-only endpoints, JSON success, progress
notification ordering through SSE fallback and active-request cancellation.
These checks do not certify proxy rewriting, CORS preflight or production TLS.

## gRPC envelope validation

Native gRPC decoding rejects requests/notifications carrying response fields,
notifications with correlation IDs, missing methods, responses carrying method or
params fields, and responses carrying both result and error. Unknown message types,
missing request IDs, malformed JSON and unsolicited/duplicate response IDs remain
errors. A receive error terminates that peer's stream; it does not dispatch a tool
or stop the listener or other sessions. The common protobuf metadata map remains
ignored and is not interpreted as an authentication claim.

Response correlation commits only after envelope and JSON payload validation.
Malformed result or error-data JSON therefore leaves the pending ID intact inside
the codec, while the public transport closes the invalid stream. Valid error data
and opaque string/numeric request ID restoration remain supported. Empty result
bytes still decode as `{}`, preserving the pinned protobuf convention.

Unit checks cover contradictory fields, correlation preservation, successful/error
response completion and duplicate response rejection. A native loopback check sends
five malformed envelopes after successful modern discovery and verifies zero tool
dispatches from those streams plus continued use of an existing healthy client.
This is stricter malformed-frame admission, not a change to the protobuf schema or
support for older protocol revisions.

## First-release transport conformance

`tests/transport_conformance.rs` runs one shared modern extension workflow over
framed JSON streams, stateless HTTP, WebSocket and gRPC. It exercises concurrent
filtered schema-detail pagination, validation despite deferred schemas, prompt
MRTR continuation, cache reuse and notification invalidation, scoped task status
snapshots/results, live replacement and graceful subscription completion. These
checks complement the deeper lifecycle and retention suites rather than asserting
identical Go/Rust experimental extension schemas.

The workflow found and fixes a gRPC embedded-ID mismatch: protobuf request IDs
are strings, while native clients may use numeric or string JSON-RPC IDs. The
codec now restores known local subscription references in acknowledgments, updates,
server cancellation and terminal responses. It preserves peer-owned opaque IDs,
including strings that resemble the codec's type tags, and preserves ordinary
request metadata and unrelated IDs. Client cancellation tags the local ID; server
cancellation preserves the peer's opaque subscription ID. Correlation remains
bounded by 1024 outstanding requests. Cancellation releases its slot immediately;
a separate bounded set of 1024 retired IDs accepts one validated late response,
then rejects duplicates. Replies older than that retention window are rejected.
Terminal responses and transport close also clear correlation. Four focused codec
regressions cover numeric/string separation, lookalike IDs, unrelated requests,
terminal cleanup, repeated cancellation, bounded retention and late replies.

`tests/tls_interop.py` adds 36 cases across WebSocket/gRPC, TLS/mTLS and both
Go/Rust directions. It checks valid discovery/tool/error/resource operations,
unknown server trust, wrong hostname, missing/untrusted client certificates,
listener survival after rejected peers and graceful server shutdown. Test keys
are public loopback fixtures. This provides cross-language TLS interoperability
evidence, not certificate revocation/rotation or production PKI qualification.
See [tls-interop-report.json](tls-interop-report.json).

The complete reproducible local acceptance gate is `tests/conformance.py`; its
scope and distribution boundary are recorded in
[release-readiness.md](release-readiness.md). Optional Go adapters remain separate
from native behavior. Legacy protocol support is still excluded.
