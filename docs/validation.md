# Validation — 2026-10-24

Scope is MCP **2026-07-28 through discovery only**. Older protocol compatibility
is excluded. Local checks use Rust 1.97.0 on macOS; the minimum compiler is 1.89.0.
Cargo.lock resolves the in-repository `mcp-protocol` engine. There is no registry
`rmcp` or `rmcp-macros` dependency. This section records the local acceptance
snapshot before GitHub setup; current hosted evidence is linked from
[repository setup](repository-setup.md).
Builds use a generated staging cache, `CARGO_INCREMENTAL=0`, offline locked Cargo
commands, and at most `-j 4`.

## Completed modern first-release acceptance — 2026-10-24

The final unchanged-source run of `tests/conformance.py` passed all **51 checks**:
formatting, both Clippy configurations with warnings denied, documentation with
warnings denied, Rust 1.89 all-target checks, resolved engine independence,
22 feature configurations, **412 tests across 46 suites**, all examples, native
smoke/typed stdio checks, all 18 pinned Go fixture runners and typed Go stdio.
No test was ignored. The source fingerprint and individual check results are in
[acceptance-report.json](acceptance-report.json).

New evidence includes one shared workflow across framed JSON streams, HTTP,
WebSocket and gRPC, and **36 bidirectional TLS/mTLS interoperability cases**.
The workflow exposed a real gRPC subscription correlation failure. The codec now
restores embedded subscription references while preserving opaque peer IDs.
Cancellation releases pending slots, retaining at most 1024 retired IDs for one
validated late reply each. Four codec regressions cover reference restoration,
opaque IDs, cancellation admission, bounded retention and late/duplicate replies.

The first complete gate ran while these fixes were being edited. Its individual
checks passed, but its source-change guard correctly rejected acceptance. The
reported final run repeated every check with the implementation held unchanged.
The checked-in CI invokes the same pinned interoperability group and retains logs.
At the time of that acceptance snapshot, hosted CI and publication had not run.
Current registry packaging and release checks are documented in
[publishing](publishing.md).
See [release-readiness.md](release-readiness.md) for the supported scope and
intentional differences. Go remains a test reference, not a runtime dependency.

## Overflow-safe SSE retry accounting — 2026-10-24

The exponential backoff now saturates its `usize` to `u32` exponent conversion
instead of truncating large counts. Reconnect failure counting also saturates
instead of overflowing. This prevents a public policy query beyond `u32::MAX`
from restarting at the base delay and prevents a saturated state-machine counter
from panicking or reopening an exhausted budget. Ordinary delays, optional delay
caps, duration saturation, zero delays and configured attempt limits retain their
behavior. The locked Tokio timer already handles unrepresentable deadlines; no
new timer cap or dependency change is introduced.

Three boundary tests cover the uncapped plateau beyond 32-bit counts on 64-bit
targets, capped/zero/saturated durations and attempt limits, and failure handling
at `usize::MAX`. Removing the fixes makes all three fail on the local 64-bit target;
the two selected existing retry-policy controls still pass.

Validation passed: all **404 tests across 45 suites**. The five selected boundary/
retry-policy tests also pass with only client and HTTP client transport features
enabled. Workspace/all-target/all-feature and minimal-build Clippy passed with
warnings denied, along with documentation warnings, Rust 1.89 checks, model/client/
server/HTTP client/HTTP routing and optional Go adapter checks, and formatting.
The first full build exhausted disk during linking; after removing 3.63 GiB of
generated staging-cache Rust objects with all builds stopped, the complete suite
passed at `-j 2`. Source files and dependency locks were preserved.

## Consistent SSE reconnect policy and pacing — 2026-10-24

Recoverable stream errors and graceful EOF now consult the retry policy before
scheduling the first reconnect. A server retry hint cannot enable a disabled
policy or shorten its local delay. The latest hint acts as a persistent minimum
across successful reconnections rather than being consumed by the first EOF.
Errors excluded from reconnection pass through the existing fatal-error mapper;
completed responses, oversized events and requests without a resumable event ID
keep their terminal behavior. Dropping the waiting stream drops its timer before
the GET is created. Retry budgets continue to count consecutive connection
failures within an outage, not total attempts across successful reconnects.

Four paused-clock tests check both EOF and body errors with local/server delay
precedence, disabled policies on common and request streams, hint persistence,
and abandonment during a retry wait. Restoring the previous implementation makes
all four fail while the six existing decoder/reconnection controls still pass.

Validation passed: all **401 tests across 45 suites**. All ten decoder/reconnection
tests also pass with only client and HTTP client transport features enabled.
Workspace/all-target/all-feature and minimal-build Clippy passed with warnings
denied, along with documentation warnings, Rust 1.89 checks, model/client/server/
HTTP client/HTTP routing and optional Go adapter feature checks, and formatting.

## Cooperative SSE byte parsing — 2026-10-24

The built-in SSE byte wrapper splits body chunks into at most 16 KiB zero-copy
slices, then self-wakes and returns pending after 32 slices (including empty
chunks). The counter persists while the downstream parser consumes chunks, so
continuous comments and incomplete events also yield before producing an SSE
event. Limiter/parser work is bounded per slice, and event-size accounting survives
all splits. A pending `Bytes` tail may retain the original backend allocation;
custom backend polling must still be nonblocking.

Four tests cover continuous comments/empty chunks/partial data with a finite
regression safety guard, a single large chunk with a comment prefix plus UTF-8/CRLF
and ordered events, event-size rejection across slices, and source errors after a
yield. Removing byte slicing and cooperative accounting makes three tests fail;
the event-size rejection control continues to pass.

Validation passed: all **397 tests across 45 suites**. The four byte parsing tests
also pass with only client and HTTP client transport features enabled. Workspace/
all-target/all-feature and minimal-build Clippy passed with warnings denied, along
with documentation warnings, Rust 1.89 checks, model/client/server/HTTP client/
HTTP routing and optional Go adapter feature checks, and formatting.

## Cooperative SSE decoder polling — 2026-10-24

The JSON-RPC SSE auto-reconnect decoder now limits each poll to 32 internal steps,
then wakes itself and returns pending. Ready control frames, data-less events,
malformed JSON and state transitions cannot consume an unlimited number of steps
in a single decoder poll. Frames are not discarded by the work limit, and message
order, event IDs, retry state and completion tracking persist across yields. This
is separate from the event byte limit and response-drain count/time budgets;
custom backends and raw byte parsing still have their own polling behavior.

Two tests cover three continuous ignored-frame kinds with a finite regression
safety guard, explicit self-waking and per-poll accounting, plus delivery and
last-event-ID resumption across a yield. Removing the work limit makes both new
tests fail while the four existing reconnection tests continue to pass.

Validation passed: all **393 tests across 45 suites**. All six decoder/reconnection
tests also pass with only client and HTTP client transport features enabled.
Workspace/all-target/all-feature and minimal-build Clippy passed with warnings
denied, along with documentation warnings, Rust 1.89 checks, model/client/server/
HTTP client/HTTP routing and optional Go adapter feature checks, and formatting.

## Request SSE reconnection stops at completion — 2026-10-24

The request-scoped auto-reconnect decoder now records a final JSON-RPC response or
error. It keeps the current response body readable for bounded connection reuse
draining, but EOF and body errors after completion terminate the decoder instead
of opening a new GET. This holds even when an event ID and zero retry interval
would previously trigger resumption during cleanup. Incomplete requests still
resume with their last event ID; common streams keep their retry behavior even
when they carry responses/errors. There is no public API or wire-format change.

Four paused-clock tests record reconnect invocations and event IDs: successful and
error completion followed by EOF/body failure, preservation of the existing body
tail, pre-completion resumption, and independent common-stream resumption. Reverting
the completion tracking makes the first two tests fail, while the resumption
controls continue to pass.

Validation passed: all **391 tests across 45 suites**. The four new decoder tests
also pass with only client and HTTP client transport features enabled. Workspace/
all-target/all-feature and minimal-build Clippy passed with warnings denied, along
with documentation warnings, Rust 1.89 checks, model/client/server/HTTP client/
HTTP routing and optional Go adapter feature checks, and formatting.

## Bounded final-response SSE draining — 2026-10-24

Final-response connection reuse draining now stops after at most 32 trailing
messages as well as the existing 50 ms timeout. It yields after each message so an
immediately ready stream does not monopolize a runtime poll, and stops at the first
stream error instead of polling beyond it. EOF still permits normal connection
reuse; exhausted budgets drop the stream without a reuse guarantee. Trailing data
is discarded, and the final response already sent to the handler remains unchanged.
This does not change common-stream delivery or introduce a public configuration
surface. Per-event byte bounds remain separate, and a backend must not block inside
its own `poll_next` implementation.

Four new tests cover an inexhaustible ready tail with a finite regression safety
guard, cancellation triggered by a ready tail, immediate stop at a stream error,
and a short tail reaching EOF. Restoring the previous drain makes the first three
fail promptly instead of hanging the executor; short-tail connection reuse still
passes. Existing cancellation, blocked delivery, timeout and adapter tests remain.

Validation passed: all **387 tests across 45 suites**. All ten stream lifetime tests
also pass with only client and reqwest transport features enabled, without `local`.
Workspace/all-target/all-feature and minimal-build Clippy passed with warnings
denied, along with documentation warnings, Rust 1.89 checks, model/client/server/
HTTP client/HTTP routing and optional Go adapter feature checks, and formatting.

## SSE cancellation during delivery and draining — 2026-10-24

The shared HTTP SSE reader now wraps its complete read/deliver/drain operation in a
biased stream-token cancellation boundary. Cancellation interrupts blocked handler
queue delivery and the final-response connection reuse drain, including common
notification streams and direct transport requests without caller metadata. It
adds no watcher task and leaves sibling and parent tokens active. Uncancelled final
responses still reach the handler and drain for at most 50 ms. Optional adapter
request cancellation continues to retain its stream until the existing lifetime
ends, as required for its cancellation POST ordering.

Three new paused-clock tests cover full handler queues for request and common
streams, immediate terminal-drain cancellation without time advancement, and the
unchanged 50 ms uncancelled drain. Restoring the previous SSE reader makes the two
cancellation tests fail while the bounded-drain test passes.

Validation passed: all **383 tests across 45 suites**. All six HTTP stream lifetime
tests also pass with only client and reqwest transport features enabled, without
`local`. Workspace/all-target/all-feature and minimal-build Clippy passed with
warnings denied, along with documentation warnings, Rust 1.89 checks, model/client/
server/HTTP client/HTTP routing and optional Go adapter feature checks, and formatting.

## Native HTTP response stream caller lifetime — 2026-10-24

Native HTTP response streams now observe the original service request lifetime,
including after the request POST has been acknowledged. Request extensions carry a
local cancellation token without adding wire fields or watcher tasks. Abandonment
interrupts both stream reads and blocked delivery to the handler, marks the request
cancelled for stream-completion cleanup, and releases the response stream. Default
transport send cancellation rules are unchanged. Common notification streams and
sibling requests are independent. Explicit Go adapters retain their previous
stream/cancellation POST ordering; direct transport sends without service metadata
keep the existing lifetime behavior. No remote termination guarantee is added.

Four new tests cover caller lifetime propagation after completed sends, sibling
isolation and metadata serialization; native stream destruction before/after first
poll; a stream blocked on a full handler channel; and the optional adapter retaining
its stream until its existing lifetime ends. Removing lifetime propagation and the
native cancellation branch makes three regressions fail while the adapter case
continues to pass. The fix restores all four.

Validation passed: all **380 tests across 45 suites**. The focused lifetime tests
also pass with only client and reqwest HTTP transport features enabled, without the
`local` feature. Workspace/all-target/all-feature and minimal-build Clippy passed
with warnings denied, along with documentation warnings, Rust 1.89 checks,
model/client/server/HTTP client/HTTP routing and optional Go adapter feature checks,
and formatting.

## Pending response and send lifetimes — 2026-10-24

The service's pending response table is now shared with the request cleanup guard.
Dropping a handle/response future, explicit cancellation and response deadlines
remove the original pending sender as well as local progress/subscription routing.
Queued requests whose lifetime ended or response receiver closed are skipped before
transport dispatch. Admission and drop removal use the same short mutex to prevent
late insertion after cleanup. Removed senders are delivered/dropped outside the
lock; request association uses a snapshot without keeping the lock during policy
checks. Service shutdown clears the table; an exit guard also clears it on abort
or unwind, even if peer clones survive.

In-flight request sends are interrupted only for transports opting in through
`Transport::supports_request_send_cancellation`, which defaults to false. Opt-in
requires cancellation safety for connection/framing reuse and request work cleanup.
Worker transports forward their worker's existing request-cancellation capability,
including native HTTP. Default/custom transports retain in-flight sends through
completion or shutdown. Cleanup does not itself send a cancellation notification
or guarantee remote termination. Completed sends and response ID normalization are
unchanged; ongoing requests remain subject to host admission limits.

Seven new paused-clock service-loop tests cover queued-drop suppression, dropped
response futures with sibling isolation and late responses, association retirement,
all three response timeout policies against stalled request/cancellation sends,
success/error responses with numeric-string IDs, shutdown with surviving peer
clones, service-task abort, and the non-cancellable transport default. The controlled
transport records send invocation and future destruction separately. Against the
original implementation, with new shared-table-only assertions omitted, three
transport-observable regressions fail: queued requests are dispatched and dropped
or timed-out request sends remain pending. All seven pass with the fix restored.

Validation passed: all **376 tests across 45 suites**; the seven new tests also
pass in a standalone client-only engine build without the `local` feature, exercising
both service task implementations. Workspace/all-target/all-feature and minimal-build
Clippy passed with warnings denied, as did documentation with warnings denied,
Rust 1.89 minimum-compiler checks, model/client/server/HTTP client/HTTP routing and
optional Go subscription adapter feature checks, and formatting.

## Request routing cleanup on drop and transport closure — 2026-10-24

Request handles now own a private field guard retaining their original request ID
and optional progress token. Dropping a handle, dropping an unpolled or pending
response future, and all response-return/error paths remove local progress watchers
and subscription routing entries. Public fields remain movable, and mutating a
public ID/token cannot redirect guard cleanup to another live request. Explicit
cancellation still releases local state before awaiting notification delivery.

Progress watchers now use an internal synchronous mutex around short map operations;
no lock is held across callbacks, awaits or notification delivery. Cleanup does not
spawn a task or require a running Tokio runtime. At this milestone, dropping a
handle did not prune the service loop's pending responder pool; the pending-response
lifetime milestone above closes that gap. Existing subscription-specific
cancellation behavior is retained.

Five new engine tests cover raw/unpolled future drops after runtime shutdown,
pending-future drops with changed public IDs/tokens and sibling request isolation,
transport closure across all response-wait branches, successful/error responses,
and explicit cancellation cleanup before transport acknowledgment. With the original
implementation restored, the three drop/transport tests fail on retained routing;
the successful/error response and explicit-cancel tests already pass. All five pass
with the guard restored. Existing timeout and subscription tests remain applicable.

Validation passed: all **369 tests across 45 suites**, including five new engine
cleanup tests; standalone engine request tests; full workspace/all-target/all-feature
and minimal-build Clippy with warnings denied; documentation with warnings denied;
Rust 1.89 minimum-compiler checks; model-only, client-only, server-only, HTTP client,
HTTP routing and optional Go subscription adapter feature checks; and formatting.

## Response timeout cancellation under backpressure — 2026-10-24

`RequestHandle::await_response` previously awaited both cancellation queue admission
and its transport acknowledgment after a response deadline expired. Full queues
and stalled transports could therefore delay the timeout indefinitely. The ordinary,
progress-reset idle and maximum-total timeout paths now use the existing bounded
best-effort cancellation enqueue. Expiry retains the typed `ServiceError::Timeout`,
request correlation and distinct idle/total reasons, and removes progress watchers.
Full or closed outgoing queues can discard cancellation; remote termination is not
guaranteed. Explicit `RequestHandle::cancel` retains its awaited-delivery behavior.
These response-wait deadlines begin when `await_response` is polled and exclude
request queue admission and MRTR callback work.

Four new engine tests use Tokio's paused clock, enabled as an engine-only dev
feature. They cover all three timeout policies with a saturated outgoing queue,
a queued cancellation with no acknowledgment, and a closed queue. A fourth sends
progress every 80 ms against a 100 ms idle deadline and verifies the 250 ms total
deadline wins without waiting for cancellation delivery. Retained response senders
keep the requests pending; watcher cleanup, exact deadlines, cancellation IDs and
reasons are checked. With the original implementation restored, the saturated
queue, missing acknowledgment and progress/total tests fail their outer deadline;
the closed-queue case passes. Restoring the fix passes all four.

Validation passed: all **364 tests across 45 suites**, including four new engine
tests; standalone engine timeout tests; workspace/all-target/all-feature and
minimal-build Clippy with warnings denied; documentation with warnings denied;
Rust 1.89 minimum-compiler checks; model-only, client-only, server-only, HTTP client,
HTTP routing and optional Go subscription adapter feature checks; and formatting.

## Direct client prompt/resource MRTR helpers — 2026-10-24

`Client::get_prompt` and `Client::read_resource` now expose the owned engine's
bounded automatic loops. Both provide `*_with_mrtr_limits` for per-operation
budgets and `*_once` for native complete/input-required responses without running
callbacks. Native result content, metadata and cache hints are retained. Original
request parameters, including explicit state/answers for manual resumption, pass
through unchanged. `read_skill_uri` now uses the automatic resource path, so both
skill discovery and content reads can answer inputs through a configured handler.
The peer remains available for advanced requests; its single-round convenience
methods do not perform automatic input handling.

Three new integration tests cover static/live automatic reads with default and
explicit budgets, zero/final rounds, input-cap rejection, native input-required
responses, manual resumption without callbacks, and skill index/content MRTR.
They assert required prompt arguments, URI-template parameters, middleware stages,
opaque state, correlated answers, text, metadata, TTL and cache scope. The existing
six skill tests also pass, including malformed index rejection and visibility.

All 360 tests passed across 45 suites, including the doc test. Strict workspace
all-target/all-feature and root no-default-feature Clippy, warnings-as-errors
rustdoc, Rust 1.89 all-target/all-feature checks, isolated model/client/server
and generic HTTP-client builds, HTTP-only routing-test and optional Go-adapter
builds, and formatting passed. CI has not run on GitHub.

## Typed-client MRTR budget overrides — 2026-10-24

`Client::call_with_mrtr_limits`, `Client::call_raw_with_mrtr_limits` and
`TypedTool::call_with_mrtr_limits` expose native per-operation limits without
requiring access to the owned running service. `MrtrLimits` is re-exported from
`client` and the crate root. Ordinary typed/raw calls delegate to the same path
with default limits. Handles retain no mutable policy, so concurrent overrides
and later default calls remain independent. `call_once` remains single-round.

Three new in-memory integration tests check all three entry points against
65-input batches: defaults reject before callbacks, explicit overrides succeed,
and concurrency-one reaches the callback handler. Additional cases verify exact
structured output, unmodified String output, JSON text decoding, execution errors,
native error-content preservation, missing/invalid output, invalid inputs,
zero/final rounds, concurrent distinct policies and single-round input results.
The server asserts original arguments, retry state and every correlated answer.

All 357 tests passed across 45 suites, including the doc test. Strict workspace
all-target/all-feature and root no-default-feature Clippy, warnings-as-errors
rustdoc, Rust 1.89 all-target/all-feature checks, isolated model/client/server
and generic HTTP-client builds, HTTP-only routing-test and optional Go-adapter
builds, and formatting passed. CI has not run on GitHub.

## Native MRTR callback cancellation scopes — 2026-10-24

Each started native MRTR elicitation callback now owns a cancellation drop guard.
Its `RequestContext::ct` is signalled on successful completion, callback error,
unwinding, or dropping the enclosing operation. Batch failure drops active sibling
callbacks and ends their scopes too. This supports cooperative helper shutdown;
it neither joins host-spawned tasks nor rolls back effects already performed.
Queued callbacks that never start have no context or helper scope to cancel.

Three new integration tests exercise automatic tool, prompt and resource loops:
success and error signal retained token clones; dropping one of two concurrent
operations with the same input key cancels only its own token; and a failing
callback cancels a blocked sibling without starting the rest of an eight-input
batch. Tests bound operation and callback-start waits to five seconds.

All 354 tests passed across 44 suites, including the doc test. The affected
integration suite passed again after a test-only lint correction. Strict
workspace all-target/all-feature and root no-default-feature Clippy,
warnings-as-errors rustdoc, Rust 1.89 all-target/all-feature checks, isolated
model/client/server and generic HTTP-client builds, HTTP-only routing-test and
optional Go-adapter builds, and formatting passed. CI has not run on GitHub.

## Native automatic MRTR input budgets — 2026-10-24

`service::MrtrLimits` bounds automatic tool/prompt/resource input loops with ten
outbound rounds (including initial and final requests), 64 input requests per
round and four concurrent callbacks per operation by default. Each dimension has
a builder; concurrency uses `NonZeroUsize`. The `*_with_mrtr_limits` methods
configure complete policies; existing automatic and round-only methods inherit
the new batch/concurrency defaults. Single-round `*_once` methods are unchanged.

All input requests are decoded before the batch size/type preflight. Over-budget
batches return the typed `InputRequiredRequestsExceeded`; retired sampling/roots
requests produce method-not-found before any callback is constructed or polled.
Accepted elicitation futures are created lazily and driven with bounded unordered
concurrency; response keys remain correlated in the returned map. A callback error
or dropping the operation drops active/queued inline work and sends no partial
retry. Callback effects are not rolled back and host-spawned work is host-owned.
The input-required final allowed round returns the round-limit error without
starting callbacks for a retry that cannot be sent. Zero rounds sends nothing;
zero inputs still permits state-only retry/backoff. Limits count parsed requests
and polled callbacks, not payload bytes, memory or wall-clock time. Separate
operations have independent concurrency budgets.

Three engine tests cover exact/default/zero batch limits, whole-batch rejection of
both retired input kinds without host callbacks, semaphore-held concurrency,
complete response-key correlation, and active/queued cleanup on callback failure
or caller cancellation. Three native in-memory integration tests apply limits to
all three operations, checking rejection before callbacks, exact-size successful
retries, zero/final-round behavior, round-only helper defaults, and state-only
retries with a zero input budget.

All 351 tests passed across 44 suites, including the doc test. Strict workspace
all-target/all-feature and root no-default-feature Clippy, warnings-as-errors
rustdoc, Rust 1.89 all-target/all-feature checks, isolated model/client/server
and generic HTTP-client builds, HTTP-only routing-test and optional Go-adapter
builds, and formatting passed. CI has not run on GitHub.

## Native subscription establishment deadline — 2026-10-24

The typed client and owned protocol peer default to a ten-second subscription
establishment deadline. `Client::listen_with_timeout` and the peer's
`listen_with_capacity_and_timeout` configure it. The deadline covers queue
admission through acknowledgment, including stalled sends. Zero rejects before
sending. Successful acknowledgment removes the deadline; application admission
and initial refresh hints retain their existing separate semantics. Expiry
returns `ServiceError::Timeout` (wrapped by `ClientError::Service` at the typed
layer), unregisters local notification routing and attempts nonblocking peer
cancellation. Cancellation delivery is best effort if the outgoing queue is
saturated or closed; it does not promise immediate remote cleanup.

Outgoing requests now reserve queue capacity before installing subscription or
progress routing state. No await separates subscription registration from queue
submission, preventing orphan registration when setup is dropped while awaiting
a full queue. This ordering also applies to ordinary request queue admission.

Two engine regressions exercise a saturated undrained outgoing queue, setup
expiry and caller cancellation, local routing removal and correlated cancellation.
Two paused-clock integration regressions cover default/custom/zero deadlines,
no request ID consumption at zero, late acknowledgment after timeout, continued
ordinary requests on the same connection, and resource updates thirty seconds
after a one-second successful establishment deadline.

All 345 tests passed across 43 suites, including the doc test. Strict workspace
all-target/all-feature and root no-default-feature Clippy, warnings-as-errors
rustdoc, Rust 1.89 all-target/all-feature checks, isolated model/client/server
and generic HTTP-client builds, HTTP-only routing-test and optional Go-adapter
builds, and formatting all passed. CI has not run on GitHub.

## Aggregate peer response-cache budget — 2026-10-24

ClientCacheConfig now defaults to 512 entries and 16 MiB of combined logical-key
UTF-8 and compact JSON response bytes. `with_max_total_bytes` configures the
budget; zero disables retention while zero max_entries still removes only the
entry-count bound. Counting uses a bounded writer outside the cache write lock,
stopping at the budget without allocating an encoded payload copy. A second
generation check prevents invalidation/configuration races from admitting a
response measured under an earlier policy. Uncacheable successful responses
retire both scope variants for the logical key before returning; they preserve
unrelated entries. Fitting responses evict oldest entries until both limits fit.
Budget reductions trim immediately. Stale entries remain subject to the same
budget and existing identity/notification/TTL cleanup.

The budget measures encoded payloads, not heap usage. Parsed values, private
partition identifiers, allocator/container overhead, in-flight clones, parser
and transport buffers are outside its accounting. Response delivery is unchanged;
serialization failures merely skip retention. Transport response limits and
list-all collection limits remain independent.

Three unit regressions check exact key/UTF-8/escaping counts, early stop without
walking a large value, serialization errors, simultaneous count/byte trimming,
zero and oldest-entry order. Native lifecycle regressions check exact and
one-byte-excess budgets, aggregate eviction with unlimited entry count, shrinking,
zero retention, oversized refresh delivery, stale retirement and unrelated cache
hits. The in-flight lifecycle case also changes the byte budget before releasing
a blocked response. Tests use local duplex streams without Go or an external
service.

All 341 workspace tests passed across 43 suites. Workspace all-target/all-feature
Clippy, root no-default Clippy, rustdoc with warnings denied, Rust 1.89.0
all-target/all-feature checking and formatting passed. Isolated model, client,
server, generic HTTP client without reqwest, HTTP-only test and Go-adapter
compilation checks passed. Checks used offline locked Cargo with at most four
jobs. CI has not run on GitHub.

## Bounded native list-all pagination — 2026-10-24

All four native catalog collectors (tools, prompts, resources and resource
templates) share cursor-cycle detection and configurable PaginationLimits.
Existing convenience methods default to 1,024 page lookups and 100,000 items;
explicit-limit methods allow host policy. Tool query metadata, group, tags,
detail and starting cursor are cloned across pages. Repeating the initial resume
cursor is also a cycle. Cached and empty pages count toward the page budget.
Zero pages rejects before a lookup; zero items permits empty pages. Exact terminal
boundaries succeed. Failures return typed local ServiceError variants without a
partial collection and leave the client usable.

These limits bound collection counts, not bytes, total memory or elapsed time.
A page is received and parsed before checking its item count. Individual request
and transport limits remain independent; incremental single-page APIs remain
available. Authorization and catalog configuration must stay stable across pages.

Four deterministic unit regressions cover boundaries, zero budgets, endless
unique empty pages, single/multiple cursor cycles, initial resume cycles, excess
items and fetch errors. Four native client/server regressions cover all catalog
APIs, default cycle detection, explicit budgets, exact success, client reuse,
filter/detail preservation and cached resumed collection. These tests use duplex
streams without an external service or Go runner.

All 336 workspace tests passed across 43 suites. Workspace all-target/all-feature
Clippy, root no-default Clippy, rustdoc with warnings denied, Rust 1.89.0
all-target/all-feature checking and formatting passed. Isolated model, client,
server, generic HTTP client without reqwest, HTTP-only test and Go-adapter
compilation checks passed. The initial full build exhausted disk space while
linking; disposable generated compiler objects/examples were removed and the
complete build/test run then passed. Checks used offline locked Cargo with at
most four jobs. CI has not run on GitHub.

## Bounded stdio input — 2026-10-23

AsyncRwTransport now defaults to a 16 MiB incoming line payload limit, inherited by
stdio tuples, combined byte streams and child-process transports. The configurable
limit counts bytes before LF, including an optional CR. No payload capacity is
allocated eagerly. Each fill-buffer/copy/consume step has no cancellation point;
partial lines remain owned by the transport across cancelled receive calls. Each
chunk is checked before appending. Oversized input ends reception immediately,
clears the partial line and cannot be resumed to admit later bytes; it does not
wait for newline or EOF. Complete unterminated EOF messages remain accepted within
the limit. Empty/malformed lines retain their previous behavior within the budget.

The framework exposes `serve_stdio_with_max_message_size` and
`Client::stdio_with_max_message_size`; the protocol engine exposes a configurable
constructor and child-process builder. Existing convenience APIs use the default.
Zero allows only empty lines. Outgoing messages, the standalone JSON codec, parsed
JSON allocations, reader buffers and process memory are outside this input cap.
The Transport receive API signals termination with None; oversized lines are
logged rather than converted into an uncorrelated JSON-RPC error response.

Six deterministic unit regressions cover exact LF/CRLF/EOF boundaries, one-byte
excess, permanently terminated reception, no-delimiter rejection, repeated
cancellation across single-byte fragments, budget reset, malformed/empty lines,
zero and the finite default without eager allocation. Native client/server tests
check oversized request rejection before dispatch and oversized response shutdown
after successful discovery/calls. Unix child fixtures check builder boundaries and
the typed client's discovery limit. No external services or Go runner are used.

All 328 workspace tests passed across 42 suites. Workspace all-target/all-feature
Clippy, root no-default Clippy, rustdoc with warnings denied, Rust 1.89.0
all-target/all-feature checking and formatting passed. Isolated model, client,
server, generic HTTP client without reqwest, Go-adapter, stdio-only,
child-client-only and stdio/client test compilation checks passed. These local
checks use offline locked Cargo; CI has not run on GitHub.

## Buffered HTTP response limits — 2026-10-23

The built-in reqwest backend bounds decoded JSON response bodies and buffered HTTP
error bodies before parsing, with a configurable 16 MiB default. The worker passes
`max_response_body_size` through discovery and subsequent POSTs. Known
Content-Length values are rejected early; each streamed chunk is checked before
appending, so chunked or unfinished bodies cannot bypass the cap. Zero rejects nonempty buffered bodies. Accepted responses whose
bodies are ignored remain unbuffered. Successful SSE responses keep their independent
event limit; long streams are not capped as a single body.

Regressions cover exact/one-byte-short boundaries, zero, malformed JSON rejected
before parsing, HTTP JSON-RPC error preservation, oversized HTML errors, chunked
responses with no end, independent SSE limits, bootstrap discovery rejection,
and continued operation after an oversized tool result. These checks use local
fixtures only. Authentication remains host-owned; the old OAuth source file is not
part of the active module tree and is outside this validation.
The limit measures buffered payload bytes, not transport chunk allocations, Vec
capacity, parsed JSON allocations or process memory. Custom backend implementations
must override `post_message_with_limits`; its default forwards the SSE limit only.

All 318 workspace tests passed across 41 suites, including five new HTTP response
limit regressions. Workspace all-target/all-feature Clippy and root no-default
Clippy passed with warnings denied; rustdoc passed with warnings denied. Rust
1.89.0 all-target/all-feature checking, isolated model/client/server builds,
generic HTTP-client checking without reqwest, HTTP-only test compilation, isolated
Go-adapter checking, and formatting passed. Checks used offline locked Cargo.

## Aggregate HTTP routing payload budget — 2026-10-23

The native HTTP transport now supplements its 1,024-tool count limit with a
4 MiB default aggregate payload byte budget. max_routing_schema_bytes and its
builder configure it; zero in either retention limit disables retention while
preserving validation and discovery delivery. An entry counts its UTF-8 tool
name plus compact JSON routing projection. Allocator/container overhead, ordering
keys, outstanding discovery tracking, in-flight responses and the separate peer
response cache are not included: this is a payload budget, not a process-memory
ceiling. A counting writer checks encoded length without allocating a serialized
copy and stops when the individual entry exceeds the configured budget.

Oldest full-discovery entries are removed until both limits fit. Refresh, invalid
schema retirement, annotation removal and list-change invalidation release their
accounted bytes. A single oversized entry is returned to the caller but not
retained; it does not evict unrelated tools. An oversized refresh removes the old
entry for that same tool, avoiding stale routing. Lean discovery does not change
accounting or order. No capacity is allocated eagerly.

Four unit regressions cover exact and one-byte-short boundaries with Unicode and
JSON-escaped names, exclusion of full-schema documentation, multiple evictions,
refresh accounting, all retirement paths, oversized schema/name handling, zero
budget with continued invalid-schema filtering, and 100-tool bounded churn with
an unconstrained entry count. A real HTTP test leaves count capacity for both
tools but a byte budget for only one: the evicted tool is rejected before its
handler runs, and uncached full rediscovery restores its promoted header.

All fifteen routing-cache unit tests and thirteen HTTP routing tests pass. Full
workspace validation passes **313 unit/integration/documentation tests across
40 suites**. Strict workspace/core-only Clippy, rustdoc with warnings denied,
Rust 1.89 workspace all-target/all-feature compilation, isolated model/client/
server/Go-adapter builds, generic HTTP client compilation without reqwest,
HTTP-only test compilation and formatting pass. No external Go runner was rerun
for this native retention change.

## HTTP routing invalidation on tool-list changes — 2026-10-23

The native HTTP transport clears routing projections and eviction order on
`notifications/tools/list_changed`, before forwarding the notification. Each
outgoing tools/list request records the current generation. Responses may update
or retire routing entries only once, for a matching request from the current
generation. Older, duplicate and unsolicited responses still undergo full schema
validation before delivery, but cannot mutate current routing contracts. Numeric
string response IDs use the same alias matching as pending stream responses.

Generation tracking contains only outstanding discovery requests. Terminal
responses consume their entries; the worker prunes failures, cancellations and
closed streams against its pending-request set at the next event-loop iteration.
Opaque shared generation tokens avoid counter wraparound. No public API,
dependency or older protocol compatibility was added.

Four new unit regressions cover immediate invalidation, late valid/invalid
responses, current-generation recovery, duplicate/unsolicited responses, repeated
changes, unrelated notifications, error cleanup, numeric string IDs and ordered
SSE completion draining. A real HTTP regression exercises JSON discovery, an SSE
notification followed by a late list response in the same POST, rejected calls
without obsolete headers, and successful calls after fresh full discovery.
Disabling immediate invalidation makes that HTTP regression fail as expected.

All eleven routing-cache unit tests and twelve HTTP routing tests pass. Full
workspace validation passes **308 unit/integration/documentation tests across
40 suites**. Strict workspace/core-only Clippy, rustdoc with warnings denied,
Rust 1.89 workspace all-target/all-feature compilation, isolated model/client/
server/Go-adapter builds, generic HTTP client compilation without reqwest,
HTTP-only test compilation and formatting pass. No external Go runner was rerun
for this native HTTP change.

## Bounded HTTP routing-schema retention — 2026-10-23

The HTTP client now retains routing projections for at most 1,024 annotated
tools by default. max_routing_schemas configures the count; zero disables
retention, not schema validation. Capacity is not allocated eagerly. The oldest
full-discovery entry is evicted first; a full refresh renews its position, while
calls and lean discovery do not. Schemas without annotated properties use no
slot, and removing annotations retires an older projection.

Only annotated parameter names, primitive types and header names are copied into
the routing cache. Full discovery responses and the separate peer response cache
retain their original schemas. The entry count limit is supplemented by the
aggregate payload byte budget described above; incoming transport message limits
remain separate. An evicted tool must be
rediscovered across HTTP before calls requiring promoted headers: clear the peer
response cache first, then fetch detail: full (optionally with group/tag filters).
A cached discovery response alone cannot repopulate transport routing state.

Three unit checks cover eviction/refresh order, 100-tool churn with fixed entry
and ordering bounds, lean retention, compact projection and unchanged responses,
annotation removal, unannotated slot avoidance, zero retention and continued
invalid-schema filtering. A real HTTP test uses a one-entry limit and cached
full discovery for two groups: both tools initially execute, an evicted tool is
rejected before execution, and clearing the peer response cache plus full
rediscovery restores its header and successful call.

All seven routing-cache unit tests and eleven HTTP routing tests pass. Full
workspace validation passes **303 unit/integration/documentation tests across
40 suites**. Workspace all-target/all-feature Clippy, core-only Clippy, rustdoc
with warnings denied, Rust 1.89 all-target/all-feature compilation, isolated
model/client/server/Go-adapter builds, generic HTTP client all-target compilation
without reqwest, HTTP-only test compilation and formatting pass. No external Go
runner was rerun for this native retention change.

## SSE completion routing-schema admission — 2026-10-23

A deterministic queued-response regression reproduced a full tools/list result
being delivered without learning its routing schema when stream completion
selected the queue-drain path. That path also bypassed invalid-annotation
filtering and stale-schema retirement. Stream delivery and completion drains now
share pending-response cleanup, cancellation-registration release and schema
admission before forwarding messages to the handler.

The test queues a valid full schema, an invalid replacement and an unrelated
lean entry, followed by a lean refresh of the valid tool. It forces draining
without scheduler timing, verifies ordered filtered delivery and request-ID
cleanup, preserves the unrelated/full schemas, retires the invalid schema and
checks the next tools/call emits the learned promoted header. Both drain call
sites use the shared admission, including recovery cleanup; no negotiation or
legacy compatibility is added.

The regression fails before schema admission is applied to draining. Full
workspace validation passes **299 unit/integration/documentation tests across
40 suites**. Workspace all-target/all-feature Clippy, core-only Clippy, rustdoc
with warnings denied, Rust 1.89 all-target/all-feature compilation, isolated
model/client/server/Go-adapter builds, generic HTTP client all-target compilation
without reqwest, HTTP-only test compilation and formatting pass. No external Go
runner was rerun for this native SSE delivery fix.

## Request routing-header isolation — 2026-10-23

Two regressions reproduced stale custom Mcp-Name and Mcp-Param-* values surviving
HTTP request construction. Standardized requests now discard these custom
routing values and rebuild Mcp-Method, Mcp-Name and promoted parameters from the
current message and learned schema. Omitted optional arguments and calls without
a learned schema cannot inherit custom promoted headers. Unrelated headers,
including names that only resemble the reserved prefix, remain intact; the
configured header map is not mutated.

A real HTTP client/server regression alternates omitted, present and omitted
optional routed arguments while custom headers contain conflicting defaults.
All three calls reach the expected handler. Existing encoded primitive routing
and live-registry tests exercise the same request path.

Both new construction regressions failed before the fix. Full workspace
validation passes **298 unit/integration/documentation tests across 40 suites**.
Workspace all-target/all-feature Clippy, core-only Clippy, rustdoc with warnings
denied, Rust 1.89 all-target/all-feature compilation, isolated model/client/server/
Go-adapter builds, HTTP-only test compilation and formatting pass. No external
Go runner was rerun for this native request-construction fix.

## Shared routing-schema validation — 2026-10-23

Five registration regressions reproduced acceptance of unusable routing contracts:
invalid tokens, case-insensitive header collisions, referenced nested annotations,
validation opt-out, and task registration. The framework now returns
InvalidInputSchema before committing registration, including for rich/protocol/task
handlers. Skipping argument-value validation does not skip this schema contract.
Failed registrations leave the tool name available for corrected registration.

The model-level validate_tool_header_annotations helper is feature-independent
and shared by registration, HTTP discovery clients and HTTP request admission.
It accepts nonempty RFC token names on explicit top-level string/integer/boolean
properties. It rejects unsupported annotations in nested schema positions,
definitions, arrays and combinators without resolving references. The iterative
schema walk ignores application data in examples, defaults, constants and custom
metadata, and reports escaped JSON-pointer locations. It is not a complete JSON
Schema validator and does not add reference-based header promotion.

An invalid full discovery refresh now retires that tool's previously learned HTTP
routing schema while preserving unrelated tools. Custom low-level handlers also
receive an admission error before execution for an invalid routing schema; a
valid raw-handler control still executes. These checks add no protocol fallback.
Focused registration and HTTP validation passes **23 tests**. Full workspace
validation passes **295 unit/integration/documentation tests across 40 suites**.
Workspace all-target/all-feature Clippy, core-only Clippy, rustdoc with warnings
denied, Rust 1.89 all-target/all-feature compilation, isolated model/client/server/
Go-adapter features, HTTP-only test compilation and formatting pass. The six
model-validator tests and five registration tests also pass with default features
disabled. No external Go runner was rerun for this native routing-contract change.

## HTTP live-registry snapshot isolation — 2026-10-23

Three deterministic regressions replaced the shared live registry immediately
between schema lookup and dispatch. Before snapshot capture, all three admitted
Region headers but executed the replacement Zone handler. The owned engine now
offers `ServerHandler::request_snapshot` before HTTP routing admission. LiveServer
captures its current registry configuration; the modern-policy, Arc and Box
wrappers forward the hook. Admission, server metadata and dispatch therefore use
the captured configuration. The next request captures the new registry, rejects
old annotations and accepts the new ones. The tests order replacement directly
in schema lookup; no sleeps or scheduler-dependent races are involved.

Application state, task managers and event hubs remain shared. Custom handlers
must implement/forward the optional hook for mutable registries; the default
keeps their existing behavior. Stream-oriented transport behavior is unchanged.
Focused HTTP policy/routing validation passes **17 tests**. Full workspace
validation passes **282 unit/integration/documentation tests across 39 suites**.
Workspace all-target/all-feature Clippy, core-only Clippy, workspace rustdoc with
warnings denied, Rust 1.89 all-target/all-feature compilation, isolated
model/client/server/Go-adapter features, HTTP-only test compilation and formatting
pass. No external Go runner was rerun for this native registry-isolation fix.

## Native HTTP routing refresh — 2026-10-23

The owned engine now resolves one service factory result per HTTP request and
uses it for both routing-schema admission and dispatch. Schemas and absent-tool
lookups are no longer retained by tool name across requests. Sequential live
replacements therefore apply their current annotations immediately, including
newly registered tools and removal of routing annotations. Invalid HTTP text in
an optional header now returns an admission error instead of being treated as
an absent header that could allow default arguments to execute.

Five new integration tests cover live annotation replacement/removal, registration
after a missing lookup, alternating service factories, malformed optional headers
and typed-client round trips. Eight round trips exercise encoded tool names,
Unicode, whitespace, control characters, literal encoding sentinels, empty strings,
i64 limits and booleans. Four regression tests failed before the fixes; all five
now pass. The focused HTTP policy/routing suites pass **14 tests**.

Full workspace validation passes **279 unit/integration/documentation tests across
39 suites**. Workspace all-target/all-feature Clippy, core-only Clippy,
workspace rustdoc with warnings denied, Rust 1.89 all-target/all-feature
compilation, isolated model/client/server/Go-adapter features, HTTP-only test
compilation and formatting pass. These native HTTP changes do not rerun the
external Go differential runner. The snapshot milestone above adds concurrent
LiveServer replacement isolation; clients must still refresh full discovery after
routing annotations change.

## Native client cache lifecycle — 2026-10-23

A new wire-level regression reproduced an obsolete fallback: after an expired
resource was successfully refreshed with `ttlMs: 0`, a subsequent failed refresh
returned the earlier value. The owned engine now retires both scope variants of
the old cache entry when a successful response has no retainable effective TTL.
Cache-generation checks still reject writes from requests started before an
invalidation or configuration change.

Six new duplex integration tests cover that regression; expiry, stale fallback,
opt-out and successful recovery; exact-URI notification invalidation and unrelated
resource isolation; bounded oldest-entry eviction and reduced entry limits;
notification/clear/identity-change/disable races against a gated in-flight read;
and all cached tool filters/detail/pages plus prompt and resource/template list
invalidation. Application notification callbacks provide acknowledgment barriers;
semaphores control race ordering. Expiry uses short real-clock waits because the
cache measures monotonic standard-library time. No network service is required.

Focused cache-policy/discovery/lifecycle validation: **15 passing tests**.
Full workspace validation: **274 passing unit/integration/documentation tests**
across 38 suites. Workspace all-target/all-feature Clippy, core-only Clippy,
workspace rustdoc with warnings denied, Rust 1.89 all-target/all-feature
compilation, isolated model/client/server/Go-adapter feature checks and formatting
pass. The workspace run includes the existing localhost transport tests.
No external Go runner is needed for this native cache-state fix; previous Go
checks compare advertised hints, not this client's internal retention policy.

## Pinned Go/Rust progressive discovery — 2026-10-23

`tests/discovery_interop.py` compares real stdio responses from the owned Rust
engine and the clean pinned Go checkout at
`52fa5746759d3284a6950e2667915d05ae2c6682`. Both use modern per-request metadata;
there is no initialize handshake. Go remains a differential test reference.

The runner passed 13 catalog queries per language: default/names/full schemas,
group and case-sensitive AND tags, duplicate tags, empty/unknown filters, and
end-of-catalog cursors. Shared fixtures include a deferred tool with an output
schema and 102 catalog tools with one hidden by visibility. Results agree on
name ordering, labels, schema presence, cursors and page sizes (100+4 unfiltered,
100+1 for the filtered catalog). Seven malformed-query/cursor cases return
invalid params. Deferred execution succeeds with valid input and still rejects
missing required input; hidden and missing calls have equal error categories
within each language.

The runner also verifies two deliberate decoder differences: Go accepts unknown
fields and empty-string detail, while Rust rejects both with invalid params.
Generated schema contents/dialects and exact error messages are not compared.
HTTP caching, concurrent registry mutation and broader transport certification
remain covered separately or open. Evidence: [discovery-interop-report.json](discovery-interop-report.json).

Reproduce with a clean checkout at the pinned commit:

```sh
python3 tests/build_go_reference.py --upstream /path/to/pinned/mcp-go --output /tmp/mcp-go-reference
cargo build --locked --example parity --all-features
python3 tests/discovery_interop.py --go-reference /tmp/mcp-go-reference
```

The fixture build, comparison, existing visibility comparison, Rust example
Clippy with warnings denied, Rust 1.89 example compilation, Python compilation
and formatting were checked locally. This test-harness milestone changes no
library behavior; the 268-test workspace result below belongs to the preceding
implementation milestone.

## Standard progressive tools/list — 2026-10-23

The owned engine now carries group, tags and schema detail on ordinary tools/list
requests. ListedTool supports omitted schemas; complete registration definitions
remain available for validation and HTTP routing. Static and live framework
handlers share the host catalog selection logic. Client cache keys include all
selection fields, and filtered page collection retains them across requests.

Five integration tests cover filters, labels/metadata, schema omission/restoration,
call validation, visibility before pagination, live registry replacement and policy
panic, invalid-parameter errors, filtered collection, and query-separated cache
hits. A new HTTP routing check covers deferred discovery, full schema learning,
and calls after a lean listing plus cached full query. Existing routing checks
now use a deferred tool. An engine unit regression covers strict optional request
parameter decoding and metadata preservation.

Validation: **268 passing workspace unit/integration/documentation tests**
across 37 suites, including 45 engine unit tests and three engine lifecycle
tests. Workspace all-target/all-feature Clippy, core-only Clippy, workspace rustdoc
with warnings denied, Rust 1.89 all-target/all-feature compilation, isolated model/
client/server/Go-adapter feature checks, and formatting pass.

The implementation milestone itself used local checks. The subsequent pinned
Go/Rust comparison above adds selected standard-wire evidence. Broader discovery
certification remains open. CI has not run on GitHub.

## Native task-ID subscriptions — 2026-10-23

The owned engine now supports `taskIds` filters, capability intersection, scoped
server task sinks, and client-side accepted-ID checks. The framework admits
bounded task filters through subscription middleware and obtains atomic initial
snapshots plus ordered, ID-filtered changes from its installed task manager.
Overflow explicitly closes the receiver for resynchronization; unrelated task
updates do not occupy its queue. Drop/cancellation and shutdown release listeners.
TTL failure is published on the next opportunistic sweep, with no expiry worker.

Ten new integration tests cover wire serialization/intersection, snapshots and
ordering, matching-task isolation, overflow, unknown IDs, per-stream/aggregate/ID
limits, middleware rejection, both-peer capability admission, complete/failed/
cancelled/input-required payloads, resumed working status, expiry, sink/client
rejection, 128 initial snapshots, and real localhost HTTP delivery. A protocol
unit test checks registration cleanup and excessive queue-capacity rejection.
A Go adapter regression checks validation and merging of the shared task filter.

Validation: **261 passing workspace unit/integration/documentation tests**.
Workspace all-target/all-feature Clippy, core-only Clippy, rustdoc with warnings
denied, Rust 1.89 all-target/all-feature compilation, and formatting pass. Separate
feature checks cover model-only, client-only, server-only, and the standalone Go
subscription adapter.
No external Go conformance runner was rerun for this native milestone.

## Independent protocol engine — 2026-10-23

The framework now uses `crates/mcp-protocol`, an attributed source fork of rmcp
3.5.1 maintained in this workspace. The upstream license and source commit are
preserved in [UPSTREAM.md](../UPSTREAM.md). This is source ownership, not a
clean-room rewrite. Cargo metadata verifies that neither `rmcp` nor `rmcp-macros`
occurs anywhere in the resolved dependency graph, including development packages;
CI now enforces this check.

Modern lifecycle changes remove the client's initialize/automatic fallback
modes, make ordinary client connections use discovery, reject initialize before
custom server handlers, require modern inline metadata, and remove legacy HTTP
session routing and restoration. HTTP remains stateless. Retired model types
remain where required to decode/reject old requests; they are not supported
protocols. New bounded engine tests cover default discovery, failed discovery
without fallback, and rejection of old version preferences before sending bytes.

Validation: **249 passing unit/integration/documentation tests**, comprising 43
retained protocol unit tests, three new protocol lifecycle tests, and 203 existing
framework checks. Workspace all-target/all-feature Clippy, core-only Clippy,
workspace rustdoc with warnings denied, Rust 1.89 all-target/all-feature
compilation, and formatting pass. Separate feature checks cover model-only,
client-only, server-only, HTTP-client engine builds, and core-only/server-only
gRPC framework builds. No external Go conformance runner was rerun for this
architecture change; historical wire comparisons below retain their original
scope. Progressive tools/list and native task-ID subscriptions are covered by the
later milestones above.

## Earlier validation

The preceding task-filter milestone reran all-feature tests, the subscription probe build, all-target/
all-feature Clippy, Rust 1.89 all-target/all-feature compilation, the standalone
`client-go-subscriptions` feature check, and formatting.
Historical checks below retain their previous scope unless explicitly recorded as rerun.

The preceding task-lifecycle follow-up extends the real Go application fixture and
Rust subscription probe with failed-task payloads and form input requested via
`input_required`, submitted via `tasks/update`, then verified in the completed
result. It also checks missing-task-capability rejection and matching IDs in
post-open working snapshots. Only the probe, fixture, runner, and documentation
changed; the framework implementation and Rust integration test count are unchanged.
Validation for this follow-up: real pinned Go HTTP checks, probe build/Clippy,
Rust 1.89 probe compilation, formatting, and Python parsing.

The preceding cache/routing milestone adds typed server cache hints and six
integration tests. The four cache tests cover all cacheable methods, clone
isolation, live replacement with the client cache disabled, partial resource
hint overrides, execution/error exclusions, SDK client reuse, private-partition
changes, and disabling client caching. Two real HTTP tests use a registered
schema-annotated tool to verify matching routing headers and reject missing,
mismatching, and duplicate method/name/parameter headers before execution.
HTTP request deadlines and server teardown are bounded.

This milestone reran the complete all-feature suite, all-feature example builds,
all-target/all-feature and core-only Clippy, Rust 1.89 all-target/all-feature
compilation, rustdoc with warnings denied, formatting, and the 23-case differential
suite. The new pinned Go cache-hint
runner checks six methods under default, configured private, and zero-TTL public
policies, plus tool/prompt/error exclusions against both actual processes.
Results are in [cache-interop-report.json](cache-interop-report.json). Client
cache expiry, stale fallback, automatic invalidation, and broad Go routing parity
were not certified. Incomplete resource reads have a code guard but no wire test
through the framework's current complete-resource middleware API.

The preceding task conformance follow-up adds three wire integration tests for
unknown task IDs, empty/unknown-key updates, declined/cancelled form input,
terminal updates/cancellation, and completed-task retention/eviction. Elicitation
`cancel` is an input response delivered to the application; it does not itself
cancel the task. The separate real Go HTTP probe now checks unknown-ID errors
and pending input preservation under empty/unknown-key updates. Its error codes
retain the documented wrapping difference. Go retention and additional input
kinds remain unverified. This follow-up reran the full all-feature test suite,
all-target/all-feature Clippy with warnings denied, Rust 1.89 all-target/all-feature
compilation, formatting, Python parsing, and all three real Go subscription probe
modes. Core-only Clippy, rustdoc, other cross-language runners, and full example
builds retain their preceding milestone results.

The latest server-card milestone adds three model tests and two bounded real
HTTP tests, including explicit modern discovery success after router merging.
The new pinned Go card/catalog runner checks matching document JSON (normalizing
only Go's derived absolute catalog URL), media/cache/CORS headers, each body's
SHA256 ETag, GET/HEAD, exact conditional 304, method rejection, and aliases.
See [server-card-interop-report.json](server-card-interop-report.json) for limits.
This milestone reran all-feature tests, all-target/all-feature Clippy and core-only
Clippy with warnings denied, Rust 1.89 all-target/all-feature compilation,
HTTP-only library compilation, rustdoc with warnings denied, formatting, and
Python parsing. It built the Rust parity example and fresh pinned Go fixture.
Other cross-language runners and full example builds retain their preceding
results. CI is configured to run this comparison; GitHub CI has not been run locally.

## Rust checks

- All-feature unit/integration tests: **100 integration tests and five unit tests**.
- Documentation tests: **one compiling example**.
- All-target/all-feature Clippy and core-only Clippy with warnings denied.
- Rust 1.89.0 all-target/all-feature compilation.
- Core-only metadata tests and all-feature compilation for the previous metadata milestone.
  The prior modern-only milestone also passed all 12 isolated CI feature configurations.
- Rustdoc with warnings denied, formatting, and all-feature example builds.
- Python fixture scripts parsed without generating cache files.

Coverage includes typed tools, input/output validation, middleware and deadlines,
resources/templates, prompts/completion, pagination, live configuration, metadata,
progress, modern elicitation, request-aware visibility, native task/MRTR behavior,
subscription admission/delivery/cleanup, and loopback HTTP/WebSocket/gRPC.

`tests/modern_only.rs` verifies static and live servers reject initialize at old
and modern revisions, advertise exactly 2026-07-28, reject retired methods, never
fall back when discovery is absent, and cancel rejected old SDK connections.
Missing resources use the modern invalid-params code (-32602).
The transport policy unit test uses a custom SDK handler advertising older
metadata to verify helpers still reject initialize and advertise only modern MCP.

Elicitation reads explicit form capabilities from the current request, without
synthesizing initialize-time peer state. It is verified over stdio byte streams,
WebSocket, and gRPC; absent and empty elicitation capabilities are rejected.

The task-subscription follow-up adds four integration tests covering bounded
opaque task filters (including infinite iterators), mixed resource/task delivery,
all five typed status payloads, malformed known task messages, and cross-stream
rejection. The existing `open` API remains available without task filters.

The metadata follow-up adds seven integration cases: modern SDK/Go size encoding
normalization without mutating raw data, missing versus malformed metadata,
partial build provenance, process handshake round trips, clone isolation,
replacement/clearing, and live discovery updates. Pure metadata decoding is
available with no framework features enabled. The SDK native identity accessor
is unchanged; the framework's `ServerMetadata` view retains the build extension
and handles the two modern icon encodings.

## In-memory skills follow-up

Added twelve integration cases for validated bundle construction/frontmatter,
file and total size/count limits, index type decoding, ordinary text/binary
resource reads, discovery extension advertisement, middleware observation,
visibility of entrypoints and the index, atomic collisions (including the
reserved index URI), clone isolation, nested bundle removal with unrelated
resources preserved, malformed index shapes/schema/JSON, missing catalogs,
and live replacement on an existing client.

This follow-up reran the complete all-feature test suite (including loopback
transports), all-target/all-feature and core-only Clippy with warnings denied,
Rust 1.89 all-target/all-feature compilation, formatting, and rustdoc with
warnings denied. The parity example and clean pinned Go fixture were rebuilt.
`tests/skills_interop.py` compares modern HTTP discovery, the full index JSON,
entrypoint and supporting text, and binary MIME/base64 content against the
actual upstream directory-backed skill. Results and bounded scope are in
[skills-interop-report.json](skills-interop-report.json). Other historical
cross-language runners were not rerun for this follow-up. CI is configured,
not remotely verified. Directory/archive/template loading remains pending.

## Directory-loading follow-up

The directory loader adds bounded startup snapshots and atomic immediate-child
collection registration. Local tests cover immutable text/binary reads, ignored
non-skill entries, malformed-bundle and resource-conflict rollback, path and
UTF-8 validation, symlinks, Unix socket rejection, per-file and bundle byte limits,
traversal entry/depth limits, and collection count/byte limits. Non-UTF-8
filesystem-name coverage is Linux-specific: the local macOS filesystem rejects
creating those names, so that case is configured for Linux CI rather than claimed
as locally executed. Invalid UTF-8 file contents are tested locally.

The expanded pinned Go HTTP fixture loads two directory skills in each
implementation, comparing their index and seven skill-file reads with inferred
Markdown/SVG/JSON MIME types and unknown-extension binary data. The report records
that Rust snapshots contents while Go reads files per request. Malformed trees
and limits are tested locally, not compared with upstream.

Validation reran all-feature tests, the core-only directory tests, all-target/
all-feature and core-only Clippy with warnings denied, Rust 1.89 all-target/
all-feature compilation, formatting, Python syntax checks, and rustdoc with
warnings denied. The parity example and pinned Go fixture were rebuilt and the
skills interoperability runner passed. Other historical cross-language runners
were not rerun. Archive and parameterized skill loaders remain pending.

## Packed-archive follow-up

Added bounded `SkillArchive` snapshots for ZIP and gzip-compressed tar resources,
including `.tgz` suffix normalization. Six local cases cover namespace and explicit
metadata validation, packed-size limits, unsupported/missing files, directories,
symlinks and Unix sockets, atomic URI/index conflicts, cross-kind namespace
conflicts, clone/removal ownership, immutable file snapshots, binary wire shape,
MIME types, discovery, visibility, middleware, and live replacement. Archives are
opaque: malformed or empty packed bytes are intentionally not parsed. Validation
of members, embedded metadata and expanded sizes is outside this API's contract.

The pinned Go fixture additionally registers ZIP, tar.gz and tgz inputs containing
root `SKILL.md` files. The comparison checks their index entries and exact decoded
binary bytes with MIME types, alongside the existing directory checks. Fixtures
align Go registration order to Rust's sorted catalog; arbitrary catalog ordering
is not compared. Rust snapshots files and uses explicit descriptions, while Go
reads files on demand and optionally reads sibling metadata. The report records
these differences. Parameterized skill templates remain pending.

Validation passed the complete all-feature suite (including loopback transports),
the four core-only archive cases, all-target/all-feature and core-only library
Clippy with warnings denied, Rust 1.89 all-target/all-feature compilation,
formatting, Python syntax checks, and rustdoc with warnings denied. The rebuilt
pinned Go fixture and Rust parity example passed the expanded skills runner.
Other historical cross-language runners were not rerun for this milestone; CI
configuration is not a claim of remote CI execution.

## Parameterized skill-template follow-up

Added atomic builder registration and owned removal for dynamic skill entrypoints.
Six local cases cover supported grammar and rejection, dropped-builder behavior,
blank descriptions, reserved-index and URI conflicts, duplicate registration,
copy-on-write removal, bundle coexistence, nameless index entries, template/static
list separation, raw parameter binding, exact overrides, visibility under
ambiguous overlapping matchers, denial without handler execution, middleware,
native text/binary blocks, handler errors and live replacement.

The Go/Rust HTTP fixture now compares a template's nameless catalog entry,
`resources/templates/list` metadata, absence from static resource listings, and
exact Markdown reads for `widget` and raw `a%2Fb`. Template ordering is deliberately
aligned in the fixture; broader grammar, errors, policies and transports remain
outside the comparison. See [skills-interop-report.json](skills-interop-report.json).

Validation passed the complete all-feature test suite, the two core-only template
cases, all-target/all-feature and core-only library Clippy with warnings denied,
Rust 1.89 all-target/all-feature compilation, formatting, Python syntax checks and
rustdoc with warnings denied. The rebuilt Rust example and pinned Go fixture
passed the expanded skills interoperability runner. Other historical cross-language
runners were not rerun; remote CI execution is not claimed.

## Pinned Go checks

The clean reference is pinned to `52fa5746759d3284a6950e2667915d05ae2c6682`, built
with Go 1.27.3 for the task-subscription follow-up. Modern requests carry protocol version, implementation,
and capabilities in metadata. The upstream transport/dispatcher is unmodified.

- `tests/cache_interop.py`: six modern cacheable methods under three policies,
  execution/error exclusions, and an explicit Go/Rust default TTL difference.
- `tests/differential.py`: 23 shared modern cases, 22 equal projections and one
  explicit missing-prompt-argument error-code difference. Go returns -32603;
  Rust returns -32602. See [differential-report.json](differential-report.json).
- `tests/network_interop.py`: four modern WebSocket/gRPC checks, Rust client to
  Go server and actual Go wire probe to Rust server for each transport. Checks
  include escaped request IDs, tool results/errors, resources, and bounded shutdown.
- `tests/transport_smoke.py`: real Rust stdio and Streamable HTTP discovery,
  lists/calls, and Ctrl-C cleanup. `client_probe` also checks modern Rust/Go stdio.
- `tests/subscription_wire.py`: preserves the native SDK/Go modern negotiation
  difference rather than claiming compatibility between their stream shapes.
- `tests/go_subscription_interop.py`: separately checks actual Go application
  negotiation, real application task snapshots/completion/failure/cancellation,
  form-input resume and capability rejection, and actual
  upstream HTTP transport pushes/disconnect cleanup using the optional modern Go
  subscription client. The task server enables `ResourceSubscribe`, required by
  pinned Go notifier wiring. All five statuses have local typed-stream coverage;
  real Go pushes cover completion, failure, cancellation, and input-required/resume.
  Empty/unknown-key updates preserve pending input, and unknown task IDs are
  rejected with the documented Go error-wrapping difference. Working state is
  verified via snapshots; other input kinds, malformed updates,
  and retention policies are outside this probe.
- `tests/visibility_interop.py`: modern filtered lists, 100+6 pagination,
  hidden/missing error categories, and visible calls/reads.
- `tests/server_metadata_interop.py`: modern identity/instructions and matching
  `buildInfo` extension. The real typed Rust client reads handshake and fresh
  discovery metadata from both processes, normalizing Go size strings into an
  owned native identity. Outgoing icon/discovery-placement differences remain
  explicit in the report.

Machine-readable reports cover selected behavior, not every extension, TLS/mTLS,
identity provider, production load, or UI rendering. The roadmap contains only
remaining modern work; no older-version certification is planned. No publishing,
deployment, paid API calls, or research-app changes were performed.

## Concurrent task responses and payload validation

The added `tests/task_concurrency_interop.py` runner uses actual Go/Rust modern
HTTP task managers with a two-step elicitation fixture. Eight races permit either
conflicting first answer to win, then check duplicate isolation while the second
input waits and result preservation after completion. One case checks two task
IDs with parallel answers; nine cases check invalid response values and stable
failure after a late corrective answer. Terminal-update and execution-error
codes differ explicitly between Go and Rust. The recorded
[report](task-concurrency-interop-report.json) covers 18 cases per language;
it is not exhaustive race detection, authorization, or general schema enforcement.

Local validation passed the new 36-case runner, both earlier task HTTP runners
(38 cases combined), the nine task/MRTR regression tests, all-target/all-feature
Clippy with warnings denied, Rust 1.89 all-target/all-feature compilation, and
Rust/Go formatting and Python syntax checks. Only fixtures, CI, and documentation
changed; the complete test suite and unrelated historical interoperability
runners were not rerun for this milestone. Remote CI execution is not claimed.

## Cancellation/update race follow-up

`tests/task_cancel_interop.py` adds 30 cases per language against the existing
real Go/Rust modern HTTP fixtures: six ordered/isolation cases and eight rounds
each of cancellation versus input, final completion, and failure. Assertions
permit either valid race ordering, correlate request acknowledgements with the
terminal outcome, check result/error payloads, and verify preservation after late
updates and repeated cancellation. Ordered cases establish both sides without
relying on the race scheduler. Go's terminal rejection and Rust's cooperative
acknowledgements remain explicit. See the
[report](task-cancel-interop-report.json).

Local validation passed all 60 new cases, the 36-case conflicting-response runner
after extracting its shared request-dispatch helper, the nine task/MRTR tests,
and Python syntax checks. No Rust implementation or fixture changed, so Rust
compilation/Clippy and unrelated historical suites were not repeated for this
milestone. The new runner is included in CI; remote CI execution is not claimed.


## Prompt/resource multi-round handlers — 2026-10-23

Added native `.protocol_handler` builders for prompts and resources, preserving
completed-result convenience handlers. Original typed request parameters are
available in context extensions and the Session facade. Intermediate results pass
through method-specific native middleware variants; completed resource responses
retain explicit cache hints and inherit missing configured defaults.

Five new duplex integration tests cover static/live input-and-completion rounds,
opaque state and response forwarding, raw template parameters, typed middleware,
cache exclusions for input requests, partial completed-result cache overrides,
SDK automatic client loops with form callbacks, required-argument checks and live
visibility changes before retry, and middleware completion/rejection of input rounds.

Validation: the complete all-feature suite passed (161 unit/integration tests plus
one documentation test). All-target/all-feature and core-only Clippy passed with
warnings denied; Rust 1.89.0 all-target/all-feature compilation, rustdoc with warnings
denied, and formatting passed. The new tests use actual SDK duplex transports.
No additional Go prompt/resource MRTR differential cases were run in this milestone;
that interoperability work remains on the roadmap. Existing cross-language reports
retain their previously recorded scope.


## Prompt/resource MRTR differential checks — 2026-10-23

The real HTTP runner `tests/prompt_resource_interop.py` passed 22 cases per language
(44 combined) against a freshly built pinned Go fixture and the new Rust
`prompt_resource_mrtr` example. Form/URL accept, decline and cancel, two input rounds,
empty and unknown response IDs, malformed string responses, foreign envelopes,
opaque string state echo, and complete-only resource cache hints are asserted.
The result is [prompt-resource-interop-report.json](prompt-resource-interop-report.json).

The implementations have different modern MRTR wire contracts: Go uses array entries
with kind/payload and reserved `_meta` retry keys; SDK 3.5.1 uses keyed method/params
objects and top-level fields. The runner deliberately uses each native envelope
and asserts these differences. It does not certify native Rust-client/Go-server
MRTR interoperability. Two-input retries in this manual runner carry cumulative
answers. Go prompt/resource handlers explicitly construct the exported elicitor
from the current request session; automatic context injection is tool-only.

The wire checks uncovered typed-decoding fallback treating malformed prompt/resource
parameters as unknown methods. The framework now returns invalid parameters (-32602)
without echoing input data. One new regression test covers missing identifiers,
wrong identifier types, scalar/array response containers and non-string state, with
both static and live servers; middleware and handlers are not invoked.

Validation reran the complete all-feature Rust suite: 162 unit/integration tests
and one documentation test passed. All-target/all-feature and core-only Clippy
with warnings denied, Rust 1.89.0 all-target/all-feature compilation, rustdoc with
warnings denied, Rust formatting, Go fixture build/formatting, and Python parsing
passed. CI now includes the differential runner; GitHub CI was not executed locally.
Other cross-language runners retain their preceding recorded scope.

## Explicit Go HTTP MRTR client adapter — 2026-10-23

The opt-in `client-go-mrtr` feature adds `client::go_mrtr::{Client, Config}` for
MCP 2026-07-28 prompt/resource elicitation against the pinned Go HTTP dialect.
`tests/go_mrtr_interop.py` drove the actual Go broker through 14 automatic
operations: form/URL accept, decline and cancel, and two sequential form inputs
for both methods. Cumulative answers, returned state and resource cache hints
were checked. Results are in [go-mrtr-interop-report.json](go-mrtr-interop-report.json).
The existing pinned Go fixture binary was reused; no Go source changed here.

Six local HTTP regression tests cover arbitrary JSON state and per-operation
isolation, cumulative answers, whole-batch validation before callbacks, callback
wire order, duplicate/replayed IDs, unsupported inputs, declared capabilities,
JSON/SSE envelopes, byte/round limits, HTTP and callback deadlines, peer/callback
errors, reserved headers, endpoint validation and redacted configuration Debug.
The client has no background replay worker; cancellation drops inline work.
Consent URLs remain host-owned input data and are not fetched.

The complete all-feature suite passed 168 unit/integration tests and one doc test.
All-target/all-feature and core-only Clippy with warnings denied, Rust 1.89.0
all-target/all-feature compilation, isolated `client-go-mrtr` compilation, rustdoc
with warnings denied, formatting and Python compilation passed. The updated batch
ordering assertion also passed in a targeted rerun. CI now builds the isolated
feature and runs the real Go adapter probe; GitHub CI was not executed locally.

This bounded adapter does not change the native SDK MRTR wire contract or certify
reverse Go-client/Rust-server interoperability. Discovery negotiation, tools,
tasks, sampling and roots are outside this API. HTTP client TLS/authentication,
redirect and retry policy remain caller-owned. No legacy compatibility was added.

## Native Rust gRPC TLS/mTLS — 2026-10-23

The opt-in `grpc-tls` feature enables Tonic/Rustls TLS with AWS-LC, matching the
existing optional HTTP TLS provider. `serve_grpc_tls` requires an explicit server
`Identity` and accepts caller-owned `ServerTlsConfig`. Requiring the identity
avoids Tonic's missing-identity panic; invalid PEM returns an error. The TLS and
plaintext helpers share server/session admission and awaited shutdown. Hosts can
continue composing `GrpcService` into a custom Tonic server.

Three native Rust tests cover modern discovery and typed tool calls over TLS and
required mutual TLS; rejection of an unknown server CA, incorrect DNS name,
plaintext, missing client certificate and an untrusted client certificate; service
availability after rejected connections; active encrypted session shutdown; and
malformed server identity. Checked-in test-only EC identities have localhost SAN,
explicit leaf key usages and fixed 2020–2120 validity. Their regeneration script
is optional; ordinary Rust tests require no Python, OpenSSL or external services.
The trusted server/client fixture chains were also verified with OpenSSL.

All 172 Rust checks passed (171 unit/integration tests and one doc test), including
the three new TLS tests. Strict all-target/all-feature and core-only Clippy,
Rust 1.89.0 all-target/all-feature compilation, isolated `grpc`, `grpc-tls` and
`client,grpc-tls` compilation, rustdoc with warnings denied, formatting and Python
script compilation passed. CI's isolated feature matrix includes server/client
TLS configurations; GitHub CI was not run locally.

No Go process or additional Go interoperability adapter was needed for this work.
The roadmap now prioritizes native Rust behavior and keeps existing Go adapters
optional. Cross-language TLS, WebSocket TLS, proxies, revocation and production
PKI remain outside these local checks. Certificate trust, rotation and application
identity mapping remain host responsibilities. No legacy protocol support was added.

## Native Rust WebSocket TLS/mTLS — 2026-10-23

The optional `websocket-tls` feature adds `serve_websocket_tls` with a host-owned
Rustls server configuration and `Client::websocket_tls` with explicit trust and
optional client identity. Tokio Rustls/Rustls types are re-exported for host
configuration. AWS-LC matches the existing HTTP/gRPC TLS provider; the framework
does not install process-global crypto configuration. No Go process is involved.

TLS and plaintext listeners share admission, Origin checks, inbound framing,
modern MCP discovery and awaited session cleanup. Admission happens before TLS;
one configured handshake deadline covers TLS, WebSocket upgrade and discovery.
Cancellation interrupts pending TLS acceptance. The new client requires wss before
opening a socket, retains request headers and shares the ordinary client's inbound
message limits and TCP/TLS/WebSocket connect deadline. Discovery follows through
the existing modern client lifecycle.

Five Rust-only loopback tests passed: server trust/hostname/Origin admission and
active-session shutdown; required mutual TLS with missing/untrusted/trusted client
certificates; TLS handshake deadline plus separate pre-TLS admission and stalled
handshake cancellation; wss-only client admission; and oversized inbound frames
with continued listener health. They reuse the checked-in localhost certificates
without changing them. The admission test uses a long handshake deadline to ensure
its prompt socket rejection comes from the connection cap rather than expiry.

The full all-feature suite passed 177 checks (176 unit/integration tests and one
doc test), including existing plaintext network transport regressions. Strict
all-target/all-feature and core-only Clippy, Rust 1.89.0 all-target/all-feature
compilation, isolated `websocket`, `websocket-tls` and `client,websocket-tls`
compilation, rustdoc with warnings denied and formatting passed. CI now checks
isolated TLS server/client feature combinations; GitHub CI was not run locally.

The roadmap records native TLS/mTLS coverage for both WebSocket and gRPC.
Cross-language TLS, proxies, revocation, certificate rotation and production PKI
remain outside these checks. Identity mapping and application authorization remain
host responsibilities; no legacy protocol compatibility was added.

## Typed live progressive catalogs — 2026-10-23

`DiscoveredTool` and `ToolDiscoveryResult` now support typed Serde decoding.
Omitted group/tags default to empty values, and omitted schemas remain optional.
Round-trip tests cover names-only entries and rich entries with schemas, title,
description, annotations, icons, metadata and continuation cursors. Malformed
required fields and tag shapes are rejected.

`LiveServer::discover_tools` selects one atomic configuration snapshot per call,
including its visibility policy. A native in-memory client/server test decodes
catalog tool output directly with `Client::call`, then checks replacement,
removal, visibility denial, invalidated/hidden cursors and a retained snapshot.
The serving tool registry and catalog registry are separate to avoid a handler
owning the live registry that owns it. Documentation explains this pattern and
retaining a snapshot for stable pagination across updates.

All 179 all-feature checks passed (178 unit/integration tests and one doc test).
Strict all-target/all-feature and core-only Clippy, Rust 1.89.0 all-target/all-feature
compilation, rustdoc with warnings denied and formatting passed. No dependencies
or Go runtime were added. This remains an opt-in host catalog; standard tools/list
extension support is still blocked by the documented rmcp 3.5.1 type limitations.

## Configurable modern HTTP policy — 2026-10-23

`HttpServerConfig`, `http_router_with_config` and `serve_http_with_config` now
configure host/Origin admission, POST body limits and JSON preference. Existing
helpers delegate with default policy. The framework configuration exposes no
legacy session/store options and retains stateless modern protocol enforcement
and the caller's shutdown token. No dependencies or Go runtime were added.

Five native loopback tests passed. They check host/port and origin/scheme
rejections before handler dispatch; default HTTPS port normalization; missing
Origin and empty versus disabled Origin validation; fixed-length and chunked
oversized bodies plus a zero-byte limit; continued service after rejection;
JSON responses without transport session IDs; legacy initialize and GET/DELETE
rejection; active-request cancellation; and progress-before-result ordering
through JSON-to-SSE fallback.

The full all-feature suite passed 184 checks (183 unit/integration tests and one
doc test). Strict all-target/all-feature and core-only Clippy, Rust 1.89.0
all-target/all-feature compilation, isolated HTTP all-target compilation, rustdoc
with warnings denied and formatting passed. CI already includes isolated HTTP
and all-feature checks; GitHub CI was not run locally. These native checks do not
certify TLS termination, proxy rewriting, CORS or endpoint authentication.

## Strict native gRPC envelopes — 2026-10-23

Native gRPC decoding now rejects contradictory request/notification/response
fields before converting the protobuf envelope to JSON-RPC. A notification cannot
carry a request ID, methods must be nonempty, and success/error response payloads
cannot coexist. Response IDs are removed from the codec's pending map only after
both envelope and JSON payload validation, fixing early correlation consumption
on malformed result or error-data JSON. The public transport terminates receive
on any malformed envelope; this does not introduce retry behavior.

Two new unit tests cover request/notification conflicts, malformed response
correlation preservation, valid success/error completion, arbitrary JSON error
data and duplicate response rejection. The existing empty-result-to-object
protobuf convention and typed request IDs remain supported. A new native loopback
test initializes five streams, sends a different malformed envelope on each,
checks that none invokes the tool, and continues calling through one healthy
client that stayed connected throughout.

All 187 all-feature checks passed (186 unit/integration tests and one doc test).
Strict all-target/all-feature Clippy, Rust 1.89.0 all-target/all-feature compilation,
isolated gRPC compilation, rustdoc with warnings denied, formatting and builds of
the two network probe examples passed. The existing network interoperability
script also passed all four selected Go/Rust WebSocket/gRPC directions using the
cached reference built against the clean pinned upstream commit. It checked typed
calls and selected escaped-ID/error/resource paths; this is not full protocol or
TLS certification. Reports/logs were written under /private/tmp. No dependencies,
protobuf schema changes, Go implementation changes or legacy support were added.


## Native task retention lifecycle — 2026-10-23

Two new native client/server tests run the same retention scenarios through static
and live servers. Completed results, structured failure data and cancelled payloads
remain unchanged across repeated reads. Cancellation clears pending input and
drops the operation. Access after TTL expires clears pending input, aborts the
expired operation and exposes failure for a fresh terminal retention window;
older completed/failed/cancelled tasks are already unknown. Get, cancel and update
return invalid-params for those evicted IDs. Unlimited tasks survive both finite
windows and are removed by explicit manager shutdown. Real elapsed time is used
because the SDK measures retention with std::time::Instant.

All 189 all-feature checks passed (188 unit/integration tests and one doc test),
as did strict all-target/all-feature Clippy, Rust 1.89.0 all-target/all-feature
compilation and formatting. No runtime code, dependencies, Go implementation or
legacy protocol support changed. These are process-local SDK integration checks;
they do not certify durable storage, high-volume admission or Go retention parity.

## Task helper cancellation scopes — 2026-10-23

Task handlers replace the creating request's token so task work can outlive the
creation response. That independent token was never cancelled, leaving helper
work waiting on Context.ct or Session::cancelled() without a termination signal.
A drop guard now cancels the task token whenever its operation future ends or is
aborted. The guard spans callback construction, polling, output validation and
metadata processing. Ordinary request cancellation remains SDK-owned.

Four new native static/live tests cover ordinary request success, tool errors,
panics, deadlines and cancellation, and task success, errors, synchronous and
asynchronous panics, output validation failure, cancellation, access-triggered SDK
expiry and manager shutdown. Task tests failed before the fix and pass with it.
They also retain one running task while other requests/tasks end, confirm task
scopes outlive their creation responses, and keep calling through the same peer.
Helpers still need to observe cancellation; the host owns joining spawned work
and stopping blocking operations.

All 193 all-feature checks passed (192 unit/integration tests and one doc test).
The four new checks also pass without default features. Strict all-target/all-feature
Clippy, Rust 1.89.0 all-target/all-feature compilation and formatting passed.
No dependencies, Go runtime changes or legacy protocol support were added.

## Strict progress ordering at floating-point limits — 2026-10-23

Adding 0.1 to a large last-progress value can round back to the same float,
allowing duplicate progress despite the Session ordering guarantee. Repeated or
decreasing progress now advances by at least one representable step. The existing
finite-range guard rejects an attempted advance past f64::MAX, leaving the last
value unchanged and sending no notification. Ordinary 0.1 increments, no-token
behavior, serialized delivery and notification error propagation are preserved.

A new native client/server regression sends an initial 2^53 value, eight concurrent
clone updates at the same value and f64::MAX. It checks strict received ordering,
finite values and consistent progress tokens, then rejects both repeated and
decreasing updates after the maximum without extra notifications. The peer remains
usable after these rejections. The test failed before the fix and passes after it.
This does not add Go interoperability certification at extreme magnitudes.

All 194 all-feature checks passed (193 unit/integration tests and one doc test),
including the five peer-operation tests. Strict all-target/all-feature Clippy,
Rust 1.89.0 all-target/all-feature compilation and formatting passed. No dependency
or protocol version changes were made.


## Bounded modern client discovery — 2026-10-23

`Client::connect` now bounds modern discovery to ten seconds. The SDK's timeout
only applies to its automatic lifecycle with legacy fallback; this framework
keeps discovery-only negotiation and supplies its own deadline instead.
`Client::connect_with_timeout` accepts a host deadline and returns the typed
`ClientError::DiscoveryTimeout` on expiry. Zero rejects before any discovery send.
The limit covers stalled sends and responses, not earlier transport construction
or requests after connecting. Optional transport convenience clients inherit it;
WebSocket TCP/TLS/upgrade has a separate deadline. Dropping discovery drops its
owned transport, with transport-specific background cleanup potentially asynchronous.

Five paused-clock tests in `tests/client_lifecycle.rs` cover stalled sends and
responses, default/custom/zero deadlines, transport release on timeout and caller
cancellation, a silent in-memory peer seeing only `server/discover` then EOF, and
a successful connection remaining usable after the former deadline. No legacy
handshake or retry is sent.

All 199 all-feature tests passed, including the doc test. Strict all-target,
all-feature Clippy, formatting, and Rust 1.89 all-target/all-feature checks passed.


## Host-configured gRPC server limits — 2026-10-23

`GrpcServerConfig` and `GrpcService::with_config` expose shared stream admission,
per-stream outbound queue capacity, protobuf message bytes in both directions
and the modern handshake deadline. Defaults remain 1024 streams, 32 queued
messages, 4 MiB messages and ten seconds. Typed configuration errors reject
capacities above Tokio limits and a zero deadline before constructing runtime
primitives. Server helpers retain defaults; host-composed services can combine
custom limits with TLS and interceptors. Ten-second writes remain unchanged.

Four tests in `tests/grpc_config.rs` cover invalid configurations without a runtime;
real HTTP/2 stream rejection including silent peers, permit reuse on disconnect
and active-session shutdown across adapter clones; timeout reclamation and
continued established-session use past the former deadline; and large inbound
and outbound message rejection followed by healthy client operation. Queue size
one is exercised throughout. SDK discovery is a bootstrap probe: a first ordinary
MCP request completes establishment and ends the handshake deadline.

All 203 all-feature tests passed, including the doc test. Strict all-target,
all-feature Clippy, formatting, Rust 1.89 all-target/all-feature checks and a
server-only `--no-default-features --features grpc` build passed.
