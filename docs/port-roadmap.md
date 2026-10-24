# mcp-go → mcp-rs parity roadmap

Baseline: klarlabs-studio/mcp-go at `52fa5746759d3284a6950e2667915d05ae2c6682`.
Status refers to this framework API, not merely types available in the protocol engine.

Scope: **MCP 2026-07-28 only**. Older revisions, initialize fallback, split HTTP/SSE,
legacy tasks, roots, and sampling are intentionally excluded; they are not backlog items.

| Upstream surface | Rust implementation | Status |
|---|---|---|
| Typed tools / fluent registration | Generic async handlers, Serde + Schemars | Implemented |
| Input schema validation | Compiled jsonschema validator before callbacks, explicit opt-out | Implemented; routing-schema annotations validated during registration even with argument validation disabled |
| Declared output schemas | Advertised schemas, successful structured output checked before delivery | Implemented |
| Rich tool results | Native content blocks, absent vs empty structured objects, metadata, annotations/title | Implemented |
| Resources / URI templates / binary content | Exact-first matching, named placeholders, native content vectors | Implemented |
| Prompts / completion | Required string argument validation, native completion callback | Implemented |
| Pagination | Stable ordered 100-entry pages and validated cursors for all four lists; native collectors reject cycles and enforce configurable page/item budgets | Implemented |
| Middleware | Typed responses, authorization, size/rate limits, observations, tracing logs | Implemented core; distributed limits/OTel exporter integration not provided |
| Recovery / deadlines / cancellation | Shared application guard; task panic/cancellation guard and lifetime-bound helper cancellation | Implemented; static/live request and task scope isolation checks |
| Dynamic registry | Atomic snapshots/replacement, removal, explicit per-peer list-change notifications | Implemented; HTTP admission/dispatch retain one request snapshot across concurrent replacement |
| Typed client | Typed tool, prompt and resource calls with per-call MRTR budgets, native result/once calls, automatic skill-resource input handling, custom protocol engine client callbacks, configurable modern discovery deadline | Implemented; stalled send/response, timeout cleanup and cancellation checks; engine response-wait deadlines return under cancellation queue saturation or missing acknowledgments; handle/future drops and transport errors synchronously release local routing and pending response state; abandoned queued requests are skipped and cancellation-safe transports can retire stalled sends |
| Stdio / Streamable HTTP | Server adapters, modern HTTP host/origin/body/JSON policy, optional child-process/HTTP clients, awaited lifecycle cleanup | Implemented; configurable bounded HTTP response collection, native request stream cleanup after caller abandonment (including after POST acknowledgment), cancellation across SSE reads/blocked delivery/response drains, count/time-bounded cooperative final-response draining without post-completion reconnects, consistent retry-policy admission, persistent server minimum delays and overflow-safe retry accounting, cooperative decoder polling for ignored frames and sliced byte parsing for comments/partial events, and cancellation-safe stdio input buffering with oversized-line termination |
| Progress / elicitation | Request-bound Session facade, native modern callbacks, finite strictly increasing progress | Implemented; concurrent clone/large-value/overflow checks |
| Modern tasks | Shared protocol engine manager, task handlers, get/update/cancel, input requests, capability admission | Implemented protocol engine integration; local update/input/retention checks and selected real Go/Rust HTTP lifecycle, form/URL responses, expiry, conflicting/duplicate responses, cancellation/update races, and malformed-update/payload checks; broader Go parity remains below |
| Modern MRTR | Native tool/prompt/resource protocol handlers, request-bound opaque state/input responses; protocol engine client loops | Implemented wrappers; local static/live round trips, middleware, admission, and automatic client loops tested; configurable native per-round input and per-operation callback concurrency limits with whole-batch modern-input preflight; lifetime-bound callback cancellation tokens with concurrent-operation isolation; 22 selected HTTP cases per language checked; opt-in Go HTTP prompt/resource elicitation adapter verified across 14 automatic operations; native Go/protocol engine envelopes still differ |
| MCP Apps | Tool/resource metadata and native HTML resources | Metadata implemented; rendering is host-owned and untested |
| Go/Rust differential checks | Shared wire cases at MCP 2026-07-28, pinned Go fixture | Implemented bounded suite; not exhaustive certification |
| Modern protocol policy | Discovery only; old initialize rejected; client fallback removed | Implemented across servers, clients, and transport helpers with negative tests |
| WebSocket / gRPC | Optional server/client adapters, bounded framing, shutdown, pinned protobuf schema | Implemented; selected cross-language checks; opt-in native WebSocket/gRPC TLS/mTLS; local certificate rejection, Origin/framing/admission, strict gRPC envelope/correlation, configurable admission/queue/message/handshake limits and shutdown checks; 36 bidirectional pinned Go/Rust loopback TLS/mTLS cases passed; production PKI qualification remains host-owned |
| Modern resource/list/task subscriptions | Shared bounded hub, exact/template URI filters, middleware admission, initial refresh hints, atomic task snapshots and bounded ID-filtered updates, scoped protocol engine notifications, cancellation and shutdown | Implemented for protocol engine 2026-07-28; opt-in pinned Go stateless HTTP client adapter implemented; Go task-ID filters implemented; native Rust task filters and status delivery implemented; configurable bounded native establishment, queue-admission and acknowledgment cleanup checks; reverse adapter pending |
| Response cache hints / modern routing headers | Typed server cache policy, per-result hint preservation, protocol engine HTTP admission | Implemented hints; bounded local routing and real Go cache-hint checks; native client reuse/expiry/stale recovery, bounded entry/aggregate-payload eviction, notification invalidation and in-flight write isolation; shared routing-schema validation and invalid-refresh retirement, per-request reserved header isolation, SSE completion/drain schema admission, bounded projected routing-schema retention with aggregate byte budgeting and eviction recovery, configurable bounded native JSON/error response collection, tool-list notification invalidation and generation-guarded late-response admission, current-request schema refresh, concurrent live admission/dispatch isolation and malformed/encoded/primitive header admission; broader cache/routing certification pending |
| Request-aware visibility | Typed request-context policy, filtered pagination, direct access/completion/subscription admission, panic containment | Implemented; selected pinned Go filter comparisons |
| Extended server metadata builders | Native identity, title/description/website/icons/instructions, typed buildInfo, live discovery, typed client decoding of modern Go/protocol engine icon sizes | Implemented; outgoing protocol engine icon arrays and discovery placement differences retained explicitly |
| Server cards / catalogs | Typed modern card and opt-in static HTTP routes, GET/HEAD/cache validators, explicit public URLs | Implemented bounded helpers and real Go checks; full schema/browser certification and live updates excluded |
| Skills | Bounded immutable bundles, directory snapshots, packed archives and dynamic templates, atomic registration/collection loading, ordinary resources/index, typed client helpers, visibility-aware index | Implemented bounded helpers; selected pinned Go checks, broader extension certification pending |
| Progressive discovery | Standard tools/list and typed host catalogs, atomic live lookup, groups, AND tags, schema detail/deferral, visibility, pagination, bounded native list-all collection and query-aware client caching | Implemented in the owned engine; local wire/client cache/HTTP routing checks and selected pinned Go stdio comparisons; broader schema/transport/concurrency certification pending |
| Webhooks | Opt-in async HTTP notifier, channel messages, bounded payloads/deadlines, typed sequential fan-out | Implemented delivery helpers; selected pinned Go wire checks; event wiring, durable delivery and auth enforcement host-owned |
| Distributed session store / Redis | Modern HTTP explicitly stateless; legacy session restoration excluded | Feasibility complete; Redis session adapter outside modern scope; shared application/task/event persistence remains host-owned |

This is a substantial core parity milestone, **not a feature-complete port** of
all upstream extensions and transports. Known deliberate differences and the
limits of the differential suite are in [compatibility.md](compatibility.md).

The framework now owns its protocol engine in `crates/mcp-protocol`, an attributed
source fork of rmcp 3.5.1 with no dependency on that package. Default client
connections use discovery; the engine rejects retired initialization even for
custom handlers. See [UPSTREAM.md](../UPSTREAM.md) for source and license details.

Native Rust-server task subscriptions now support exact IDs, atomic initial
snapshots, bounded matching status updates, capability checks, middleware
admission, cancellation and cleanup through the owned engine. Progressive discovery
now carries `group`, `tags`, `detail`, and optional schemas on standard `tools/list`.
Typed client queries and page collection preserve selection, and response cache
keys separate filters and schema detail. Broader cross-language certification remains.

Modern HTTP uses the owned stateless transport manager. Independent instances
accept self-contained requests without transport session IDs. Shared tasks,
application state and durable event replay need host-owned storage.

The first-release acceptance contract is now explicit in
[release-readiness.md](release-readiness.md). The shared native transport matrix
covers concurrent progressive discovery, MRTR, cache invalidation, task streams,
live replacement and graceful subscription completion on framed streams, HTTP,
WebSocket and gRPC. The complete local gate also runs every existing pinned Go
fixture, including discovery and webhooks, plus bidirectional TLS/mTLS checks.
Go remains a differential test reference, not a runtime dependency.

The broad-certification qualifications above describe the limits of bounded tests,
not unspecified implementation work. No named supported modern helper remains
unimplemented in this roadmap. Additional reverse adapters, exact wire alignment,
exhaustive extension certification and production deployment qualification remain
outside the first-release contract. Hosted CI and publication remain separate
release steps; older protocol compatibility will not be added.

Selected task conformance now covers form and URL elicitation with accept/decline/cancel,
and two host-configured TTL cases over real Go/Rust HTTP. Go removes overdue tasks;
the Rust protocol engine exposes failure and retains it for another TTL window. These differences
are documented and asserted. Concurrent conflicting answers, duplicates across input
rounds, independent task IDs, and selected invalid payloads are also checked against
both servers. Terminal update and execution-error differences remain explicit.
Cancellation/update checks now cover ordered cancellation, duplicate cancellations,
isolation, and races against input, completion, and failure. Either valid race
ordering is permitted; Go/Rust terminal admission differences are asserted.
Native static/live retention checks now cover completed, failed, cancelled,
expired and unlimited tasks, including stable result/error payloads, pending-input
cleanup, operation drops, eviction errors and manager shutdown. Retention across
process restarts, durable storage and high-volume admission remain host concerns;
these checks do not certify broader Go retention parity.
