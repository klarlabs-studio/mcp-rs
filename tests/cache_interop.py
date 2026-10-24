"""Compare modern cache hints from the actual pinned Go and Rust applications."""
import argparse
from contextlib import ExitStack
import json
from pathlib import Path
from subscription_wire import ROOT, Wire

META = {"io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientInfo": {"name": "cache-probe", "version": "1"},
        "io.modelcontextprotocol/clientCapabilities": {}}
CACHEABLE = [("server/discover", {}), ("tools/list", {}), ("prompts/list", {}),
             ("resources/list", {}), ("resources/templates/list", {}),
             ("resources/read", {"uri": "about://server"})]


def request(wire, method, params, identifier):
    wire.write({"jsonrpc": "2.0", "id": identifier, "method": method,
                "params": dict(params, _meta=META)})
    reply = wire.read()
    assert reply.get("id") == identifier, reply
    return reply


def check(binary, mode, ttl_ms, scope):
    with ExitStack() as cleanup:
        wire = Wire([str(binary)] + ([mode] if mode else []))
        cleanup.callback(wire.close)
        for identifier, (method, params) in enumerate(CACHEABLE):
            reply = request(wire, method, params, identifier)
            result = reply["result"]
            assert result["ttlMs"] == ttl_ms, reply
            assert result["cacheScope"] == scope, reply
            assert result.get("resultType", "complete") == "complete", reply
        # Noncacheable execution and errors must not acquire cache hints.
        for identifier, (method, params) in enumerate([
            ("tools/call", {"name": "hello", "arguments": {"name": "Ada"}}),
            ("prompts/get", {"name": "greet", "arguments": {"name": "Ada"}}),
            ("resources/read", {"uri": "missing://resource"}),
        ], start=len(CACHEABLE)):
            reply = request(wire, method, params, identifier)
            if method == "resources/read":
                assert "error" in reply and "result" not in reply, reply
            else:
                assert "result" in reply, reply
            payload = reply.get("result", reply.get("error"))
            assert "ttlMs" not in payload and "cacheScope" not in payload, reply
    return {"ttlMs": ttl_ms, "cacheScope": scope,
            "cacheable_methods": [method for method, _ in CACHEABLE],
            "execution_and_error_exclusions": "passed"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go-reference", type=Path, required=True)
    args = parser.parse_args()
    checks = {}
    for peer, binary in [("go", args.go_reference.resolve()),
                         ("rust", ROOT / "target/debug/examples/parity")]:
        checks[peer] = {
            "default": check(binary, None, 60_000 if peer == "go" else 0, "private"),
            "configured_private": check(binary, "--cache-private-stdio", 12_345, "private"),
            "configured_public_stale": check(binary, "--cache-public-stdio", 0, "public"),
        }
    report = {"upstream_commit": "52fa5746759d3284a6950e2667915d05ae2c6682",
              "protocol_version": "2026-07-28", "status": "passed", "checks": checks,
              "intentional_difference": "Rust defaults to immediately stale/private; pinned Go defaults to 60000ms/private.",
              "scope": "Cache hints on six successful read/list/discovery methods plus exclusion of tool/prompt execution and a missing-resource error. No response storage, client cache reuse, invalidation, HTTP intermediary cache, or exhaustive cache certification."}
    (ROOT / "docs/cache-interop-report.json").write_text(json.dumps(report, indent=2) + "\n")
    print("Cache hints: six methods across default/private/public policies match explicit expectations; execution/error exclusions pass for real pinned Go and Rust processes.")


if __name__ == "__main__":
    main()
