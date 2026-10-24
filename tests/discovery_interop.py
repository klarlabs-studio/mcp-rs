"""Compare standard progressive tools/list over stdio with the pinned Go engine.

Build parity with --features stdio and the Go fixture with build_go_reference.py.
No initialize handshake or legacy protocol is used. Go is only a test reference.
"""
import argparse
from contextlib import ExitStack
import json
from pathlib import Path

from subscription_wire import ROOT, Wire

BASELINE = "52fa5746759d3284a6950e2667915d05ae2c6682"
META = {
    "io.modelcontextprotocol/protocolVersion": "2026-07-28",
    "io.modelcontextprotocol/clientInfo": {"name": "discovery-interop", "version": "1"},
    "io.modelcontextprotocol/clientCapabilities": {},
}


def check(binary):
    with ExitStack() as cleanup:
        wire = Wire([str(binary), "--discovery-stdio"])
        cleanup.callback(wire.close)
        sequence = 0

        def request(method, params):
            nonlocal sequence
            sequence += 1
            wire.write({"jsonrpc": "2.0", "id": sequence, "method": method,
                        "params": dict(params, _meta=META)})
            reply = wire.read()
            assert reply.get("id") == sequence, reply
            return reply

        discovery = request("server/discover", {})
        assert "2026-07-28" in discovery["result"]["supportedVersions"], discovery

        def collect(params):
            tools, sizes, cursors, seen = [], [], [], set()
            while True:
                reply = request("tools/list", params)
                assert "result" in reply, reply
                result = reply["result"]
                sizes.append(len(result["tools"]))
                for tool in result["tools"]:
                    # Schema dialects and generated titles differ by language. Compare
                    # their advertised presence and the shared discovery fields.
                    tools.append({**{key: tool.get(key, default) for key, default in [
                        ("name", ""), ("description", ""), ("group", ""), ("tags", [])]},
                        "input_schema": "inputSchema" in tool,
                        "output_schema": "outputSchema" in tool})
                cursor = result.get("nextCursor")
                if cursor is None:
                    break
                assert cursor not in seen, "cursor did not advance"
                seen.add(cursor)
                cursors.append(cursor)
                params = dict(params, cursor=cursor)
            names = [tool["name"] for tool in tools]
            assert names == sorted(set(names)), names
            assert "page_000" not in names, names
            return {"tools": tools, "page_sizes": sizes, "cursors": cursors}

        queries = {
            "default": {}, "names": {"detail": "names"}, "full": {"detail": "full"},
            "billing": {"group": "billing"},
            "billing_full": {"group": "billing", "detail": "full"},
            "read_finance": {"tags": ["read", "finance"]},
            "catalog_names": {"group": "catalog", "tags": ["read", "finance"], "detail": "names"},
            "duplicate_tag": {"group": "billing", "tags": ["read", "read"]},
            "case_sensitive_tag": {"tags": ["Read"]},
            "case_sensitive_group": {"group": "Billing"},
            "unknown_group": {"group": "absent"},
            "empty_filters": {"group": "", "tags": []},
            "after_final": {"group": "catalog", "cursor": "page_101"},
        }
        catalogs = {name: collect(params) for name, params in queries.items()}
        assert catalogs["default"]["page_sizes"] == [100, 4], catalogs["default"]
        assert catalogs["catalog_names"]["page_sizes"] == [100, 1], catalogs["catalog_names"]
        assert catalogs["read_finance"]["page_sizes"] == [100, 2], catalogs["read_finance"]
        assert catalogs["default"] == catalogs["empty_filters"]
        for name in ("case_sensitive_tag", "case_sensitive_group", "unknown_group", "after_final"):
            assert catalogs[name]["tools"] == [], catalogs[name]
        assert [tool["name"] for tool in catalogs["billing"]["tools"]] == ["billing.create", "billing.list"]
        assert [tool["name"] for tool in catalogs["duplicate_tag"]["tools"]] == ["billing.list"]
        for tool in catalogs["default"]["tools"]:
            assert tool["input_schema"] == (tool["name"] != "billing.create"), tool
        assert all(not tool["input_schema"] and not tool["output_schema"] for tool in catalogs["names"]["tools"])
        assert all(tool["input_schema"] for tool in catalogs["full"]["tools"])
        assert all(tool["output_schema"] for tool in catalogs["billing_full"]["tools"])

        invalid = {}
        for name, params in {
            "unknown_detail": {"detail": "unknown"}, "wrong_tags": {"tags": "read"},
            "wrong_group": {"group": 42}, "wrong_detail": {"detail": []},
            "missing_cursor": {"cursor": "absent"},
            "hidden_cursor": {"group": "catalog", "cursor": "page_000"},
            "filtered_cursor": {"group": "billing", "cursor": "search"},
        }.items():
            reply = request("tools/list", params)
            assert reply.get("error", {}).get("code") == -32602, (name, reply)
            invalid[name] = reply["error"]["code"]

        # Strict Rust decoding intentionally rejects inputs accepted by Go's
        # encoding/json decoder. Record these rather than relaxing Rust's API.
        differences = {}
        for name, params in {"empty_detail": {"detail": ""}, "unknown_field": {"typo": True}}.items():
            reply = request("tools/list", params)
            differences[name] = "accepted" if "result" in reply else reply["error"]["code"]

        success = request("tools/call", {"name": "billing.create", "arguments": {"q": "invoice"}})
        assert success["result"]["structuredContent"] == {"q": "invoice"}, success
        bad_call = request("tools/call", {"name": "billing.create", "arguments": {}})
        assert "error" in bad_call or bad_call.get("result", {}).get("isError") is True, bad_call
        hidden = request("tools/call", {"name": "page_000", "arguments": {"q": "hidden"}})
        missing = request("tools/call", {"name": "missing", "arguments": {"q": "hidden"}})
        assert "error" in hidden and "error" in missing, (hidden, missing)
        assert hidden["error"]["code"] == missing["error"]["code"], (hidden, missing)
        return {"catalogs": catalogs, "invalid_queries": invalid,
                "decoder_differences": differences, "deferred_call_validated": True,
                "hidden_equals_missing": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go-reference", type=Path, required=True)
    parser.add_argument("--rust-reference", type=Path, default=ROOT / "target/debug/examples/parity")
    args = parser.parse_args()
    go, rust = check(args.go_reference.resolve()), check(args.rust_reference.resolve())
    assert go["decoder_differences"] == {"empty_detail": "accepted", "unknown_field": "accepted"}, go
    assert rust["decoder_differences"] == {"empty_detail": -32602, "unknown_field": -32602}, rust
    assert {key: value for key, value in go.items() if key != "decoder_differences"} == {
        key: value for key, value in rust.items() if key != "decoder_differences"}, (go, rust)
    report = {"upstream_commit": BASELINE, "status": "selected_progressive_discovery_verified",
              "queries": {name: {"names": [tool["name"] for tool in result["tools"]],
                                 "page_sizes": result["page_sizes"], "cursors": result["cursors"]}
                          for name, result in rust["catalogs"].items()},
              "checks": ["group and case-sensitive AND tags", "default/names/full schema presence",
                         "filtered visibility before 100-entry pagination", "invalid queries and cursors",
                         "deferred tool execution still validates input", "hidden and missing access agree"],
              "decoder_differences": {"go": go["decoder_differences"], "rust": rust["decoder_differences"]},
              "limitations": "Selected stdio requests at one pinned revision. Schema content/dialects, HTTP client caching, concurrent catalog mutations, transport certification and complete error wording are not compared."}
    (ROOT / "docs/discovery-interop-report.json").write_text(json.dumps(report, indent=2) + "\n")
    print("Pinned Go/Rust progressive discovery: 13 catalog queries, 7 invalid queries, deferred execution and strict-decoder differences verified")


if __name__ == "__main__":
    main()
