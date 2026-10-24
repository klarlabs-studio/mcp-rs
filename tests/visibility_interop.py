"""Compare filtered discovery and direct access against the actual pinned Go filters."""
import argparse
from contextlib import ExitStack
import json
from pathlib import Path
from subscription_wire import ROOT, Wire
META = {'io.modelcontextprotocol/protocolVersion': '2026-07-28', 'io.modelcontextprotocol/clientInfo': {'name': 'interop', 'version': '1'}, 'io.modelcontextprotocol/clientCapabilities': {}}


def check(binary):
    with ExitStack() as cleanup:
        wire = Wire([str(binary), "--visibility-stdio"])
        cleanup.callback(wire.close)
        sequence = 0
        def request(method, params):
            nonlocal sequence
            sequence += 1
            wire.write({"jsonrpc":"2.0", "id":sequence,"method":method,"params":dict(params, _meta=META)})
            reply = wire.read()
            assert reply.get("id") == sequence, reply
            return reply
        discovery = request("server/discover", {})
        assert "2026-07-28" in discovery["result"]["supportedVersions"], discovery
        tools, sizes, cursor, seen = [], [], None, set()
        while True:
            reply = request("tools/list", {} if cursor is None else {"cursor":cursor})["result"]
            sizes.append(len(reply["tools"]))
            tools.extend(item["name"] for item in reply["tools"])
            cursor = reply.get("nextCursor")
            if cursor is None:
                break
            assert cursor not in seen, "cursor did not advance"
            seen.add(cursor)
        assert "page_000" not in tools and "hello" in tools and len(tools) == 106, tools
        assert sizes == [100, 6], sizes
        resources = request("resources/list", {})["result"]["resources"]
        templates = request("resources/templates/list", {})["result"]["resourceTemplates"]
        prompts = request("prompts/list", {})["result"]["prompts"]
        assert len(resources) == 2 and templates == [] and prompts == [], (resources, templates, prompts)
        codes = {}
        for label, method, hidden, missing in [
            ("tool", "tools/call", {"name":"page_000","arguments":{}}, {"name":"missing","arguments":{}}),
            ("resource", "resources/read", {"uri":"users://Felix"}, {"uri":"missing://uri"}),
            ("prompt", "prompts/get", {"name":"greet"}, {"name":"missing"}),
        ]:
            hidden_reply, missing_reply = request(method, hidden), request(method, missing)
            assert "error" in hidden_reply and "error" in missing_reply, (hidden_reply, missing_reply)
            assert hidden_reply["error"]["code"] == missing_reply["error"]["code"], (hidden_reply, missing_reply)
            codes[label] = hidden_reply["error"]["code"]
        visible = request("tools/call", {"name":"hello","arguments":{"name":"Felix"}})["result"]
        assert visible["content"][0]["text"] == "Hello Felix", visible
        resource = request("resources/read", {"uri":"about://server"})["result"]
        assert resource["contents"][0]["text"] == "About the fixture", resource
        return {"projection":{"tools":tools,"page_sizes":sizes,"resources":sorted(item["uri"] for item in resources),"templates":templates,"prompts":prompts,"visible_tool":"Hello Felix","visible_resource":"About the fixture"}, "hidden_equals_missing_error_codes":codes}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go-reference", type=Path, required=True)
    args = parser.parse_args()
    go = check(args.go_reference.resolve())
    rust = check(ROOT / "target/debug/examples/parity")
    assert go["projection"] == rust["projection"], (go, rust)
    report = {"upstream_commit":"52fa5746759d3284a6950e2667915d05ae2c6682", "status":"selected_visibility_semantics_verified",
              "go":go, "rust":rust,
              "scope":"Actual pinned Go WithToolFilter/WithResourceFilter/WithPromptFilter versus Rust visibility policy: filtered pagination, static/template lists, direct hidden access, and visible tool/resource success.",
              "limitations":"Per-language hidden/missing error codes are retained; this does not certify identity providers, completion/subscription parity, every revision, or all error wording."}
    (ROOT / "docs/visibility-interop-report.json").write_text(json.dumps(report, indent=2)+"\n")
    print("Pinned Go/Rust visibility: matching filtered lists, 100+6 pagination, hidden/missing error categories, and visible tool/resource access")


if __name__ == "__main__":
    main()
