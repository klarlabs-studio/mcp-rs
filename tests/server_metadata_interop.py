"""Verify server metadata and typed decoding of pinned Go/SDK identities."""
import argparse
from contextlib import ExitStack
import json
import subprocess
from pathlib import Path
from subscription_wire import ROOT, Wire

TITLE = "Research 🦀"
INSTRUCTIONS = "Use search before reading details.\nCite the original source."
EXPECTED = {"name":"parity-fixture","version":"1","title":TITLE,
            "description":"Track papers, code, and videos","websiteUrl":"https://example.test/research"}
META = {"io.modelcontextprotocol/protocolVersion":"2026-07-28",
        "io.modelcontextprotocol/clientInfo":{"name":"metadata-probe","version":"1"},
        "io.modelcontextprotocol/clientCapabilities":{}}


def check(binary, peer, revision):
    with ExitStack() as cleanup:
        wire = Wire([str(binary), "--metadata-stdio"])
        cleanup.callback(wire.close)
        request = {"jsonrpc":"2.0", "id":"metadata", "method":"server/discover", "params":{"_meta":META}}
        wire.write(request)
        reply = wire.read()
        assert reply.get("id") == "metadata" and "result" in reply, reply
        result = reply["result"]
        identity = result["_meta"]["io.modelcontextprotocol/serverInfo"]
        assert {key:identity[key] for key in EXPECTED} == EXPECTED, identity
        assert result["instructions"] == INSTRUCTIONS, result
        rust_icon = {"src":"https://example.test/icon.svg","mimeType":"image/svg+xml","sizes":["any"],"theme":"dark"}
        go_icon = {"src":"https://example.test/icon.svg","mimeType":"image/svg+xml","sizes":"any","theme":"dark"}
        assert identity["icons"] == [go_icon if peer == "go" else rust_icon], identity
        assert identity["buildInfo"] == {"commit":"abc123","buildDate":"2026-10-17T10:00:00Z"}, identity
        wire.write({"jsonrpc":"2.0","id":"ready","method":"tools/list","params":{"_meta":META}})
        ready = wire.read()
        assert ready.get("id") == "ready" and "result" in ready, ready
        return {"revision":revision, "standard_fields_and_instructions":"passed", "icon":identity["icons"][0],
                "go_build_info_extension_present":"buildInfo" in identity,
                "discovery_identity_in_meta":True,
                "discovery_top_level_identity_present":"serverInfo" in result}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go-reference", type=Path, required=True)
    args = parser.parse_args()
    results = {}
    for peer, binary in [("go", args.go_reference.resolve()), ("rust", ROOT / "target/debug/examples/parity")]:
        results[peer] = [check(binary, peer, revision) for revision in ["2026-07-28"]]
    typed = {}
    for peer, binary in [("go", args.go_reference.resolve()), ("rust", ROOT / "target/debug/examples/parity")]:
        result = subprocess.run([str(ROOT / "target/debug/examples/client_probe"), "--metadata-stdio", str(binary)],
                                capture_output=True, text=True, timeout=20, check=True)
        metadata = json.loads(result.stdout)
        assert metadata["buildInfo"] == {"commit":"abc123","buildDate":"2026-10-17T10:00:00Z"}, metadata
        assert metadata["identity"]["icons"][0]["sizes"] == ["any"], metadata
        assert {key:metadata["identity"][key] for key in EXPECTED} == EXPECTED, metadata
        typed[peer] = "handshake and fresh discovery decoded, icon sizes normalized in owned typed view, buildInfo preserved"
    report = {"upstream_commit":"52fa5746759d3284a6950e2667915d05ae2c6682", "status":"build_information_and_typed_modern_icon_interop_verified",
              "checks":results, "typed_client_checks":typed,
              "build_info":"Both peers emit matching buildInfo inside the modern metadata identity; the native SDK Implementation view alone omits it.",
              "differences":["Modern SDK icons use array-valued sizes; pinned modern Go uses string-valued sizes.",
                             "Modern Go duplicates serverInfo at top level; SDK Rust identity is in io.modelcontextprotocol/serverInfo result metadata."],
              "scope":"Actual pinned Go metadata options and Rust typed builders; selected common fields, optional icon encodings, discovery placement. Typed Rust client connects to both actual processes and decodes handshake and fresh discovery identities. Outbound icon encoding still differs; this is not a bidirectional rich-icon certification or UI rendering test."}
    (ROOT / "docs/server-metadata-interop-report.json").write_text(json.dumps(report,indent=2,ensure_ascii=False)+"\n")
    print("Server metadata: title, description, website, instructions verified against pinned Go at MCP 2026-07-28; buildInfo matched; typed clients read both modern icon dialects; outbound icon/discovery placement differences recorded")


if __name__ == "__main__":
    main()
