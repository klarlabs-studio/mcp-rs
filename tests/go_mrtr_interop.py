"""Check the opt-in Rust HTTP MRTR adapter against the pinned Go broker."""
import argparse
import json
from pathlib import Path
from network_interop import ROOT, run_server


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go-reference", type=Path, required=True)
    args = parser.parse_args()
    output = run_server(args.go_reference.resolve(), "--prompt-resource-mrtr-http",
                        lambda address: [str(ROOT / "target/debug/examples/go_mrtr_probe"),
                                         "http://" + address + "/mcp"])
    assert "14 prompt/resource operations passed" in output, output
    report = {
        "status": "explicit_http_client_adapter_verified",
        "upstream_commit": "52fa5746759d3284a6950e2667915d05ae2c6682",
        "operations": 14,
        "result": output,
        "scope": "Automatic form/URL accept, decline and cancel for prompts/resources; two sequential form inputs with cumulative answers; returned state and complete-only resource cache hints.",
        "limitations": [
            "Opt-in client-go-mrtr HTTP API; native SDK MRTR retains its different wire shape.",
            "Only MCP 2026-07-28; no initialize or legacy fallback.",
            "Prompt/resource elicitation only; no tool calls, tasks, sampling or roots.",
            "Opaque JSON state and malformed-peer bounds are tested locally; the actual Go fixture echoes string state.",
            "No reverse Go-client/Rust-server MRTR adapter or discovery negotiation is certified.",
            "The host callback handles consent; the adapter does not fetch URL consent endpoints."
        ]
    }
    (ROOT / "docs/go-mrtr-interop-report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(output)


if __name__ == "__main__":
    main()
