"""Verify the explicit Go HTTP subscription client, preserving the native wire gap test."""
import argparse
import json
from pathlib import Path
from network_interop import ROOT, run_server


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go-reference", type=Path, required=True)
    args = parser.parse_args()
    client = ROOT / "target/debug/examples/go_subscription_probe"
    checks = []
    for server_mode, client_mode, scope in [
        ("--task-subscription-http", "--tasks", "Actual pinned Go application task manager and HTTP transport: capability rejection, snapshots, completion, failure, cancellation, input-required/resume, unknown-ID errors, empty/unknown-key updates"),
        ("--subscription-http", "--negotiate", "Actual pinned Go application handler and HTTP transport"),
        ("--subscription-transport", "--transport", "Actual pinned Go HTTP transport with deterministic fixture handler; pushes use upstream NotifySubscription"),
    ]:
        output = run_server(args.go_reference.resolve(), server_mode,
                            lambda endpoint: [str(client), client_mode, "http://" + endpoint + "/mcp"])
        checks.append({"scope": scope, "result": output})
        print(output)
    report = {"upstream_commit": "52fa5746759d3284a6950e2667915d05ae2c6682",
              "status": "explicit_http_client_adapter_verified", "checks": checks,
              "limitations": ["Native SDK listen retains its different wire shape; use the opt-in client API.",
                              "Go supplies no accepted-filter acknowledgement or initial snapshot hint.",
                              "No native Rust-server task filters, reverse server adapter, automatic Go resource publisher integration, replay, or reconnect certification.",
                              "Pinned Go HTTP wraps unknown-task errors as -32603 with -32602 in the message; Rust SDK returns -32602 directly.",
                              "Pinned Go ServeHTTP requires Capabilities.ResourceSubscribe to wire task notifications; enabled in this fixture.",
                              "Working state is checked via post-open task snapshots; this does not certify every transition, input kind, malformed update, or Go task retention policy."]}
    (ROOT / "docs/go-subscription-interop-report.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
