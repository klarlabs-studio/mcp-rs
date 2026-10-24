"""Record the intentional modern subscription wire difference from pinned Go.
Both peers receive identical params, including old `uris` and SDK resource filters.
This is an incompatibility check, not a claim that SDK listen works against Go.
"""
import argparse
from contextlib import ExitStack
import json
import os
from pathlib import Path
import select
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
class Wire:
    def __init__(self, command):
        self.child = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.buffer = b""
    def write(self, value):
        self.child.stdin.write(json.dumps(value).encode() + b"\n")
        self.child.stdin.flush()
    def read(self):
        deadline = time.monotonic() + 10
        while b"\n" not in self.buffer:
            ready, _, _ = select.select([self.child.stdout], [], [], max(0, deadline-time.monotonic()))
            if not ready:
                raise TimeoutError("modern subscription peer did not respond")
            chunk = os.read(self.child.stdout.fileno(), 65536)
            if not chunk:
                raise RuntimeError("modern subscription peer closed unexpectedly")
            self.buffer += chunk
        line, self.buffer = self.buffer.split(b"\n", 1)
        return json.loads(line)
    def close(self):
        self.child.stdin.close()
        try:
            self.child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.child.kill()
            self.child.wait()
            raise RuntimeError("subscription peer did not exit after EOF")
        if self.child.returncode:
            raise RuntimeError(self.child.stderr.read().decode())

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go-reference", type=Path, required=True)
    args = parser.parse_args()
    identifier = 'watch"\\scoped'
    request = {"jsonrpc":"2.0", "id":identifier, "method":"subscriptions/listen", "params":{
        "notifications":{"resourceSubscriptions":["users://Felix"]}, "uris":["users://Felix"],
        "_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28", "io.modelcontextprotocol/clientInfo":{"name":"wire-probe","version":"1"}, "io.modelcontextprotocol/clientCapabilities":{}}}}
    with ExitStack() as cleanup:
        go = Wire([str(args.go_reference.resolve())]); cleanup.callback(go.close)
        rust = Wire([str(ROOT / "target/debug/examples/parity"), "--subscription-stdio"]); cleanup.callback(rust.close)
        go.write(request); rust.write(request)
        go_first, rust_first = go.read(), rust.read()
        assert go_first.get("id") == identifier, go_first
        assert isinstance(go_first.get("result", {}).get("subscriptionId"), str) and go_first["result"]["subscriptionId"], go_first
        assert rust_first.get("method") == "notifications/subscriptions/acknowledged", rust_first
        assert rust_first["params"]["notifications"]["resourceSubscriptions"] == ["users://Felix"], rust_first
        assert rust_first["params"]["_meta"]["io.modelcontextprotocol/subscriptionId"] == identifier, rust_first
        initial = rust.read()
        assert initial.get("method") == "notifications/resources/updated" and initial["params"]["uri"] == "users://Felix", initial
        assert initial["params"]["_meta"]["io.modelcontextprotocol/subscriptionId"] == identifier, initial
        rust.write({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":identifier}})
    report = {"upstream_commit":"52fa5746759d3284a6950e2667915d05ae2c6682", "status":"documented_wire_difference", "go":{"first_frame":"JSON-RPC result with generated subscriptionId","filter_acknowledgment":False}, "rust":{"first_frame":"notifications/subscriptions/acknowledged","subscription_id":"originating JSON-RPC request ID","initial_refresh_hint":True}, "scope":"Native SDK listen retains a different negotiation shape. Use the explicit client-go-subscriptions HTTP API; its evidence is in go-subscription-interop-report.json."}
    (ROOT / "docs/subscription-wire-report.json").write_text(json.dumps(report, indent=2)+"\n")
    print("Modern subscription wire difference verified; Rust scoped acknowledgment and initial hint passed.")
if __name__ == "__main__":
    main()
