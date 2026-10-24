"""Compare webhook POSTs from Rust and the pinned Go helper to a local receiver.

Usage: python3 tests/webhook_interop.py --go-reference /tmp/mcp-go-reference
Build the Rust webhook_probe example with --features webhooks first.
"""
import argparse
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import subprocess
from threading import Thread

ROOT = Path(__file__).resolve().parents[1]
EXPECTED = [
    {"jsonrpc": "2.0", "method": "notifications/channel/message",
     "params": {"channel": "status", "message": {"type": "text", "text": "hi"}}},
    {"jsonrpc": "2.0", "method": "notifications/resources/updated", "params": {"uri": "data://item"}},
    {"jsonrpc": "2.0", "method": "notifications/test", "params": None},
]


@contextmanager
def receiver(status):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            requests.append({"body": json.loads(body), "content_type": self.headers.get("Content-Type"),
                             "mcp_method": self.headers.get("Mcp-Method"), "x_hook": self.headers.get("X-Hook")})
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        server.daemon_threads = True
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_address[1]}/hook", requests
        finally:
            server.shutdown()
            thread.join(timeout=5)
            if thread.is_alive():
                raise RuntimeError("webhook receiver failed to stop")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go-reference", type=Path, required=True)
    args = parser.parse_args()
    commands = {"rust": [str(ROOT / "target/debug/examples/webhook_probe")],
                "go": [str(args.go_reference.resolve()), "--webhook"]}
    checks = []
    for status in [202, 204, 503]:
        captures = {}
        for language, command in commands.items():
            with receiver(status) as (endpoint, captured):
                result = subprocess.run([*command, endpoint], capture_output=True, text=True, timeout=20)
                assert (result.returncode == 0) == (status < 300), (language, status, result.stderr)
                expected = EXPECTED if status < 300 else EXPECTED[:1]
                assert len(captured) == len(expected), (language, status, captured)
                for request, body in zip(captured, expected):
                    assert request == {"body": body, "content_type": "application/json",
                                       "mcp_method": body["method"], "x_hook": "1"}, request
                captures[language] = captured.copy()
        assert captures["rust"] == captures["go"], captures
        checks.append({"http_status": status, "matching_requests": len(captures["rust"]),
                       "result": "accepted" if status < 300 else "rejected_without_retry"})
    report = {"upstream_commit": "52fa5746759d3284a6950e2667915d05ae2c6682",
              "status": "selected_webhook_payloads_headers_and_status_handling_match", "checks": checks,
              "scope": "Actual Rust and pinned Go helper POSTs to a local HTTP receiver: channel, resource update, null params, no request ID, content type, Mcp-Method, custom header, 202/204 acceptance and 503 rejection. Not TLS, endpoint authentication, retries under transport faults, event subscription, durable delivery, or a formal webhook protocol certification."}
    (ROOT / "docs/webhook-interop-report.json").write_text(json.dumps(report, indent=2) + "\n")
    print("Webhook interop: payloads and headers matched; 202/204 accepted and 503 rejected without application retry")


if __name__ == "__main__":
    main()
