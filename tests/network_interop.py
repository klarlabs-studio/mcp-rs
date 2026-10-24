"""Bidirectional cross-language modern WS/gRPC checks against the pinned Go reference.
Usage: python3 tests/network_interop.py --go-reference /tmp/mcp-go-reference
"""
import argparse
import json
from pathlib import Path
import signal
import socket
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]


def run_server(binary, mode, probe):
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        address = reservation.getsockname()
    endpoint = f"{address[0]}:{address[1]}"
    server = subprocess.Popen([str(binary), mode, endpoint], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    try:
        deadline = time.monotonic() + 10
        while True:
            if server.poll() is not None:
                raise RuntimeError(f"server exited early: {server.stderr.read()}")
            try:
                with socket.create_connection(address, timeout=0.2):
                    break
            except OSError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("server did not listen")
                time.sleep(0.05)
        completed = subprocess.run(probe(endpoint), capture_output=True, text=True, timeout=20)
        if completed.returncode != 0:
            raise RuntimeError(f"probe exit {completed.returncode}: {completed.stderr} {completed.stdout}")
        return completed.stdout.strip()
    finally:
        if server.poll() is None:
            server.send_signal(signal.SIGINT)
        try:
            _, error = server.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()
            server.communicate()
            raise RuntimeError("server failed graceful shutdown within five seconds")
        if server.returncode != 0:
            raise RuntimeError(f"server exit {server.returncode}: {error}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go-reference", type=Path, required=True)
    parser.add_argument("--report", type=Path, default=ROOT / "docs/network-interop-report.json")
    args = parser.parse_args()
    rust = ROOT / "target/debug/examples/parity"
    client = ROOT / "target/debug/examples/client_probe"
    go = args.go_reference.resolve()
    results = []
    for mode, prefix in [("--websocket", "ws://"), ("--grpc", "http://")]:
        output = run_server(go, mode, lambda endpoint: [str(client), mode, prefix + endpoint])
        results.append({"transport": mode[2:], "direction": "Rust client → Go server", "result": output})
        output = run_server(rust, mode, lambda endpoint: [str(go), "--probe-" + mode[2:], ("ws://" if mode == "--websocket" else "") + endpoint])
        results.append({"transport": mode[2:], "direction": "Go wire probe → Rust server", "result": output})
    report = {"upstream_commit": "52fa5746759d3284a6950e2667915d05ae2c6682", "checks": results,
              "scope": "MCP 2026-07-28 discovery and selected network operations; not a full protocol or TLS certification."}
    args.report.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    for check in results:
        print(f"{check['transport']}: {check['direction']}: {check['result']}")


if __name__ == "__main__":
    main()
