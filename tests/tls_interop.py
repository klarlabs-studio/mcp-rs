"""Pinned Go/Rust TLS and mutual-TLS checks in both directions, on loopback only.

Build all Rust examples and tests/build_go_reference.py first. Every rejection
is followed by an authenticated discovery/tool/error/resource operation. This is
bounded interoperability evidence, not a PKI or production-load certification.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
BASELINE = "52fa5746759d3284a6950e2667915d05ae2c6682"


def run(command, env, accepted):
    result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=12)
    if accepted:
        if result.returncode != 0 or not ("TLS probe:" in result.stdout or "Go wire probe:" in result.stdout):
            raise AssertionError(f"valid TLS probe failed: {command}: {result.stdout} {result.stderr}")
    else:
        # Timeout/configuration/CLI failures are not evidence of certificate rejection.
        error = result.stderr.lower()
        if result.returncode == 0 or not any(word in error for word in (
            "certificate", "unknownca", "unknownissuer", "invalidpeer", "invalidname", "tls", "transport error", "connectionclosed",
        )):
            raise AssertionError(f"expected TLS rejection: {command}: {result.stdout} {result.stderr}")
    return "accepted" if accepted else "rejected"


def check(go, transport, authentication, rust_server):
    rust = ROOT / "target/debug/examples/tls_probe"
    env = dict(os.environ, MCP_RS_TLS_FIXTURES=str(ROOT / "tests/fixtures/tls"))
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        address = reservation.getsockname()
    endpoint = f"{address[0]}:{address[1]}"
    server_command = ([str(rust), "serve", transport, endpoint, authentication] if rust_server
                      else [str(go), f"--{transport}-tls", endpoint, authentication])
    results = []
    with tempfile.TemporaryFile(mode="w+") as errors:
        server = subprocess.Popen(server_command, env=env, stdout=subprocess.DEVNULL, stderr=errors)
        try:
            deadline = time.monotonic() + 10
            while True:
                if server.poll() is not None:
                    errors.seek(0)
                    raise RuntimeError(f"TLS server exited early: {errors.read()}")
                try:
                    with socket.create_connection(address, timeout=0.2):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("TLS server did not listen")
                    time.sleep(0.05)
            cases = [("wrong-ca", False), ("wrong-host", False)]
            if authentication == "mtls":
                cases += [("missing", False), ("untrusted", False)]
            else:
                cases += [("missing", True)]
            cases += [("trusted", True)]
            for identity, accepted in cases:
                command = ([str(go), f"--probe-{transport}-tls",
                            (f"wss://{endpoint}/mcp" if transport == "websocket" else endpoint), identity]
                           if rust_server else [str(rust), "probe", transport, endpoint, identity])
                result = run(command, env, accepted)
                results.append({"identity": identity, "result": result})
        finally:
            if server.poll() is None:
                server.send_signal(signal.SIGINT)
            try:
                server.wait(timeout=6)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()
                raise RuntimeError("TLS server failed graceful shutdown")
            if server.returncode != 0:
                errors.seek(0)
                raise RuntimeError(f"TLS server exit {server.returncode}: {errors.read()}")
    return {"transport": transport, "authentication": authentication,
            "direction": "Go probe → Rust server" if rust_server else "Rust client → Go server", "checks": results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go-reference", type=Path, required=True)
    parser.add_argument("--report", type=Path, default=ROOT / "docs/tls-interop-report.json")
    args = parser.parse_args()
    results = []
    for transport in ("websocket", "grpc"):
        for authentication in ("tls", "mtls"):
            for rust_server in (False, True):
                result = check(args.go_reference.resolve(), transport, authentication, rust_server)
                results.append(result)
                print(f"{transport}/{authentication}: {result['direction']}: {len(result['checks'])} passed", flush=True)
    report = {"upstream_commit": BASELINE, "checks": results,
              "scope": "Loopback TLS/mTLS discovery, tools, errors, resources, trust/hostname/client-identity rejection, listener survival and graceful shutdown. No revocation, rotation, proxies or production PKI certification."}
    args.report.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
