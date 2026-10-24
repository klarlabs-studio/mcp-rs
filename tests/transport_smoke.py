"""Wire smoke tests for the built example. No paid calls or external network.
Run: cargo build --example basic && python3 tests/transport_smoke.py
"""
import json
import os
from pathlib import Path
import select
import signal
import socket
import subprocess
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "target/debug/examples/basic"
META = {"io.modelcontextprotocol/protocolVersion":"2026-07-28", "io.modelcontextprotocol/clientInfo":{"name":"smoke","version":"1"}, "io.modelcontextprotocol/clientCapabilities":{}}
DISCOVER = {"jsonrpc":"2.0","id":1,"method":"server/discover","params":{"_meta":META}}
LIST = {"jsonrpc":"2.0","id":2,"method":"tools/list","params":{"_meta":META}}
CALL = {"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"hello","arguments":{"name":"Felix"},"_meta":META}}



def stop(process):
    if process.poll() is None:
        process.send_signal(signal.SIGINT)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
        raise AssertionError("Server did not complete graceful shutdown within 5 seconds")
    assert process.returncode == 0, f"Server exited with {process.returncode}"


def assert_tools(result):
    assert result["result"]["tools"][0]["name"] == "hello", result


def assert_hello(result):
    assert result["result"]["content"][0]["text"] == "Hello Felix", result


def stdio():
    process = subprocess.Popen([str(EXAMPLE)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    def send(request, response=True):
        process.stdin.write(json.dumps(request) + "\n")
        process.stdin.flush()
        if not response:
            return
        ready, _, _ = select.select([process.stdout], [], [], 10)
        assert ready, "Stdio response timed out"
        return json.loads(process.stdout.readline())
    try:
        assert send(DISCOVER)["result"]["_meta"]["io.modelcontextprotocol/serverInfo"]["name"] == "mcp-rs-example"
        assert_tools(send(LIST))
        assert_hello(send(CALL))
    finally:
        stop(process)
    print("stdio: discovery, list tools, typed tool call passed")


def http():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    env = dict(os.environ, MCP_RS_BIND=f"127.0.0.1:{port}")
    process = subprocess.Popen([str(EXAMPLE), "--http"], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    session = None
    def send(request):
        headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream", "MCP-Protocol-Version": "2026-07-28", "Mcp-Method": request["method"]}
        if "name" in request.get("params", {}):
            headers["Mcp-Name"] = request["params"]["name"]
        if session:
            headers["Mcp-Session-Id"] = session
        req = urllib.request.Request(f"http://127.0.0.1:{port}/mcp", json.dumps(request).encode(), headers)
        with urllib.request.urlopen(req, timeout=10) as response:
            body = response.read().decode()
            if not body:
                return response.headers, None
            if "text/event-stream" in response.headers.get("Content-Type", ""):
                data = [line[5:].strip() for line in body.splitlines() if line.startswith("data:")]
                body = data[-1]
            return response.headers, json.loads(body)
    try:
        for _ in range(100):
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=.1):
                    break
            except OSError:
                if process.poll() is not None:
                    raise RuntimeError(process.stderr.read().decode())
                time.sleep(.05)
        headers, result = send(DISCOVER)
        session = headers.get("Mcp-Session-Id")
        assert result["result"]["_meta"]["io.modelcontextprotocol/serverInfo"]["name"] == "mcp-rs-example", result
        assert_tools(send(LIST)[1])
        assert_hello(send(CALL)[1])
    finally:
        stop(process)
    print("HTTP: discovery, list tools, typed tool call passed")


if __name__ == "__main__":
    stdio()
    http()
