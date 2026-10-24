"""Check modern task-update wire admission and pending-input preservation.
Uses the pinned Go task fixture and a Rust fixture over real HTTP, without a
subscription wire adapter. Every malformed update is followed by a task snapshot.
"""
import argparse
import json
from pathlib import Path
import time
import urllib.error
import urllib.request
from network_interop import ROOT, run_server

META = {
    "io.modelcontextprotocol/protocolVersion": "2026-07-28",
    "io.modelcontextprotocol/clientInfo": {"name": "task-update-probe", "version": "1"},
    "io.modelcontextprotocol/clientCapabilities": {
        "extensions": {"io.modelcontextprotocol/tasks": {}}, "elicitation": {"form": {}}
    },
}


def send(endpoint, method, params):
    headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream",
               "Mcp-Protocol-Version": "2026-07-28", "Mcp-Method": method}
    if method == "tools/call":
        headers["Mcp-Name"] = params["name"]
    elif method.startswith("tasks/") and isinstance(params.get("taskId"), str):
        headers["Mcp-Name"] = params["taskId"]
    body = {"jsonrpc": "2.0", "id": "update-probe", "method": method,
            "params": dict(params, _meta=META)}
    request = urllib.request.Request(endpoint, json.dumps(body).encode(), headers)
    try:
        response = urllib.request.urlopen(request, timeout=3)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        status = response.status
        data = response.read().decode()
        if response.headers.get_content_type() == "text/event-stream":
            messages = [json.loads(line[6:]) for line in data.splitlines() if line.startswith("data: ")]
            replies = [message for message in messages if message.get("id") == "update-probe"]
            assert len(replies) == 1, data
            value = replies[0]
        else:
            try:
                value = json.loads(data)
            except json.JSONDecodeError:
                value = None
    if value is not None:
        assert value.get("jsonrpc") == "2.0" and value.get("id") == "update-probe", value
    return status, value


def control(endpoint, method, params):
    status, value = send(endpoint, method, params)
    assert status == 200 and value is not None and "error" not in value, (status, value)
    return value["result"]


def snapshot(endpoint, task_id, state):
    deadline = time.monotonic() + 3
    while True:
        result = control(endpoint, "tasks/get", {"taskId": task_id})
        if result["status"] == state:
            return result
        assert time.monotonic() < deadline, result
        time.sleep(0.01)


def probe(endpoint, language):
    ack = control(endpoint, "tools/call", {"name": "gated", "arguments": {"key": "input"}})
    task_id = ack["taskId"]
    control(endpoint, "tools/call", {"name": "release", "arguments": {"key": "input"}})
    pending = snapshot(endpoint, task_id, "input_required")
    requests = pending["inputRequests"]
    assert len(requests) == 1, requests
    request_id = next(iter(requests))
    checks = []
    cases = [
        ("missing responses", {"taskId": task_id}),
        ("null responses", {"taskId": task_id, "inputResponses": None}),
        ("empty array responses", {"taskId": task_id, "inputResponses": []}),
        ("string responses", {"taskId": task_id, "inputResponses": "invalid"}),
        ("number responses", {"taskId": task_id, "inputResponses": 42}),
        ("boolean responses", {"taskId": task_id, "inputResponses": True}),
        ("array scalar element", {"taskId": task_id, "inputResponses": [42]}),
        ("numeric task ID", {"taskId": 42, "inputResponses": {}}),
        ("missing task ID", {"inputResponses": {}}),
        ("empty responses", {"taskId": task_id, "inputResponses": {}}),
        ("unknown response key", {"taskId": task_id, "inputResponses": {"unknown": {"action": "cancel"}}}),
    ]
    for name, params in cases:
        status, value = send(endpoint, "tasks/update", params)
        outcome = {"http_status": status, "error_code": value.get("error", {}).get("code") if value else None,
                   "accepted": status == 200 and value is not None and "result" in value}
        accepted = name in {"empty responses", "unknown response key"} or (
            language == "Go" and name in {"missing responses", "null responses", "empty array responses"})
        expected = {"http_status": 200,
                    "error_code": None if accepted else (-32603 if language == "Go" else -32602),
                    "accepted": accepted}
        assert outcome == expected, (language, name, expected, outcome, value)
        if accepted:
            assert isinstance(value["result"], dict), (name, value)
        after = control(endpoint, "tasks/get", {"taskId": task_id})
        assert after["status"] == "input_required" and after["inputRequests"] == requests, (name, after)
        checks.append({"case": name, **outcome, "pending_input_preserved": True})
    control(endpoint, "tasks/update", {"taskId": task_id, "inputResponses": {
        request_id: {"action": "accept", "content": {"name": "Ada"}}}})
    completed = snapshot(endpoint, task_id, "completed")
    assert completed["result"]["content"][0]["text"] == "hello Ada", completed
    print(json.dumps({"language": language, "checks": checks, "valid_resume": "hello Ada"}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go-reference", type=Path)
    parser.add_argument("--probe")
    parser.add_argument("--language", choices=["Go", "Rust"])
    args = parser.parse_args()
    if args.probe:
        probe(args.probe, args.language)
        return
    assert args.go_reference, "--go-reference is required"
    import sys
    results = []
    for language, binary, mode in [("Go", args.go_reference.resolve(), "--task-subscription-http"),
                                    ("Rust", ROOT / "target/debug/examples/task_wire", "--http")]:
        output = run_server(binary, mode, lambda endpoint: [sys.executable, __file__, "--probe",
            "http://" + endpoint + "/mcp", "--language", language])
        results.append(json.loads(output))
    report = {"status": "passed", "cases_per_language": 11, "upstream_commit": "52fa5746759d3284a6950e2667915d05ae2c6682", "checks": results,
              "scope": "Selected raw HTTP task update admission and pending-input preservation; no complete task conformance or retention certification."}
    (ROOT / "docs/task-update-interop-report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
