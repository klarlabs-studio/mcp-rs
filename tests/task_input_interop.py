"""Check form/URL task responses and host-configured TTLs over modern HTTP."""
import argparse
import json
from pathlib import Path
import sys
import time

from network_interop import ROOT, run_server
from task_update_interop import META, control, send, snapshot

# Every request carries its own modern capability context.
META["io.modelcontextprotocol/clientCapabilities"]["elicitation"]["url"] = {}


def eventually_unknown(endpoint, task_id, language, expected_status):
    deadline = time.monotonic() + 5
    while True:
        status, value = send(endpoint, "tasks/get", {"taskId": task_id})
        assert status == 200 and value is not None, (status, value)
        if "error" in value:
            code = value["error"]["code"]
            assert code == (-32603 if language == "Go" else -32602), value
            return code
        assert value["result"]["status"] == expected_status, value
        assert time.monotonic() < deadline, value
        time.sleep(0.025)


def probe(endpoint, language):
    checks = []
    for mode in ("form", "url"):
        for action in ("accept", "decline", "cancel"):
            ack = control(endpoint, "tools/call", {"name": "ask", "arguments": {"key": mode}})
            task_id = ack["taskId"]
            pending = snapshot(endpoint, task_id, "input_required")
            requests = pending["inputRequests"]
            assert len(requests) == 1, requests
            request_id, request = next(iter(requests.items()))
            assert request["method"] == "elicitation/create", request
            params = request["params"]
            assert params["mode"] == mode, params
            if mode == "form":
                assert params["message"] == "What is your name?", params
                assert params["requestedSchema"] == {
                    "type": "object", "properties": {"name": {"type": "string"}}}, params
            else:
                assert params["message"] == "Open the consent page", params
                assert params["url"] == "https://example.test/consent", params
                assert params["elicitationId"] == "consent-1", params
            answer = {"action": action}
            if mode == "form" and action == "accept":
                answer["content"] = {"name": "Ada"}
            control(endpoint, "tasks/update", {"taskId": task_id,
                "inputResponses": {request_id: answer}})
            completed = snapshot(endpoint, task_id, "completed")
            expected = mode + ":" + action
            assert completed["result"]["content"][0]["text"] == expected, completed
            # The handler decides the outcome even when the input action is cancel.
            checks.append({"mode": mode, "action": action, "status": "completed", "text": expected})

    retention = []
    for name in ("retained", "overdue"):
        created = control(endpoint, "tools/call", {"name": name, "arguments": {"key": "ttl"}})
        task_id = created["content"][0]["text"]
        state = snapshot(endpoint, task_id, "completed" if name == "retained" else "working")
        assert state["ttlMs"] == 1500, state
        if name == "retained":
            assert state["result"]["content"][0]["text"] == "retained", state
        elif language == "Rust":
            failed = snapshot(endpoint, task_id, "failed")
            assert "error" in failed, failed
        expected_status = "completed" if name == "retained" else "working" if language == "Go" else "failed"
        code = eventually_unknown(endpoint, task_id, language, expected_status)
        retention.append({"case": name, "ttl_ms": 1500,
            "observed_states": (["completed", "unknown"] if name == "retained" else
                ["working", "unknown"] if language == "Go" else ["working", "failed", "unknown"]),
            "unknown_error_code": code})
    print(json.dumps({"language": language, "input_checks": checks, "retention_checks": retention}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go-reference", type=Path)
    parser.add_argument("--probe")
    parser.add_argument("--language", choices=["Go", "Rust"])
    args = parser.parse_args()
    if args.probe:
        assert args.language, "--language is required with --probe"
        probe(args.probe, args.language)
        return
    assert args.go_reference, "--go-reference is required"
    results = []
    for language, binary, mode in [("Go", args.go_reference.resolve(), "--task-input-http"),
            ("Rust", ROOT / "target/debug/examples/task_wire", "--http")]:
        output = run_server(binary, mode, lambda address: [sys.executable, __file__,
            "--probe", "http://" + address + "/mcp", "--language", language])
        results.append(json.loads(output))
    report = {"status": "passed", "cases_per_language": 8,
        "upstream_commit": "52fa5746759d3284a6950e2667915d05ae2c6682", "checks": results,
        "scope": "Six form/URL elicitation responses and two host-configured TTL cases. "
                 "Expiry differences are asserted; no broad retention, payload-schema or durable-task certification."}
    (ROOT / "docs/task-input-interop-report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
