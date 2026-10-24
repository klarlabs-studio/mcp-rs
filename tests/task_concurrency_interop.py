"""Race task responses and check handler rejection of opaque input payloads.

Uses independent real HTTP requests against the pinned Go and Rust managers.
The two-step fixture holds a task open so duplicates are tested before completion.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import sys
from threading import Barrier

from network_interop import ROOT, run_server
from task_update_interop import control, send, snapshot


def start(endpoint, tool):
    created = control(endpoint, "tools/call", {"name": tool, "arguments": {"key": "form"}})
    task_id = created["taskId"]
    pending = snapshot(endpoint, task_id, "input_required")
    requests = pending["inputRequests"]
    assert len(requests) == 1, pending
    return task_id, next(iter(requests))


def update(task_id, key, answer):
    return {"taskId": task_id, "inputResponses": {key: answer}}


def accepted(outcome):
    status, value = outcome
    assert status == 200 and value is not None and "error" not in value, outcome
    assert isinstance(value.get("result"), dict), outcome


def parallel_requests(endpoint, requests):
    # Synchronize client dispatch; either request may win on the server.
    barrier = Barrier(len(requests))

    def dispatch(value):
        barrier.wait(timeout=5)
        method, params = value
        return send(endpoint, method, params)

    with ThreadPoolExecutor(max_workers=len(requests)) as pool:
        futures = [pool.submit(dispatch, value) for value in requests]
        return [future.result(timeout=5) for future in futures]


def parallel_updates(endpoint, params):
    return parallel_requests(endpoint, [("tasks/update", value) for value in params])


def terminal_update(endpoint, task_id, key, answer, language):
    status, value = send(endpoint, "tasks/update", update(task_id, key, answer))
    assert status == 200 and value is not None, (status, value)
    if language == "Go":
        assert value.get("error", {}).get("code") == -32603, value
    else:
        accepted((status, value))


def race(endpoint, language):
    task_id, name_key = start(endpoint, "sequence")
    answers = [{"action": "accept", "content": {"name": name}} for name in ("Ada", "Grace")]
    for outcome in parallel_updates(endpoint, [update(task_id, name_key, answer) for answer in answers]):
        accepted(outcome)
    pending = snapshot(endpoint, task_id, "input_required")
    requests = pending["inputRequests"]
    assert len(requests) == 1 and name_key not in requests, pending
    confirm_key, request = next(iter(requests.items()))
    message = request["params"]["message"]
    assert message in ("Confirm Ada", "Confirm Grace"), pending
    winner = message.removeprefix("Confirm ")

    # Both matching and conflicting duplicates are ignored while round two waits.
    for outcome in parallel_updates(endpoint, [update(task_id, name_key, answer) for answer in answers]):
        accepted(outcome)
    after = control(endpoint, "tasks/get", {"taskId": task_id})
    assert after["status"] == "input_required" and after["inputRequests"] == requests, after
    control(endpoint, "tasks/update", update(task_id, confirm_key, {
        "action": "accept", "content": {"confirm": True}}))
    completed = snapshot(endpoint, task_id, "completed")
    assert completed["result"]["content"][0]["text"] == winner, completed
    terminal_update(endpoint, task_id, name_key, answers[0], language)
    final = control(endpoint, "tasks/get", {"taskId": task_id})
    assert final["status"] == "completed" and final["result"] == completed["result"], final


def isolated_tasks(endpoint):
    tasks = [(name, *start(endpoint, "sequence")) for name in ("Ada", "Grace")]
    assert tasks[0][1] != tasks[1][1], tasks
    params = [update(task_id, key, {"action": "accept", "content": {"name": name}})
              for name, task_id, key in tasks]
    for outcome in parallel_updates(endpoint, params):
        accepted(outcome)
    confirmations = []
    for name, task_id, name_key in tasks:
        pending = snapshot(endpoint, task_id, "input_required")
        requests = pending["inputRequests"]
        assert len(requests) == 1 and name_key not in requests, pending
        key, request = next(iter(requests.items()))
        assert request["params"]["message"] == "Confirm " + name, pending
        confirmations.append(update(task_id, key, {"action": "accept", "content": {"confirm": True}}))
    for outcome in parallel_updates(endpoint, confirmations):
        accepted(outcome)
    for name, task_id, _ in tasks:
        final = snapshot(endpoint, task_id, "completed")
        assert final["result"]["content"][0]["text"] == name, final


def invalid_payloads(endpoint, language):
    cases = [("null", None), ("string", "invalid"), ("number", 42), ("boolean", True),
             ("array", []), ("missing action", {}), ("unknown action", {"action": "unknown"}),
             ("missing accepted content", {"action": "accept"}),
             ("wrong field type", {"action": "accept", "content": {"name": 42}})]
    checks = []
    for name, answer in cases:
        task_id, key = start(endpoint, "ask")
        # The container is valid. Opaque values are accepted by tasks/update;
        # the handler (or Go's elicitation decoder) subsequently rejects them.
        accepted(send(endpoint, "tasks/update", update(task_id, key, answer)))
        failed = snapshot(endpoint, task_id, "failed")
        assert failed.get("error", {}).get("code") == (-32603 if language == "Go" else -32602), failed
        assert not failed.get("inputRequests") and "result" not in failed, failed
        terminal_update(endpoint, task_id, key, {"action": "accept", "content": {"name": "Ada"}}, language)
        final = control(endpoint, "tasks/get", {"taskId": task_id})
        assert final["status"] == "failed" and final["error"] == failed["error"], final
        checks.append({"case": name, "update_acknowledged": True, "status": "failed",
                       "error_code": failed["error"]["code"], "late_answer_preserves_failure": True})
    return checks


def probe(endpoint, language):
    for _ in range(8):
        race(endpoint, language)
    isolated_tasks(endpoint)
    print(json.dumps({"language": language, "race_rounds": 8,
        "conflicting_answers_choose_one": True, "duplicate_answers_preserve_next_input": True,
        "terminal_updates_preserve_result": True, "independent_task_ids": True,
        "terminal_update_error_code": -32603 if language == "Go" else None,
        "invalid_payloads": invalid_payloads(endpoint, language)}))


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
    report = {"status": "passed", "cases_per_language": 18,
        "upstream_commit": "52fa5746759d3284a6950e2667915d05ae2c6682", "checks": results,
        "scope": "Eight conflicting-response races with duplicate and terminal updates, "
                 "one two-task isolation case, and nine rejected response payloads. "
                 "Selected HTTP behavior, not exhaustive race detection, authorization or generic schema validation."}
    (ROOT / "docs/task-concurrency-interop-report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
