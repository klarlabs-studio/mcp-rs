"""Check task cancellation/update races against real modern HTTP managers.

Ordered cases establish endpoints; concurrent cases permit either valid ordering.
No test assumes that simultaneous client dispatch fixes the server's schedule.
"""
import argparse
import json
from pathlib import Path
import sys
import time

from network_interop import ROOT, run_server
from task_concurrency_interop import accepted, parallel_requests, start, update
from task_update_interop import control, send, snapshot

GOOD = {"action": "accept", "content": {"name": "Ada"}}
BAD = {"action": "accept", "content": {"name": 42}}
CONFIRM = {"action": "accept", "content": {"confirm": True}}


def admitted(outcome, language, terminal_error=False):
    status, value = outcome
    assert status == 200 and value is not None, outcome
    if "error" in value:
        assert language == "Go" and terminal_error, outcome
        assert "result" not in value and value["error"]["code"] == -32603, outcome
        return False
    accepted(outcome)
    return True


def settled(endpoint, task_id, allowed):
    deadline = time.monotonic() + 3
    while True:
        task = control(endpoint, "tasks/get", {"taskId": task_id})
        status = task["status"]
        if status in ("completed", "failed", "cancelled"):
            assert status in allowed, task
            assert not task.get("inputRequests"), task
            if status == "cancelled":
                assert "result" not in task and "error" not in task, task
            return task
        assert status in ("working", "input_required") and time.monotonic() < deadline, task
        time.sleep(0.01)


def signature(task):
    return {key: task[key] for key in ("status", "result", "error", "inputRequests") if key in task}


def preserved(endpoint, task_id, key, before, language):
    # Repeat cancellation and send a late response after the terminal state is known.
    for method, params in [("tasks/update", update(task_id, key, GOOD)),
                           ("tasks/cancel", {"taskId": task_id})]:
        was_admitted = admitted(send(endpoint, method, params), language, terminal_error=True)
        assert was_admitted == (language == "Rust"), (language, method)
        after = control(endpoint, "tasks/get", {"taskId": task_id})
        assert signature(after) == signature(before), (before, after)


def next_input(endpoint, task_id, first_key):
    task = snapshot(endpoint, task_id, "input_required")
    requests = task["inputRequests"]
    assert len(requests) == 1 and first_key not in requests, task
    key, request = next(iter(requests.items()))
    assert request["params"]["message"] == "Confirm Ada", task
    return key


def ordered(endpoint, language):
    checks = []
    task_id, key = start(endpoint, "sequence")
    accepted(send(endpoint, "tasks/cancel", {"taskId": task_id}))
    final = settled(endpoint, task_id, {"cancelled"})
    preserved(endpoint, task_id, key, final, language)
    checks.append("cancel before input response")

    task_id, first_key = start(endpoint, "sequence")
    control(endpoint, "tasks/update", update(task_id, first_key, GOOD))
    key = next_input(endpoint, task_id, first_key)
    accepted(send(endpoint, "tasks/cancel", {"taskId": task_id}))
    final = settled(endpoint, task_id, {"cancelled"})
    preserved(endpoint, task_id, key, final, language)
    checks.append("cancel after first input response")

    task_id, key = start(endpoint, "sequence")
    outcomes = parallel_requests(endpoint, [("tasks/cancel", {"taskId": task_id})] * 2)
    admissions = [admitted(value, language, terminal_error=True) for value in outcomes]
    assert sum(admissions) == (1 if language == "Go" else 2), outcomes
    final = settled(endpoint, task_id, {"cancelled"})
    preserved(endpoint, task_id, key, final, language)
    checks.append("concurrent duplicate cancellation")

    for answer, state in [(GOOD, "completed"), (BAD, "failed")]:
        task_id, key = start(endpoint, "ask")
        control(endpoint, "tasks/update", update(task_id, key, answer))
        final = settled(endpoint, task_id, {state})
        if state == "completed":
            assert final["result"]["content"][0]["text"] == "form:accept", final
        else:
            assert final["error"]["code"] == (-32603 if language == "Go" else -32602), final
        preserved(endpoint, task_id, key, final, language)
        checks.append("cancel after " + state)

    cancelled_id, cancelled_key = start(endpoint, "sequence")
    other_id, first_key = start(endpoint, "sequence")
    assert cancelled_id != other_id
    outcomes = parallel_requests(endpoint, [("tasks/cancel", {"taskId": cancelled_id}),
        ("tasks/update", update(other_id, first_key, GOOD))])
    for outcome in outcomes:
        accepted(outcome)
    final = settled(endpoint, cancelled_id, {"cancelled"})
    key = next_input(endpoint, other_id, first_key)
    control(endpoint, "tasks/update", update(other_id, key, CONFIRM))
    other = settled(endpoint, other_id, {"completed"})
    assert other["result"]["content"][0]["text"] == "Ada", other
    preserved(endpoint, cancelled_id, cancelled_key, final, language)
    checks.append("cancellation isolated from another task")
    return checks


def race(endpoint, language, mode):
    tool = "ask" if mode == "failure" else "sequence"
    task_id, key = start(endpoint, tool)
    answer = BAD if mode == "failure" else GOOD
    if mode == "completion":
        control(endpoint, "tasks/update", update(task_id, key, GOOD))
        key = next_input(endpoint, task_id, key)
        answer = CONFIRM
    outcomes = parallel_requests(endpoint, [("tasks/cancel", {"taskId": task_id}),
        ("tasks/update", update(task_id, key, answer))])
    cancel_accepted, update_accepted = [admitted(value, language, terminal_error=True) for value in outcomes]
    allowed = {"cancelled"}
    if mode != "input":
        allowed.add("completed" if mode == "completion" else "failed")
    final = settled(endpoint, task_id, allowed)
    status = final["status"]
    if status == "cancelled":
        assert cancel_accepted, outcomes
    else:
        assert update_accepted, outcomes
        if status == "completed":
            assert final["result"]["content"][0]["text"] == "Ada", final
        else:
            assert final["error"]["code"] == (-32603 if language == "Go" else -32602), final
    if language == "Rust":
        assert cancel_accepted and update_accepted, outcomes
    else:
        # Go immediately makes cancellation terminal; any pre-existing outcome
        # rejects cancellation instead. Rust records intent cooperatively.
        assert cancel_accepted == (status == "cancelled"), (outcomes, final)
    preserved(endpoint, task_id, key, final, language)


def probe(endpoint, language):
    checks = ordered(endpoint, language)
    races = []
    for mode, allowed in [("input", ["cancelled"]),
                          ("completion", ["cancelled", "completed"]),
                          ("failure", ["cancelled", "failed"])]:
        for _ in range(8):
            race(endpoint, language, mode)
        races.append({"race": mode, "rounds": 8, "allowed_terminal_states": allowed,
                      "terminal_state_preserved": True})
    print(json.dumps({"language": language, "ordered_and_isolation_checks": checks,
        "race_checks": races, "terminal_cancel_and_update_error_code": -32603 if language == "Go" else None}))


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
    report = {"status": "passed", "cases_per_language": 30,
        "upstream_commit": "52fa5746759d3284a6950e2667915d05ae2c6682", "checks": results,
        "scope": "Six ordered/isolation cases and eight rounds each of cancellation versus input, "
                 "completion, and failure. Either valid race ordering is permitted; not exhaustive "
                 "schedule coverage, execution cancellation, authorization, or distributed consistency."}
    (ROOT / "docs/task-cancel-interop-report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
