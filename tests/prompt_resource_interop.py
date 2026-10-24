"""Bounded prompt/resource MRTR behavior and explicit Go/SDK wire differences.
Uses each implementation's own modern envelope; it does not implement a client adapter.
"""
import argparse
import copy
import json
from pathlib import Path
import sys
import urllib.error
import urllib.request

from network_interop import ROOT, run_server
from task_update_interop import META

STATE = "opaque-fixture-state"
INPUT_KEY = "io.modelcontextprotocol/inputResponses"
STATE_KEY = "io.modelcontextprotocol/requestState"


def send(endpoint, language, method, mode, responses=None, state=STATE, foreign=False):
    params = {"uri": "mrtr://" + mode} if method == "resources/read" else {"name": "input", "arguments": {"mode": mode}}
    meta = copy.deepcopy(META)
    meta["io.modelcontextprotocol/clientCapabilities"]["elicitation"]["url"] = {}
    # These are different modern wire contracts. Never normalize the response on wire.
    envelope = ("Rust" if language == "Go" else "Go") if foreign else language
    if envelope == "Go":
        if state is not None:
            meta[STATE_KEY] = state
        if responses is not None:
            meta[INPUT_KEY] = ([{"id": key, "payload": value} for key, value in responses.items()]
                               if isinstance(responses, dict) else responses)
    else:
        if state is not None:
            params["requestState"] = state
        if responses is not None:
            params["inputResponses"] = responses
    params["_meta"] = meta
    headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream",
               "Mcp-Protocol-Version": "2026-07-28", "Mcp-Method": method,
               "Mcp-Name": params.get("uri", params.get("name"))}
    body = {"jsonrpc": "2.0", "id": "mrtr-probe", "method": method, "params": params}
    request = urllib.request.Request(endpoint, json.dumps(body).encode(), headers)
    try:
        response = urllib.request.urlopen(request, timeout=3)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        status = response.status
        data = response.read().decode()
        if response.headers.get_content_type() == "text/event-stream":
            replies = [json.loads(line[6:]) for line in data.splitlines() if line.startswith("data: ")]
            replies = [reply for reply in replies if reply.get("id") == "mrtr-probe"]
            assert len(replies) == 1, data
            value = replies[0]
        else:
            value = json.loads(data)
    assert status == 200 and value["jsonrpc"] == "2.0" and value["id"] == "mrtr-probe", (status, value)
    return value


def result(*args, **kwargs):
    value = send(*args, **kwargs)
    assert "error" not in value, value
    return value["result"]


def pending(value, language, mode, second=False, state=STATE):
    assert value["resultType"] == "input_required", value
    assert "ttlMs" not in value and "cacheScope" not in value, value
    assert value.get("requestState") == state, value
    requests = value["inputRequests"]
    request_id = "ir-1" if second else "ir-0"
    if language == "Go":
        assert isinstance(requests, list) and len(requests) == 1, value
        assert requests[0]["id"] == request_id and requests[0]["kind"] == "elicitation", value
        params = requests[0]["payload"]
    else:
        assert isinstance(requests, dict) and set(requests) == {request_id}, value
        assert requests[request_id]["method"] == "elicitation/create", value
        params = requests[request_id]["params"]
    if mode == "url":
        expected = {"mode": "url", "message": "Open consent", "url": "https://example.test/consent", "elicitationId": "consent-1"}
    else:
        key, field_type = ("confirm", "boolean") if second else ("name", "string")
        expected = {"mode": "form", "message": "Confirm Ada" if second else "Choose a name",
                    "requestedSchema": {"type": "object", "properties": {key: {"type": field_type}}, "required": [key]}}
    assert params == expected, (params, expected)
    return request_id


def completed(value, method, mode, expected):
    assert value["resultType"] == "complete", value
    if method == "resources/read":
        assert value["contents"] == [{"uri": "mrtr://" + mode, "mimeType": "text/plain", "text": expected}], value
        assert value["ttlMs"] == 777 and value["cacheScope"] == "public", value
    else:
        assert value["messages"] == [{"role": "user", "content": {"type": "text", "text": expected}}], value
        assert value["description"] == "Input complete", value
        assert "ttlMs" not in value and "cacheScope" not in value, value


def probe(endpoint, language):
    checks = []
    for method in ("resources/read", "prompts/get"):
        for mode in ("form", "url"):
            for action in ("accept", "decline", "cancel"):
                first = result(endpoint, language, method, mode)
                request_id = pending(first, language, mode)
                answer = {"action": action}
                if mode == "form" and action == "accept":
                    answer["content"] = {"name": "Ada"}
                expected = mode + ":" + action
                completed(result(endpoint, language, method, mode, {request_id: answer}, first["requestState"]), method, mode, expected)
                checks.append({"method": method, "case": mode + ":" + action, "result": expected})
        first = result(endpoint, language, method, "sequence")
        request_id = pending(first, language, "sequence")
        responses = {request_id: {"action": "accept", "content": {"name": "Ada"}}}
        second = result(endpoint, language, method, "sequence", responses, first["requestState"])
        next_id = pending(second, language, "sequence", second=True)
        # Go replays the handler; carry prior answers explicitly in this manual probe.
        responses[next_id] = {"action": "accept", "content": {"confirm": True}}
        completed(result(endpoint, language, method, "sequence", responses, second["requestState"]), method, "sequence", "sequence:Ada")
        checks.append({"method": method, "case": "two-input-rounds", "result": "sequence:Ada"})
        for case, responses in [("empty-responses", {}), ("unknown-response-id", {"unknown": {"action": "cancel"}})]:
            pending(result(endpoint, language, method, "form", responses), language, "form")
            checks.append({"method": method, "case": case, "result": "input_required"})
        foreign = result(endpoint, language, method, "form", {"ir-0": {"action": "accept", "content": {"name": "Ada"}}}, foreign=True)
        pending(foreign, language, "form", state=None)
        checks.append({"method": method, "case": "foreign-envelope-not-consumed", "result": "input_required"})
        malformed = send(endpoint, language, method, "form", "invalid")
        if language == "Go":
            pending(malformed["result"], language, "form")
            outcome = "input_required"
        else:
            assert malformed["error"]["code"] == -32602, malformed
            outcome = -32602
        checks.append({"method": method, "case": "string-responses", "result": outcome})
    assert len(checks) == 22, checks
    print(json.dumps({"language": language, "checks": checks}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go-reference", type=Path)
    parser.add_argument("--probe")
    parser.add_argument("--language", choices=["Go", "Rust"])
    args = parser.parse_args()
    if args.probe:
        assert args.language
        probe(args.probe, args.language)
        return
    assert args.go_reference
    results = []
    for language, binary, mode in [("Go", args.go_reference.resolve(), "--prompt-resource-mrtr-http"),
                                  ("Rust", ROOT / "target/debug/examples/prompt_resource_mrtr", "--http")]:
        output = run_server(binary, mode, lambda address: [sys.executable, __file__, "--probe", "http://" + address + "/mcp", "--language", language])
        results.append(json.loads(output))
    report = {"status": "passed", "cases_per_language": 22, "upstream_commit": "52fa5746759d3284a6950e2667915d05ae2c6682", "checks": results,
              "wire_compatible": False,
              "scope": "Modern HTTP prompt/resource form and URL accept/decline/cancel, two input rounds with cumulative manual responses, missing/unknown IDs, foreign envelopes, malformed responses, state echo and cache hints. Go array/kind/payload plus reserved _meta retry keys differ from SDK keyed method/params plus top-level retry fields. No native Rust-client/Go-server MRTR compatibility or automatic replay adapter is claimed. Go prompt/resource fixture constructs NewElicitor from SessionFromContext because those handlers lack automatic elicitor injection. URL payloads are inspected only; no consent URL is fetched."}
    (ROOT / "docs/prompt-resource-interop-report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
