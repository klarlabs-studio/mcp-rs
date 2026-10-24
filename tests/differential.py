"""Run the same wire cases against pinned Go and Rust reference servers.
Compares explicit semantic projections, not whole messages: runtime metadata,
schema descriptions, and human-readable diagnostic wording can legitimately vary.
No results are silently normalized; every checked path is in the shared fixture.
"""
import argparse
from contextlib import ExitStack
import json
from pathlib import Path
import select
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
META = {'io.modelcontextprotocol/protocolVersion': '2026-07-28', 'io.modelcontextprotocol/clientInfo': {'name': 'interop', 'version': '1'}, 'io.modelcontextprotocol/clientCapabilities': {}}


class Peer:
    def __init__(self, binary):
        self.process = subprocess.Popen([str(binary)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.buffer = b""
        self.sequence = 0

    def send(self, method, params, notification=False):
        self.sequence += 1
        request = {"jsonrpc": "2.0", "method": method, "params": dict(params, _meta=META)}
        if not notification:
            request["id"] = self.sequence
        self.process.stdin.write(json.dumps(request).encode() + b"\n")
        self.process.stdin.flush()
        if notification:
            return
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            while b"\n" in self.buffer:
                line, self.buffer = self.buffer.split(b"\n", 1)
                response = json.loads(line)
                if response.get("id") == request["id"]:
                    return response
            ready, _, _ = select.select([self.process.stdout], [], [], max(0, deadline - time.monotonic()))
            if ready:
                import os
                chunk = os.read(self.process.stdout.fileno(), 65536)
                if not chunk:
                    raise AssertionError("Reference server closed stdout unexpectedly")
                self.buffer += chunk
        raise AssertionError(f"Reference timed out for {method}")

    def discover(self, version):
        result = self.send("server/discover", {})
        assert version in result["result"]["supportedVersions"], result

    def close(self):
        self.process.stdin.close()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
            raise AssertionError("Reference did not exit after stdin EOF")
        if self.process.returncode != 0:
            raise AssertionError(self.process.stderr.read().decode())


def pointer(value, path):
    for segment in path.strip("/").split("/"):
        segment = segment.replace("~1", "/").replace("~0", "~")
        value = value[int(segment)] if isinstance(value, list) else value[segment]
    return value


def project(response, case):
    projection = {}
    for path, expected in case.get("expect", {}).items():
        actual = pointer(response, path)
        assert actual == expected, f"{path}: expected {expected!r}, got {actual!r}"
        projection[path] = actual
    for path, expected in case.get("length", {}).items():
        actual = len(pointer(response, path))
        assert actual == expected, f"length {path}: expected {expected}, got {actual}"
        projection["length:" + path] = actual
    for path in case.get("absent", []):
        try:
            pointer(response, path)
        except (KeyError, IndexError):
            projection["absent:" + path] = True
        else:
            raise AssertionError(f"{path} must be absent")
    return projection


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go-reference", type=Path, required=True)
    parser.add_argument("--rust-reference", type=Path, default=ROOT / "target/debug/examples/parity")
    args = parser.parse_args()
    cases = json.loads((ROOT / "tests/fixtures/core-wire.json").read_text())
    count = 0
    differences = []
    for version in ["2026-07-28"]:
        with ExitStack() as cleanup:
            peers = {}
            for language, binary in [("Go", args.go_reference), ("Rust", args.rust_reference)]:
                peer = Peer(binary.resolve())
                cleanup.callback(peer.close)
                peers[language] = peer
            for peer in peers.values():
                peer.discover(version)
            for case in cases:
                projections = {}
                for language, peer in peers.items():
                    response = peer.send(case["method"], case["params"])
                    try:
                        effective = dict(case, expect=dict(case.get("expect", {})))
                        effective["expect"].update(case.get("expect_by_language", {}).get(language, {}))
                        projections[language] = project(response, effective)
                    except (AssertionError, KeyError, IndexError) as error:
                        raise AssertionError(f"{language} {version} {case['name']}: {error}; response={response}") from error
                differing = {path for path in projections["Go"] if projections["Go"][path] != projections["Rust"][path]}
                allowed = {path for overrides in case.get("expect_by_language", {}).values() for path in overrides}
                assert differing <= allowed, f"Undeclared mismatch in {case['name']}: {differing - allowed}"
                if differing:
                    assert case.get("difference_reason"), "Every allowed difference requires an explanation"
                    differences.append({"revision": version, "case": case["name"], "paths": sorted(differing), "reason": case["difference_reason"]})
                count += 1
    report = {"upstream_commit":"52fa5746759d3284a6950e2667915d05ae2c6682", "status": "passed", "cases_per_revision": len(cases), "revisions": ["2026-07-28"], "comparisons": count, "equal_comparisons": count-len(differences), "documented_differences": differences}
    (ROOT / "docs/differential-report.json").write_text(json.dumps(report, indent=2)+"\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
