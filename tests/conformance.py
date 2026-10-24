"""Reproducible first-release acceptance gate; failures produce a nonzero exit.

Examples:
  python3 tests/conformance.py --offline --go-reference /tmp/go-reference
  python3 tests/conformance.py --only interop --go-reference /tmp/go-reference
  python3 tests/conformance.py --only quality --only features --offline

Build the pinned Go reference explicitly with build_go_reference.py first. Local
runs require permission to bind loopback sockets. No suite publishes anything.
Reports describe only checks actually run; skipped groups are never marked passed.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
GROUPS = ("quality", "features", "tests", "interop")
FRAMEWORK_FEATURES = (
    "", "stdio", "http", "client", "client-stdio", "client-http",
    "client-go-subscriptions", "client-go-mrtr", "elicitation", "webhooks",
    "websocket", "websocket-tls", "grpc", "grpc-tls",
    "client,websocket", "client,websocket-tls", "client,grpc", "client,grpc-tls",
)
PROTOCOL_FEATURES = ("", "client", "server", "client,transport-streamable-http-client-reqwest")
INTEROP = (
    "differential", "network_interop", "tls_interop", "subscription_wire",
    "go_subscription_interop", "visibility_interop", "server_metadata_interop",
    "cache_interop", "server_card_interop", "skills_interop", "discovery_interop",
    "task_update_interop", "task_input_interop", "task_concurrency_interop",
    "task_cancel_interop", "prompt_resource_interop", "go_mrtr_interop", "webhook_interop",
)


def fingerprint():
    """Hash implementation, fixtures and gate configuration, excluding generated evidence."""
    files = [ROOT / "Cargo.toml", ROOT / "Cargo.lock"]
    for directory in ("src", "crates", "examples", "tests", ".github"):
        files.extend(p for p in (ROOT / directory).rglob("*")
                     if p.is_file() and "__pycache__" not in p.parts and p.suffix != ".pyc")
    digest = hashlib.sha256()
    for path in sorted(set(files)):
        digest.update(str(path.relative_to(ROOT)).encode() + b"\0")
        digest.update(path.read_bytes())
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--only", action="append", choices=GROUPS)
    parser.add_argument("--go-reference", type=Path)
    parser.add_argument("--cargo", default="cargo")
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--jobs", type=int, default=2)
    parser.add_argument("--msrv", default="1.89.0")
    parser.add_argument("--report", type=Path, default=ROOT / "target/conformance/report.json")
    args = parser.parse_args()
    groups = args.only or list(GROUPS)
    if args.jobs < 1:
        parser.error("--jobs must be positive")
    if "interop" in groups and (args.go_reference is None or not args.go_reference.is_file()):
        parser.error("interop requires --go-reference pointing to the pinned fixture binary")
    if os.environ.get("CARGO_TARGET_DIR"):
        parser.error("unset CARGO_TARGET_DIR: existing wire fixtures expect ROOT/target/debug/examples")
    args.report.parent.mkdir(parents=True, exist_ok=True)
    logs = args.report.parent / "logs"
    logs.mkdir(exist_ok=True)
    env = dict(os.environ, CARGO_INCREMENTAL="0", PYTHONDONTWRITEBYTECODE="1")
    checks = []
    report = {"scope": "MCP 2026-07-28 supported first-release scope; bounded checks, not exhaustive certification",
              "started_at": datetime.now(timezone.utc).isoformat(), "source_sha256": fingerprint(),
              "requested_groups": groups, "skipped_groups": [g for g in GROUPS if g not in groups],
              "status": "running", "checks": checks}
    args.report.write_text(json.dumps(report, indent=2) + "\n")

    def run(group, name, command, extra_env=None, timeout=1200):
        logfile = logs / f"{len(checks):03}-{name}.log"
        started = time.monotonic()
        entry = {"group": group, "name": name, "command": command, "log": str(logfile.resolve()), "status": "running"}
        checks.append(entry)
        print(f"RUN {group}/{name}", flush=True)
        try:
            with logfile.open("w") as output:
                result = subprocess.run(command, cwd=ROOT, env=dict(env, **(extra_env or {})),
                                        stdout=output, stderr=subprocess.STDOUT, timeout=timeout)
            entry.update(returncode=result.returncode, status="passed" if result.returncode == 0 else "failed")
            if result.returncode:
                print(logfile.read_text()[-6000:], file=sys.stderr)
                raise RuntimeError(f"{name} failed: {logfile}")
        except BaseException:
            entry["status"] = "failed"
            raise
        finally:
            entry["seconds"] = round(time.monotonic() - started, 3)
            args.report.write_text(json.dumps(report, indent=2) + "\n")
        print(f"PASS {group}/{name} ({entry['seconds']}s)", flush=True)

    def cargo(group, name, command, *options, toolchain=None, extra_env=None):
        invocation = [args.cargo] + (["+" + toolchain] if toolchain else []) + [command]
        if command != "fmt":
            invocation += ["--locked", "-j", str(args.jobs)] + (["--offline"] if args.offline else [])
        run(group, name, invocation + list(options), extra_env)

    try:
        if "quality" in groups:
            cargo("quality", "format", "fmt", "--all", "--check")
            cargo("quality", "clippy", "clippy", "--workspace", "--all-targets", "--all-features", "--", "-D", "warnings")
            cargo("quality", "clippy-minimal", "clippy", "--lib", "--no-default-features", "--", "-D", "warnings")
            cargo("quality", "documentation", "doc", "--workspace", "--no-deps", "--all-features", extra_env={"RUSTDOCFLAGS": "-D warnings"})
            cargo("quality", "msrv", "check", "--workspace", "--all-targets", "--all-features", toolchain=args.msrv)
            # Read the resolved graph, not source string matches or a possibly stale lock file.
            command = [args.cargo, "metadata", "--locked", "--format-version", "1"] + (["--offline"] if args.offline else [])
            graph = json.loads(subprocess.check_output(command, cwd=ROOT, env=env))
            if any(p["name"] in {"rmcp", "rmcp-macros"} for p in graph["packages"]):
                raise RuntimeError("external MCP SDK dependency detected")
            checks.append({"group": "quality", "name": "independent-engine", "status": "passed", "command": command})
        if "features" in groups:
            for index, features in enumerate(FRAMEWORK_FEATURES):
                cargo("features", f"framework-{index}", "check", "--lib", "--no-default-features", "--features", features)
            for index, features in enumerate(PROTOCOL_FEATURES):
                cargo("features", f"protocol-{index}", "check", "--manifest-path", "crates/mcp-protocol/Cargo.toml", "--lib", "--no-default-features", "--features", features)
        if "tests" in groups:
            cargo("tests", "workspace", "test", "--workspace", "--all-features")
            cargo("tests", "examples", "build", "--all-features", "--examples")
            run("tests", "transport-smoke", [sys.executable, "tests/transport_smoke.py"])
            run("tests", "typed-stdio", [str(ROOT / "target/debug/examples/client_probe"), "--stdio", str(ROOT / "target/debug/examples/basic")])
        if "interop" in groups:
            for name in INTEROP:
                run("interop", name, [sys.executable, f"tests/{name}.py", "--go-reference", str(args.go_reference.resolve())])
            run("interop", "typed-go-stdio", [str(ROOT / "target/debug/examples/client_probe"), "--stdio", str(args.go_reference.resolve())])
        if report["source_sha256"] != fingerprint():
            raise RuntimeError("source changed during acceptance gate; rerun against the final source")
        report["status"] = "passed"
    except (Exception, KeyboardInterrupt) as error:
        report["status"] = "failed"
        report["error"] = str(error)
        print(f"FAIL: {error}", file=sys.stderr)
    finally:
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(f"{report['status'].upper()}: {len(checks)} checks; report: {args.report}", flush=True)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
