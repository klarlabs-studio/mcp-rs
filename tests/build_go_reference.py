"""Build the Go reference against the exact inspected upstream commit.
Usage: python3 tests/build_go_reference.py --upstream /path/to/mcp-go --output /tmp/mcp-go-reference
Go may download the upstream's declared dependencies during this explicit build.
"""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import tempfile

BASELINE = "52fa5746759d3284a6950e2667915d05ae2c6682"
ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    upstream = args.upstream.resolve()
    revision = subprocess.check_output(["git", "-C", str(upstream), "rev-parse", "HEAD"], text=True).strip()
    if revision != BASELINE:
        raise SystemExit(f"Reference must be pinned to {BASELINE}, got {revision}")
    if subprocess.check_output(["git", "-C", str(upstream), "status", "--porcelain"], text=True).strip():
        raise SystemExit("Reference checkout must be clean")
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="mcp-go-reference-") as directory:
        stage = Path(directory)
        for source in (ROOT / "tests/go-reference").glob("*.go"):
            shutil.copy2(source, stage / source.name)
        (stage / "go.mod").write_text('module mcp-rs-reference\n\ngo 1.26.0\n\nrequire go.klarlabs.de/mcp v0.0.0\n\nreplace go.klarlabs.de/mcp => ' + json.dumps(str(upstream)) + '\n')
        subprocess.run(["go", "mod", "tidy"], cwd=stage, check=True)
        subprocess.run(["go", "build", "-trimpath", "-o", str(output), "."], cwd=stage, check=True)
    print(f"Go reference: {revision} -> {output}")


if __name__ == "__main__":
    main()
