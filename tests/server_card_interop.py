"""Compare bounded Server Card / catalog HTTP behavior against pinned Go."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from network_interop import ROOT, run_server


def probe(endpoint):
    base = "http://" + endpoint
    documents = {}
    for path, media in [("/mcp/server-card", "application/mcp-server-card+json"),
                        ("/.well-known/ai-catalog.json", "application/ai-catalog+json"),
                        ("/.well-known/mcp/catalog.json", "application/ai-catalog+json")]:
        with urlopen(base + path, timeout=3) as response:
            body = response.read()
            assert response.status == 200
            assert response.headers["Content-Type"] == media
            assert response.headers["Cache-Control"] == "public, max-age=3600"
            assert response.headers["Access-Control-Allow-Origin"] == "*"
            etag = response.headers["ETag"]
            assert etag == '"' + hashlib.sha256(body).hexdigest()[:32] + '"'
            documents[path] = json.loads(body)
        with urlopen(Request(base + path, method="HEAD"), timeout=3) as response:
            assert response.status == 200 and response.read() == b""
            assert response.headers["ETag"] == etag
        try:
            urlopen(Request(base + path, headers={"If-None-Match": etag}), timeout=3)
            raise AssertionError("conditional request did not return 304")
        except HTTPError as response:
            assert response.code == 304 and response.read() == b""
        try:
            urlopen(Request(base + path, data=b"", method="POST"), timeout=3)
            raise AssertionError("POST was accepted")
        except HTTPError as response:
            assert response.code == 405
    card = documents["/mcp/server-card"]
    assert card["name"] == "local/card-fixture"
    assert card["remotes"] == [{"type": "streamable-http", "url": "https://example.test/mcp", "supportedProtocolVersions": ["2026-07-28"]}]
    catalog = documents["/.well-known/ai-catalog.json"]
    assert catalog == documents["/.well-known/mcp/catalog.json"]
    # Go derives an absolute URL from Host; Rust uses the explicit relative URL.
    assert catalog["entries"][0]["url"] in [base + "/mcp/server-card", "/mcp/server-card"]
    catalog["entries"][0]["url"] = "/mcp/server-card"
    print(json.dumps({"card": card, "catalog": catalog}, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go-reference", type=Path)
    parser.add_argument("--probe")
    args = parser.parse_args()
    if args.probe:
        probe(args.probe)
        return
    if not args.go_reference:
        parser.error("--go-reference is required")
    results = []
    for binary in [ROOT / "target/debug/examples/parity", args.go_reference.resolve()]:
        output = run_server(binary, "--server-card-http", lambda endpoint: [sys.executable, __file__, "--probe", endpoint])
        results.append(json.loads(output))
    assert results[0] == results[1], results
    report = {"upstream_commit": "52fa5746759d3284a6950e2667915d05ae2c6682",
              "status": "bounded_card_catalog_http_checks_passed",
              "checks": ["Matching modern card JSON and normalized catalog JSON", "GET/HEAD, media types, cache/CORS headers, per-body SHA256 ETag, exact conditional 304, POST405, catalog aliases"],
              "limitations": ["Rust requires explicit card URL and catalog identifier; Go derives an absolute URL from Host by default. Comparison normalizes that fixture URL only.", "Rust accepts weak/list/wildcard If-None-Match and advertises GET, HEAD in CORS; Go checks exact ETag and advertises GET.", "No full schema certification, icon encoding equivalence, proxy/TLS/browser/preflight certification, or live card synchronization."]}
    (ROOT / "docs/server-card-interop-report.json").write_text(json.dumps(report, indent=2) + "\n")
    print("Pinned Go/Rust Server Card and catalog HTTP checks passed")


if __name__ == "__main__":
    main()
