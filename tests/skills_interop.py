"""Compare bounded Agent Skills discovery and file reads against pinned Go."""
import argparse
import base64
import json
from pathlib import Path
from urllib.error import HTTPError
import urllib.request

from network_interop import ROOT, run_server

META = {
    "io.modelcontextprotocol/protocolVersion": "2026-07-28",
    "io.modelcontextprotocol/clientInfo": {"name": "skills-interop", "version": "1"},
    "io.modelcontextprotocol/clientCapabilities": {},
}
EXTENSION = "io.modelcontextprotocol/skills"
SCHEMA = "https://schemas.agentskills.io/discovery/0.2.0/schema.json"
SKILL_MD = "---\nname: git-workflow\ndescription: Follow team Git conventions\n---\n\n# Git\n\nUse feature branches.\n"
RELEASE_MD = "---\nname: release-checklist\ndescription: Prepare a safe release\n---\n\n# Release\n\nReview the changelog.\n"
ARCHIVE_ZIP = "UEsDBBQAAAAIAAAAIVADubX0VgAAAGoAAAAIAAAAU0tJTEwubWQ1y80NgCAMQOF7p2jiuQ7AzSEcoIEqDYgE0Bin1/hzft8jIki8iEEu1usudGoGJ9UWzU3XZHB4g8O74KRH24pgDRoj0H1D9wuAsQo2L5jZBp6/5aE9XFBLAQIUAxQAAAAIAAAAIVADubX0VgAAAGoAAAAIAAAAAAAAAAAAAACkAQAAAABTS0lMTC5tZFBLBQYAAAAAAQABADYAAAB8AAAAAAA="
ARCHIVE_TAR_GZ = "H4sIAAAAAAAC/+3UuwrCMBiG4cy5ioBzS1uqQjdBB1EXxQsIbdRQT7RVxKu3VrTQxcmi+D7LH/LnAIEvi8l4OnV3ifggr9QLw6qWmtXze/X4Md/3w0AoT7TglBc6K68U/8lxHLnXOxMpncUbezZO+Rzrq0xMHmf2WNjDPlKDRytRVU+t7KU4ZUblqd1u5f0E2XmukXKZG1VsjDrqONXr16ZqsSsFvsp8NBjORp/9AN7l3wv8Zv4Dv0v+29C5x9OtQ01AAQAAAAAAAAAAAAAAfssNeaEIQgAoAAA="
ARCHIVE_TGZ = "H4sIAAAAAAAC/+3TywrCMBCF4azzFAHXLa1WC+5cit2JDxDaqEFbJU1FfHrrFXHvDf9vMyEzw6zOdDLOsrAsxAtFrUGSnGvruUZxv3t/X/7TOOkJFYk3aGqvXXtS/KcgCGSlSzNU2uVLuzOBXxxkYerc2a23m2qoRpdGodqOmtu9b5xR9cqu1/K0LTu3CSlntVF+adRW5yu9uK6cR0Mp8IXmG1dqH/q9/1z+ozh5zn+axuT/HR4yTUIBAAAAAAAAAAAAAAB+0BHJLb/jACgAAA=="


def probe(endpoint):
    base = "http://" + endpoint + "/mcp"
    session = None
    sequence = 0

    def request(method, params):
        nonlocal session, sequence
        sequence += 1
        payload = {"jsonrpc": "2.0", "id": sequence, "method": method,
                   "params": dict(params, _meta=META)}
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "MCP-Protocol-Version": "2026-07-28",
            "Mcp-Method": method,
        }
        if "uri" in params:
            headers["Mcp-Name"] = params["uri"]
        if session:
            headers["Mcp-Session-Id"] = session
        req = urllib.request.Request(base, json.dumps(payload).encode(), headers)
        try:
            response = urllib.request.urlopen(req, timeout=5)
        except HTTPError as error:
            raise AssertionError(f"{method} HTTP {error.code}: {error.read().decode()}") from error
        with response:
            session = response.headers.get("Mcp-Session-Id", session)
            body = response.read().decode()
            if "text/event-stream" in response.headers.get("Content-Type", ""):
                body = [line[5:].strip() for line in body.splitlines() if line.startswith("data:")][-1]
            result = json.loads(body)
        assert result.get("id") == sequence, result
        assert "error" not in result, result
        return result["result"]

    discovery = request("server/discover", {})
    assert "2026-07-28" in discovery["supportedVersions"], discovery
    extensions = discovery.get("capabilities", {}).get("extensions", {})
    assert EXTENSION in extensions, discovery

    def read(uri):
        return request("resources/read", {"uri": uri})["contents"][0]

    index = read("skill://index.json")
    assert index["mimeType"] == "application/json", index
    parsed_index = json.loads(index["text"])
    assert parsed_index["$schema"] == SCHEMA, parsed_index
    skills = parsed_index["skills"]
    assert skills == [{
        "name": "archive-targz", "type": "archive", "description": "Archived skill archive-targz",
        "url": "skill://archive-targz.tar.gz",
    }, {
        "name": "archive-tgz", "type": "archive", "description": "Archived skill archive-tgz",
        "url": "skill://archive-tgz.tar.gz",
    }, {
        "name": "archive-zip", "type": "archive", "description": "Archived skill archive-zip",
        "url": "skill://archive-zip.zip",
    }, {
        "name": "git-workflow",
        "type": "skill-md",
        "description": "Follow team Git conventions",
        "url": "skill://git-workflow/SKILL.md",
    }, {
        "name": "release-checklist",
        "type": "skill-md",
        "description": "Prepare a safe release",
        "url": "skill://release-checklist/SKILL.md",
    }, {
        "type": "mcp-resource-template",
        "description": "Product documentation",
        "url": "skill://docs/{product}/SKILL.md",
    }], parsed_index

    template_entry = {
        "type": "mcp-resource-template",
        "description": "Product documentation",
        "url": "skill://docs/{product}/SKILL.md",
    }
    assert skills[-1] == template_entry, parsed_index

    listed_templates = request("resources/templates/list", {})["resourceTemplates"]
    assert any(item == {
        "uriTemplate": "skill://docs/{product}/SKILL.md",
        "name": "skill-template",
        "description": "Product documentation",
        "mimeType": "text/markdown",
    } for item in listed_templates), listed_templates
    listed_resources = request("resources/list", {})["resources"]
    assert all(item.get("uri") != "skill://docs/{product}/SKILL.md" for item in listed_resources), listed_resources

    entrypoint = read("skill://git-workflow/SKILL.md")
    notes = read("skill://git-workflow/notes.md")
    svg = read("skill://git-workflow/references/workflow.svg")
    metadata = read("skill://git-workflow/references/meta.json")
    binary = read("skill://git-workflow/icon.unknown")
    release_entrypoint = read("skill://release-checklist/SKILL.md")
    checklist = read("skill://release-checklist/templates/checklist.md")
    archive_zip = read("skill://archive-zip.zip")
    archive_targz = read("skill://archive-targz.tar.gz")
    archive_tgz = read("skill://archive-tgz.tar.gz")
    template_read = read("skill://docs/widget/SKILL.md")
    encoded_template_read = read("skill://docs/a%2Fb/SKILL.md")
    projection = {
        "extension_advertised": EXTENSION in extensions,
        "index": parsed_index,
        "entrypoint": {"mimeType": entrypoint["mimeType"], "text": entrypoint["text"]},
        "notes": {"mimeType": notes["mimeType"], "text": notes["text"]},
        "nested_svg": {"mimeType": svg["mimeType"], "text": svg["text"]},
        "nested_json": {"mimeType": metadata["mimeType"], "text": metadata["text"]},
        "binary": {"mimeType": binary["mimeType"], "base64": binary["blob"]},
        "second_entrypoint": {
            "mimeType": release_entrypoint["mimeType"], "text": release_entrypoint["text"]
        },
        "nested_markdown": {"mimeType": checklist["mimeType"], "text": checklist["text"]},
        "archives": [
            {"mimeType": item["mimeType"], "base64": item["blob"]}
            for item in (archive_zip, archive_targz, archive_tgz)
        ],
        "template": {"mimeType": template_read["mimeType"], "text": template_read["text"]},
        "encoded_template": {
            "mimeType": encoded_template_read["mimeType"], "text": encoded_template_read["text"]
        },
    }
    assert projection["entrypoint"]["text"] == SKILL_MD, projection
    assert projection["notes"] == {"mimeType": "text/markdown", "text": "# Notes\n"}, projection
    assert projection["nested_svg"] == {
        "mimeType": "image/svg+xml", "text": "<svg>workflow</svg>\n"
    }, projection
    assert projection["nested_json"] == {
        "mimeType": "application/json", "text": '{"kind":"workflow"}\n'
    }, projection
    assert base64.b64decode(projection["binary"]["base64"]) == bytes([0, 1, 2, 255]), projection
    assert projection["binary"]["mimeType"] == "application/octet-stream", projection
    assert projection["second_entrypoint"] == {
        "mimeType": "text/markdown", "text": RELEASE_MD
    }, projection
    assert projection["nested_markdown"] == {
        "mimeType": "text/markdown", "text": "# Checklist\n"
    }, projection
    assert projection["archives"] == [
        {"mimeType": "application/zip", "base64": ARCHIVE_ZIP},
        {"mimeType": "application/gzip", "base64": ARCHIVE_TAR_GZ},
        {"mimeType": "application/gzip", "base64": ARCHIVE_TGZ},
    ], projection
    assert projection["template"] == {"mimeType": "text/markdown", "text": "# Product\n\nwidget\n"}, projection
    assert projection["encoded_template"] == {
        "mimeType": "text/markdown", "text": "# Product\n\na%2Fb\n"
    }, projection
    return projection


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go-reference", type=Path, required=True)
    parser.add_argument("--report", type=Path, default=ROOT / "docs/skills-interop-report.json")
    args = parser.parse_args()
    results = {}
    for name, binary in [("rust", ROOT / "target/debug/examples/parity"),
                         ("go", args.go_reference.resolve())]:
        output = run_server(binary, "--skills-http", lambda endpoint: ["python3", __file__, "--probe", endpoint])
        results[name] = json.loads(output)
    assert results["rust"] == results["go"], results
    report = {
        "upstream_commit": "52fa5746759d3284a6950e2667915d05ae2c6682",
        "status": "directory_archive_and_skill_template_discovery_and_reads_match",
        "checks": [
            "Modern server/discover advertises io.modelcontextprotocol/skills",
            "skill://index.json contains both immediate-child directory skills in order",
            "resources/read returns matching root and nested text files with Markdown, SVG, and JSON MIME types",
            "resources/read returns matching unknown-extension binary MIME type and base64 payload",
            "skill index includes ZIP, .tar.gz, and .tgz archive entries with normalized .tgz URL",
            "resources/read returns exact matching archive bytes and ZIP/gzip MIME types",
            "skill://index.json advertises one parameterized template without a name",
            "resources/templates/list exposes template metadata and resources/list omits the template",
            "resources/read serves concrete template URIs with raw parameters and Markdown MIME",
        ],
        "limitations": [
            "Two immediate-child directory-backed skills, three archive-backed skills, one template, and twelve skill resource payloads plus the index are covered.",
            "Rust snapshots files when registering; Go reads them from disk per request, so post-registration disk changes are not compared.",
            "The Go fixture registers archive entries before directory entries in sorted order to match this Rust index projection; production registration-order behavior is not certified.",
            "Archive checks compare opaque payload bytes and MIME metadata only; archive contents are not unpacked or inspected by either server.",
            "The template check covers ordinary path substitution and preserves the raw percent-encoded capture a%2Fb; decoding and RFC 6570 edge cases are not certified.",
            "Malformed trees and archives, path validation, limits, errors, visibility, caching, and other transports are not compared across implementations.",
        ],
        "projection": results["rust"],
    }
    args.report.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    print("Pinned Go/Rust Agent Skills HTTP discovery and file reads passed")


if __name__ == "__main__":
    import sys
    if len(sys.argv) == 3 and sys.argv[1] == "--probe":
        print(json.dumps(probe(sys.argv[2]), sort_keys=True))
    else:
        main()
