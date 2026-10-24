"""Regenerate checked-in bindings. Requires Rust, protoc, and dependency access.
Normal library builds do not execute this tool or need protoc.
"""
import json
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
with tempfile.TemporaryDirectory(prefix="mcp-rs-codegen-") as directory:
    stage = Path(directory)
    (stage / "src").mkdir()
    shutil.copy2(ROOT / "proto/mcp.proto", stage / "mcp.proto")
    (stage / "Cargo.toml").write_text('[package]\nname="mcp-rs-codegen"\nversion="0.0.0"\nedition="2024"\n[dependencies]\ntonic-prost-build="=0.14.6"\n')
    # Connect is the RPC name: suppress Tonic's same-named channel constructor.
    (stage / "src/main.rs").write_text('fn main() -> Result<(), Box<dyn std::error::Error>> { tonic_prost_build::configure().build_transport(false).out_dir(' + json.dumps(str(stage)) + ').compile_protos(&["mcp.proto"], &["."])?; Ok(()) }\n')
    subprocess.run(["cargo", "run", "--quiet"], cwd=stage, check=True)
    generated = (stage / "mcp.v1.rs").read_text()
    header = '// Schema: klarlabs-studio/mcp-go, commit 52fa5746759d3284a6950e2667915d05ae2c6682\n// MIT Copyright (c) 2025 Felix Geelhaar; see LICENSE and UPSTREAM.md.\n'
    (ROOT / "src/transport/grpc_proto.rs").write_text(header + generated)
subprocess.run(["cargo", "fmt"], cwd=ROOT, check=True)
