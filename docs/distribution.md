# Source distribution — 0.1.0

The locally validated distribution is a source bundle consumed through a Cargo
path dependency. Registry packaging is also configured as `klarlabs-mcp` and
`klarlabs-mcp-protocol`; Rust imports retain their original names. The framework
uses its own protocol engine in both source and registry distributions.
See [publishing](publishing.md) for the registry release process and status links.

The bundle includes `Cargo.lock`, the framework, `crates/mcp-protocol`, examples,
fixtures, CI configuration, documentation and upstream license/provenance notices.
It excludes Git internals, build output and local reference checkouts. Included
TLS fixture keys are public loopback-only test identities.

The adjacent JSON manifest records every bundled file's SHA256, the archive's
SHA256 and the source fingerprint from the completed acceptance run. Verify the
archive before extraction, then use its root as a local Cargo dependency:

```toml
[dependencies]
mcp-rs = { path = "../mcp-rs-0.1.0" }
```

This historical source bundle is separate from the crates.io packages generated
by Cargo. Both registry packages use explicit file allowlists and retain their
upstream licensing/provenance notices. CI verifies the complete workspace package
dry run, including the versioned engine dependency, before release.
The GitHub repository is [klarlabs-studio/mcp-rs](https://github.com/klarlabs-studio/mcp-rs).
A configured workflow is not proof of publication; verify versions on crates.io.

Release evidence is in [acceptance-report.json](acceptance-report.json). The
source-bundle consumer check is recorded separately in the distribution manifest;
it does not replace the full protocol and interoperability acceptance suite.

A standalone consumer fixture is shipped beside the archive. Extract the bundle
in that same directory and run:

```sh
tar -xzf mcp-rs-0.1.0-source.tar.gz
cargo run --locked --manifest-path consumer/Cargo.toml
```

Add `--offline` when dependencies are already cached. The fixture uses only the
public `mcp-rs` API, a framed in-memory connection and a ten-second runtime bound.
It checks modern discovery, a typed tool call and awaited shutdown. Its source,
lockfile and run log are checksummed in the adjacent manifest.
