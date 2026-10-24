# Source distribution — 0.1.0

The locally validated distribution is a source bundle consumed through a Cargo
path dependency. Neither crate is published to a registry. Both packages retain
`publish = false`, and the framework deliberately uses its bundled protocol engine.

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

This source bundle is not a crates.io package. Registry publication requires a
chosen, available package identity for both crates, versioned engine dependency,
reviewed package contents, hosted CI and registry access. The GitHub repository is
[klarlabs-studio/mcp-rs](https://github.com/klarlabs-studio/mcp-rs); hosted checks
run there. The versioned local engine dependency is configured, but registry
package ownership and publication remain separate release work.

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
