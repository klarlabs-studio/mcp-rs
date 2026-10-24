# Publishing to crates.io

Repository: https://github.com/klarlabs-studio/mcp-rs

| Package | Rust import | Version |
| --- | --- | --- |
| `klarlabs-mcp` | `mcp_rs` | `0.1.0` |
| `klarlabs-mcp-protocol` | `mcp_protocol` | `0.1.0` |

The unprefixed names are already owned by unrelated crates.io users (hyphens and
underscores are equivalent there). Both packages restrict publication to
`crates-io`; the framework aliases its versioned, repository-owned engine dependency.
Package contents explicitly include source, fixtures needed by shipped examples,
README files and license/provenance notices; build caches and CI metadata are excluded.

## First publication

1. Sign in to crates.io, verify your account email and create a short-lived API
   token scoped to publishing these two package names (including creating them).
2. Run `cargo login` locally to publish from a trusted checkout, or add the token
   as the repository Actions secret `CRATES_IO_TOKEN` for tag-based publication.
   Never commit tokens or paste them into issues/chat.
3. On Rust 1.90 or newer, run:

   ```sh
   cargo publish --locked --workspace --all-features --dry-run
   ```

   This packages and builds both crates against their packaged dependency,
   without uploading anything. CI requires this check alongside stable/MSRV tests.
   The library itself still supports Rust 1.89; workspace publication uses newer Cargo.
4. Review green required checks on the release commit, verify both manifest
   versions match, and create the matching `v0.1.0` tag. The tag workflow repeats
   package verification and the complete acceptance gate, publishes the protocol
   engine before the framework, and creates the GitHub source release.

   For an explicitly authorized local publication from a clean validated checkout:

   ```sh
   cargo publish --locked --workspace --all-features
   ```

5. Verify both registry versions and a fresh consumer build:
   https://crates.io/crates/klarlabs-mcp and
   https://crates.io/crates/klarlabs-mcp-protocol.

Publication is not implied by this document or by an enabled workflow. Registry
versions are authoritative. No acceptance or CI dry-run command uploads crates.

## Recovery and future releases

Cargo publishes in dependency order, but publishing multiple crates is not atomic.
If publication fails after the protocol package succeeds, verify that version and
its source/checksum first, then publish only the remaining framework package:

```sh
cargo publish --locked --all-features -p klarlabs-mcp
```

Do not blindly retry a whole release or bump versions to conceal a failed upload.
Published versions cannot be overwritten. For later releases, update both manifests
and the engine dependency requirement together, then run the same checks.

After the initial publication, crates.io trusted publishing can replace the token:
configure each crate for `klarlabs-studio/mcp-rs`, workflow `release.yml`, and update
the workflow to request `id-token: write` and exchange OIDC credentials. Until that
configuration exists on both crates, the explicit scoped repository secret is used.
