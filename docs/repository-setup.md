# GitHub repository setup

Repository: https://github.com/klarlabs-studio/mcp-rs

The setup follows `klarlabs-studio/mcp-go` while retaining Rust-specific checks.
The repository is public with `main` as its default branch, issues/projects/wiki,
merge/squash/rebase options and automatic merge support. Source and licensing
remain independent of GitHub hosting; both crates still disable registry publishing.

## Main branch

The required `Required checks` job aggregates quality, stable/MSRV tests, both
feature matrices and pinned Go interoperability. It fails if any dependency fails
or is skipped. Strict up-to-date status checks, pull requests, stale review
invalidation, linear history and resolved conversations match mcp-go's policy.
Force pushes and branch deletion are disabled. As in mcp-go, no approving-review
count is required and administrator enforcement is disabled.

## Contributor tools

Rust issue forms and a PR template retain the upstream repository's structure.
`SECURITY.md` uses the established Klarlabs reporting address. The `triage` label
is present for incoming bug reports. Workflow token permissions default to read;
individual publication jobs request only their required write permissions.

## Documentation and releases

The Pages workflow builds Rust API documentation from both workspace packages,
with documentation warnings denied, and deploys it to
https://klarlabs-studio.github.io/mcp-rs/.

Version tags run a complete acceptance gate before publishing a GitHub source
release, acceptance report and SHA256 checksums. Both crate versions must match
the tag. This workflow does not publish crates to a registry.

The Relicta configuration follows conventional versioning and changelog settings
from mcp-go. AI notes are disabled; local defaults are dry-run, no Git push and
draft releases. `relicta version` reports 4.2.0 and `relicta health` validates the
configuration. No plan/bump/approve/publish lifecycle was executed during setup.

The Go reusable CI workflow and Warden check-skipping workflow are not copied:
required Rust statuses must reflect jobs actually executed for this source.
Current execution evidence is linked from GitHub Actions, not inferred from
local acceptance reports.
