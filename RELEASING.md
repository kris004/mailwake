# Mailwake release details

This file records the repository-specific release contract. Stable versions use
`vMAJOR.MINOR.PATCH`. Prereleases use the overlay-compatible `alpha`, `beta`, or
`rc` SemVer suffix, optionally followed by a numeric sequence with no separator,
`.` or `-`. The version after the leading `v` must exactly match `Cargo.toml`.

## Protected release inputs

The release commit must be reachable from protected `main` with these checks
green:

- `Format, lint, and test`
- `Rust 1.95 compatibility`

The `refs/tags/v*` namespace must reject deletion and non-fast-forward updates,
with no bypass actor. GitHub release immutability must be enabled before the next
release is published.

## Published asset contract

Each release contains exactly these two project-built assets:

- `mailwake-VERSION-x86_64-unknown-linux-gnu-ubuntu-24.04.tar.gz`
- `SHA256SUMS`

The archive contains the dynamically linked x86-64 glibc/OpenSSL 3 binary,
documentation, examples, helper scripts, systemd units, project licenses,
third-party license notices, and Rust standard-library copyright notices. The
workflow attests both files and attaches those exact workflow-artifact bytes to
the release draft.

Mailwake does not publish a separate custom source archive. GitHub's generated
source archives remain available, and the Gentoo overlay consumes the archive
for the release commit rather than either binary asset.

## Workflow behavior

A signed annotated tag push runs `Release binaries`. Before building, the
workflow requires a direct tag-to-commit relationship, a valid GitHub OpenPGP
verification result, protected-branch reachability, the named checks above, and
no existing release for the tag. It tests, builds, packages, and uploads once;
later jobs attest and upload the same artifact files without rebuilding.

The workflow leaves a draft so its notes, prerelease/latest classification,
asset digests, and attestations can be reviewed. Publication is a separate
maintainer action after release immutability is confirmed. If a later job fails,
rerun only the failed jobs so the successful build artifact is reused. Never
replace an asset or move, delete, or recreate a public tag.

Running `Release binaries` manually from the default branch is the
non-publishing validation path. It performs the test, build, package, checksum,
and artifact-upload stages, but does not attest, create a tag, or create a
release.

`v0.1.0-beta.1` and `v0.1.0-beta.2` predate this contract. Their unsigned
annotated tags and mutable historical release state must not be rewritten.
