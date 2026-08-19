# Contributing to mailwake

Thanks for your interest in improving `mailwake`. Contributions of all sizes are
welcome, including bug reports, documentation fixes, new tests, and code
changes.

## Before you start

Small fixes can go straight to a pull request. For a new source, a substantial
behavior change, or anything that changes delivery guarantees, consider opening
an issue first so the approach can be discussed before you invest significant
time.

Please report suspected security vulnerabilities using the process in
[SECURITY.md](SECURITY.md), rather than opening a public issue with sensitive
details.

The following documents provide useful context:

- [README.md](README.md) introduces the project and its supported sources;
- [docs/design.md](docs/design.md) explains the architecture and safety model;
- [docs/configuration.md](docs/configuration.md) describes the public
  configuration interface;
- [docs/operations.md](docs/operations.md) covers runtime and systemd behavior.

## Reporting bugs

Search the existing issues before opening a new one. A useful bug report
includes:

- the output of `mailwake --version` and your operating system;
- what you expected to happen and what happened instead;
- the smallest configuration or sequence of steps that reproduces the problem;
- a relevant, redacted log excerpt when one is available.

Please remove account names, addresses, local paths, command output, and other
personal data. Never post credential-helper output: it may be an access token.

## Development setup

Development requires Rust 1.95 or newer. On Linux, you will also need
`pkg-config` and the OpenSSL development headers. GNU Make is convenient but not
required for normal Cargo development. See [Install from
source](README.md#install-from-source) for the complete platform requirements.

Fork the repository, clone your fork, and build the project:

```sh
git clone https://github.com/YOUR-USER/mailwake.git
cd mailwake
cargo build --locked
cargo test --locked
```

Install the `rustfmt` and `clippy` components if your Rust toolchain does not
already include them:

```sh
rustup component add rustfmt clippy
```

You can run the development binary without installing it:

```sh
cargo run --locked -- --help
cargo run --locked -- check-config --config examples/config.example.toml
```

`check-config` does not connect to remote services or execute configured
commands. You do not need to install the binary or systemd unit, or configure a
real mail account, to build and test the project.

## Repository guide

- `src/main.rs` contains the CLI and daemon task wiring.
- `src/config.rs` defines and validates the configuration format.
- `src/imap.rs`, `src/gmail_api_poll.rs`, `src/fs_state.rs`, and
  `src/system_resume.rs` implement event sources.
- `src/command.rs`, `src/lane.rs`, and `src/process.rs` implement command
  scheduling and process lifecycle behavior.
- `examples/`, `docs/`, and `contrib/` contain public examples, documentation,
  the OAuth helper, and systemd units.

Most tests live beside the code they cover. Run one test or a group of related
tests while iterating, for example:

```sh
cargo test --locked --lib imap::tests::
```

## Making changes

`mailwake` turns source events into generic command triggers:

```text
source -> debounce/coalesce/cooldown -> command lane -> shell command
```

A source may be provider-specific, but mail synchronization, local indexing,
OAuth browser flows, and mail-client behavior should remain in configured
commands or external helpers. This keeps the daemon useful beyond any one mail
tool or workflow.

When changing observable behavior:

- add or update tests;
- update the relevant documentation and examples;
- preserve configuration compatibility unless a breaking change has been
  discussed;
- describe any effect on delivery, timeouts, logging, privileges, or rollback.

Configurations and logs may contain credentials or personal information. Use
fake identities such as `user@example.com`, paths under `/home/alice`, and
temporary directories in tests. Review your diff before submitting it, and do
not include personal configuration, OAuth data, tokens, passwords, or private
keys. If a real credential is committed accidentally, revoke it immediately and
follow [SECURITY.md](SECURITY.md); removing it in a later commit is not
sufficient.

## Checks before opening a pull request

Run the standard Rust checks:

```sh
cargo fmt --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --locked
```

Run `cargo fmt` to apply formatting if the formatting check fails. For
documentation changes, review the rendered Markdown and verify any links you
added or changed.

If you changed the OAuth shell helper, also run:

```sh
sh -n contrib/oauth/gmail-oauth-token-oauth2l
shellcheck contrib/oauth/gmail-oauth-token-oauth2l
```

If you changed a systemd unit or its Makefile installation logic, verify the
generated basic and hardened units against a temporary installation:

```sh
tmpdir=$(mktemp -d)
CARGO_TARGET_DIR="$tmpdir/target" make PREFIX="$tmpdir/prefix" install
"$tmpdir/prefix/bin/mailwake" --version
make PREFIX="$tmpdir/prefix" SYSTEMD_USER_DIR="$tmpdir/basic" install-systemd
systemd-analyze --user verify "$tmpdir/basic/mailwake.service"
make PREFIX="$tmpdir/prefix" SYSTEMD_USER_DIR="$tmpdir/hardened" \
  install-systemd-hardened
systemd-analyze --user verify "$tmpdir/hardened/mailwake.service"
rm -rf "$tmpdir"
```

GitHub Actions repeats these checks and also verifies the minimum Rust version,
package contents, installation targets, and generated systemd units.

## Opening a pull request

Please keep each pull request focused and include:

- the problem being solved;
- the approach taken and any user-visible behavior change;
- the tests or manual checks you ran;
- related issues, if any.

Draft pull requests are welcome when you want early feedback. Review comments
may ask for changes to behavior, tests, or documentation; that discussion is a
normal part of contributing.

## Licensing

Unless explicitly stated otherwise, any contribution intentionally submitted
for inclusion in this project, as defined in the Apache-2.0 license, is licensed
under both Apache-2.0 and MIT, without additional terms or conditions.
