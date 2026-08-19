# Contributing

Thank you for improving `mailwake`. Keep changes focused, generic, and safe for a
public repository.

## Scope

`mailwake` is an event-driven command trigger:

```text
source -> debounce/coalesce/cooldown -> command lane -> shell command
```

Provider-specific sources may produce generic wake-up signals, but mail clients,
sync engines, local indexes, OAuth browser flows, and application-specific
business logic stay outside the core. Do not make Gmail, lieer, notmuch, Maildir,
aerc, or another integration part of the core model.

## Privacy

Never commit:

- local configuration or `.env` files;
- OAuth client JSON, access tokens, refresh tokens, passwords, or private keys;
- real email addresses, account names, hostnames, usernames, or home paths;
- generated output containing private data.

Use obvious placeholders such as:

```text
user@example.com
/home/alice/.mail/example
/home/alice/.config/mailwake/config.toml
~/.mail/example
~/.config/mailwake/config.toml
```

Use temporary directories in tests. Keep personal configurations in ignored
files such as `config.toml`, `*.local.toml`, `examples/*.local.toml`, or `.env`.
Before submitting, search the changed files and reachable history for any
private identifiers relevant to your environment.

## Development checks

Rust 1.95 or newer is required. Run:

```sh
cargo fmt
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --locked
```

Also validate the shell helper when it changes:

```sh
sh -n contrib/oauth/gmail-oauth-token-oauth2l
shellcheck contrib/oauth/gmail-oauth-token-oauth2l
```

For systemd unit changes, run:

```sh
systemd-analyze --user verify \
  contrib/systemd/mailwake.service \
  contrib/systemd/mailwake-hardened.service
```

Review `git diff --check` and the exact staged paths before committing. Do not
include unrelated cleanup in a focused change.

## Tests and documentation

- Add or update tests for observable behavior changes.
- Prefer named commands and top-level sources in new examples; the nested
  mailbox form is compatibility-only.
- Document delivery, timeout, logging, privilege, and rollback implications.
- Keep examples runnable in principle but use only fake paths and identities.
- Do not claim a source is durable or push-based when it polls or keeps only an
  in-memory baseline.

## Licensing

Unless explicitly stated otherwise, any contribution intentionally submitted
for inclusion in this project, as defined in the Apache-2.0 license, is licensed
under both Apache-2.0 and MIT, without additional terms or conditions.
