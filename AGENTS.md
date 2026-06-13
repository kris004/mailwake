# Agent policy for this repository

Keep `mailwake` generic and safe to publish. This file is part of the public
repo, so it must also avoid maintainer-specific names, email addresses, hostnames,
and local paths.

- Do not commit private local config, OAuth client JSON, access tokens, real
  email addresses, or machine-specific paths.
- Use clearly fake placeholders in tracked docs, examples, and tests:
  - `user@example.com`
  - `/home/alice/.mail/example`
  - `/home/alice/.config/mailwake/config.toml`
  - `~/.mail/example`
  - `~/.config/mailwake/config.toml`
- Keep personal configs in ignored files such as `config.toml`,
  `*.local.toml`, `examples/*.local.toml`, or `.env`.
- Tests should use placeholders or temporary directories, not real user paths.
- `mailwake` is an event-driven command trigger. Do not add email-specific
  behavior or make Gmail, lieer, notmuch, Maildir, or aerc part of the core
  model.
- Before publishing or committing cleanup work, run a targeted `rg -i` search
  for known private usernames, email addresses, account names, and local paths.
- Validate Rust changes with `cargo fmt`, `cargo clippy --all-targets
  --all-features --locked -- -D warnings`, and `cargo test --locked`.
