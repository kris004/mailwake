# mailwake

`mailwake` is a small Unix daemon that turns events from configured sources
into controlled command runs. It debounces noisy signals, serializes related
commands, and supervises command timeouts and shutdown.

```text
source -> debounce/coalesce/cooldown -> command lane -> shell command
```

It was built for mail synchronization workflows, but mail is an integration,
not the core model. `mailwake` does not sync or store mail; sources emit wake-up
signals and user-supplied commands do the work.

## Status

`mailwake` is pre-1.0 and currently installed from source. Configuration and
runtime behavior may still change before a stable release. It targets Unix-like
systems and requires a POSIX-style `sh`; current CI tests Linux, while other
Unix-like systems are not yet verified. systemd notification and
`system_resume` support are Linux-specific. The Rust modules are implementation
details, not a stable library API.

## Sources

| Type | Signal | Notes |
| --- | --- | --- |
| `imap_idle` | An IMAP mailbox reports a change while in IDLE | Uses read-only mailbox selection. TLS and certificate verification are on by default. |
| `gmail_api_poll` | A Gmail history id advances | Polling, not push. Uses Gmail metadata/history APIs and an external OAuth token helper. |
| `fs_state` | Watched filesystem activity settles and optional opaque state changes | Filesystem events are interrupts, not a recursive synchronization queue. |
| `system_resume` | systemd/logind reports that the machine resumed | Linux-only; waits for a configurable settle period before triggering. |

All source types feed the same command-lane machinery. Commands in one lane do
not overlap; different lanes may run concurrently. Repeated triggers are
coalesced according to debounce, cooldown, and busy-lane rules.

## Delivery model

Source events are **wake-up signals, not durable work items**. `mailwake` does
not persist an event queue, and a signal that occurs while the daemon is offline
may not be replayed. Configure idempotent commands that reconcile current state
rather than assuming one command run per event. For `imap_idle`,
`gmail_api_poll`, and `fs_state`, use `run_on_startup = true` when a startup
reconciliation is required.

Configuration is loaded only at process start. Restart the daemon after changing
the file.

## Install from source

Requirements:

- Rust 1.95 or newer;
- a Unix-like operating system and `sh`;
- GNU Make for the `make` targets;
- the platform development libraries required by Rust's native TLS backend
  (`pkg-config` and OpenSSL development headers on Linux; package names vary).

From a source checkout:

```sh
make build
make test
make install
```

The default install location is `~/.local/bin/mailwake`. Override it with
`PREFIX=/usr/local` or `BINDIR=/some/bin`. `make uninstall` removes the binary;
the separate `uninstall-systemd` target removes an installed example unit.
The commands below assume the selected binary directory is on `PATH`; otherwise,
replace `mailwake` with its absolute path.

Make variables are not persisted. If you install a systemd unit for a custom
`PREFIX` or `BINDIR`, pass the same override to both `make install` and
`make install-systemd` (or `make install-systemd-hardened`).

There are currently no packaged binaries or package-manager releases.

## Quick start

Start with the generic placeholder configuration:

```sh
mkdir -p ~/.config/mailwake
cp examples/config.example.toml ~/.config/mailwake/config.toml
chmod 600 ~/.config/mailwake/config.toml
$EDITOR ~/.config/mailwake/config.toml
```

Replace every placeholder path and command, then validate and test the command
before starting the daemon:

```sh
mailwake check-config --config ~/.config/mailwake/config.toml
mailwake test-command --config ~/.config/mailwake/config.toml --command refresh-state
mailwake --config ~/.config/mailwake/config.toml
```

`check-config` parses and validates the configuration without connecting to
remote services or executing helpers, state commands, or configured commands.
Set `RUST_LOG=mailwake=debug` for more verbose runtime diagnostics from
`mailwake` without enabling debug logs in every dependency.

For a mail workflow, start with [`examples/gmail.example.toml`](examples/gmail.example.toml)
and the [Gmail/IMAP integration guide](docs/gmail.md).

## Security model

A `mailwake` configuration is trusted executable input:

- `cmd`, `state_cmd`, and credential helpers run through `sh -c` with the daemon
  user's privileges, environment, and working directory. `mailwake` does not
  interpolate source payloads into those command strings.
- Keep the configuration private (`0600`) and use a dedicated, least-privileged
  service account or user service where practical.
- Auth-helper stdout is treated as secret material and is never logged.
  Configured command output is different: the default `failure_tail` mode logs
  a bounded tail on failure. Use `output_mode = "silent"` for commands whose
  output may contain secrets; `journal` deliberately streams output.
- Prefer `xoauth2_cmd`, `gmail_token_cmd`, or `password_cmd` over a literal
  `password`. Keep OAuth clients and token caches out of the repository.
- Plaintext IMAP and invalid-certificate acceptance are explicit, warning-level
  opt-ins.

See [Security policy](SECURITY.md) for reporting vulnerabilities and
[Configuration reference](docs/configuration.md) for the exact output and TLS
controls.

## Runtime behavior

- Commands have bounded runtimes; captured-output modes also have byte limits.
  Timeout, capture-limit failure, or shutdown terminates the command's Unix
  process group. `journal` mode streams output without a capture limit.
- A command failure is a command outcome; it does not by itself stop the daemon.
- Authentication failures and Gmail API permission rejections are fail-stop
  errors. The daemon exits with status `78` so a supervisor can leave the
  problem visible instead of retrying forever. Other protocol failures,
  including IMAP mailbox command rejection, reconnect with backoff.
- Network and protocol failures reconnect with backoff.
- systemd readiness and watchdog support are opportunistic. The daemon also runs
  normally without `NOTIFY_SOCKET`.

See [Operations](docs/operations.md) for lane behavior, readiness, failure
semantics, logging, and the supplied user-service examples.

## CLI

```text
mailwake [--config PATH] [--no-systemd] [--initial-connect-required]
mailwake check-config [--config PATH]
mailwake test-command --command NAME [--config PATH]
```

The legacy `test-command --account NAME --mailbox NAME` form remains available
for legacy mailbox configuration. Run `mailwake --help` or a subcommand with
`--help` for the complete interface.

## Scope

`mailwake` intentionally does not:

- sync or store mail;
- understand lieer, notmuch, mbsync, Maildir, aerc, or Gmail label semantics;
- manage a browser/device-code OAuth flow;
- provide a durable job queue or general automation framework;
- recursively synchronize filesystem trees;
- install system sleep hooks or require root for resume events.

Keep the boundary narrow: source event in, configured command out.

## Documentation

- [Configuration reference](docs/configuration.md)
- [Operations and systemd](docs/operations.md)
- [Gmail/IMAP integration](docs/gmail.md)
- [Design notes](docs/design.md)
- [Generic example](examples/config.example.toml)
- [Gmail example](examples/gmail.example.toml)
- [Contributing](CONTRIBUTING.md)

## Development

```sh
cargo fmt --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --locked
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for repository scope, privacy rules, and
submission checks.

## License

Licensed under either the [Apache License, Version 2.0](LICENSE-APACHE) or the
[MIT License](LICENSE-MIT), at your option.

Unless explicitly stated otherwise, contributions intentionally submitted for
inclusion in this project are licensed under both licenses, without additional
terms or conditions.
