# Operations

This document describes delivery, command lifecycle, logging, readiness, and the
supplied systemd user services.

## Event and delivery semantics

`mailwake` treats source activity as a reason to reconcile current state. It
does not persist or replay individual events.

- Events during a debounce window are coalesced.
- Events that arrive during a command's cooldown are coalesced into a later
  request.
- Only one command in a lane runs at a time.
- Repeated requests for the same busy command become at most one follow-up run.
- Different lanes may run concurrently.
- Events that occur while the daemon is offline may be missed.

Use idempotent commands. Enable `run_on_startup` on `imap_idle`,
`gmail_api_poll`, or `fs_state` when startup should reconcile possible downtime.
A startup run is a normal command request: lanes, cooldowns, timeouts, and
coalescing still apply.

Configuration is not hot-reloaded. Restart the process after changing it.

## Command lifecycle

Configured commands and `state_cmd` run as:

```text
sh -c 'configured string'
```

They inherit the daemon's environment and working directory. Standard input is
`/dev/null`. Source payloads are not interpolated into the string.

Each command runs in a separate Unix session/process group. On timeout, output
limit, or daemon shutdown, `mailwake` terminates the group and reaps the shell so
child processes are not deliberately left behind.

A configured-command nonzero exit, signal, timeout, or output-limit breach is a
command outcome. It is logged and reported to the source/lane machinery but does
not by itself stop the daemon. An `fs_state` source does not accept a failed run
as reconciled; it remains dirty and can retry rather than recording a false
baseline.

## Failure classes

| Condition | Runtime behavior |
| --- | --- |
| Invalid or unreadable configuration | Startup exits nonzero with a safe path/reason message. |
| Transient IMAP, Gmail API, or network failure | Source retries with backoff. |
| Credential helper fails or remote authentication is rejected | Daemon stops; reauthorization-required failures use exit `78`. |
| Gmail API returns a permission rejection | Daemon stops with exit `78`. |
| IMAP rejects a mailbox/protocol command | Source reconnects with backoff. |
| Configured command fails or times out | Run fails; daemon remains active. |
| `state_cmd` fails | Source remains unreconciled and retries on later work. |
| Supervised task crashes or appears stale | Runtime health fails and systemd watchdog pings are withheld. |

The supplied units set `RestartPreventExitStatus=78`, so a classified
reauthorization problem remains visible instead of disappearing inside a
restart loop. Other failures may restart but are bounded by the unit's start
limit.

## Logging

The default log level is `info`. Configure the standard `tracing-subscriber`
filter through `RUST_LOG`:

```sh
RUST_LOG=mailwake=debug mailwake --config ~/.config/mailwake/config.toml
```

For a user service, use a drop-in rather than editing an installed unit:

```ini
# ~/.config/systemd/user/mailwake.service.d/logging.conf
[Service]
Environment=RUST_LOG=mailwake=debug
```

Then run:

```sh
systemctl --user daemon-reload
systemctl --user restart mailwake.service
journalctl --user -u mailwake.service -f
```

Auth-helper stdout/stderr is never logged. Configured command output is governed
by `output_mode`; the default `failure_tail` logs a bounded tail on failure.
Choose `silent` for commands whose output may contain secrets or private data.
Command, source, account, and mailbox names can appear in normal logs, along
with paths included in errors; do not put secrets or private identities in
those names.
See [Command output](configuration.md#command-output).

## Readiness

With systemd notification available, default startup sends `READY=1` after
configuration validation and source-task supervision. It intentionally does not
run a separate credential preflight. Consequently, default readiness means the
daemon is supervising its sources, not that every remote source is currently
connected.

`--initial-connect-required` delays readiness until every source completes its
initial setup:

- IMAP: login, read-only mailbox selection, and IDLE setup;
- Gmail API polling: one metadata baseline;
- `fs_state`: watcher installation and any configured startup baseline;
- `system_resume`: active systemd/logind D-Bus subscription.

Terminal initial failures, such as authentication or Gmail API permission
rejection, fail startup rather than reporting ready. Transient network and
protocol failures keep retrying with backoff, so `mailwake` has no internal
deadline for initial readiness. When using this option under systemd, set
`TimeoutStartSec` to the maximum time startup may remain pending.

After readiness, `run_on_startup` requests are released into the normal command
system.

## Watchdog and status

The supplied units use `Type=notify` and `WatchdogSec=2min`. `mailwake` sends
watchdog pings only while source watcher tasks and command lane tasks are alive
and making expected progress. A stale watcher, crashed lane, or command running
past its configured timeout makes runtime health fail and stops watchdog pings.

The daemon also publishes a generic `STATUS=` summary. It contains the stable
fragment `running commands: N` and only generic counts/state. It does not expose
command names or command strings. `StatusText` is informational; it does not
replace readiness, exit-status, or watchdog checks.

Without `NOTIFY_SOCKET` and `WATCHDOG_USEC`, the daemon runs normally in a
terminal or under another service manager. `--no-systemd` disables opportunistic
notification explicitly.

## Install the systemd user service

The basic unit expects the default binary and config locations:

```text
~/.local/bin/mailwake
~/.config/mailwake/config.toml
```

Install and start it with:

```sh
make install
make install-systemd
systemctl --user daemon-reload
systemctl --user enable --now mailwake.service
systemctl --user status mailwake.service
```

`make install-systemd` writes the unit with `ExecStart` derived from the current
`BINDIR`; it does not reload or enable the unit. The checked-in unit itself uses
the default `%h/.local/bin/mailwake` path when copied directly.
Make variables are not persisted between invocations, so pass the same custom
`PREFIX` or `BINDIR` to both `make install` and the systemd install target.

The daemon handles unavailable networks by reconnecting, so the example user
unit does not depend on a commonly absent user-level `network-online.target`.

If helpers or commands rely on `~/.local/bin` and the user manager does not have
that path, add a service drop-in:

```ini
[Service]
Environment=PATH=%h/.local/bin:/usr/local/bin:/usr/bin:/bin
```

Absolute helper and command paths remain more predictable.

To remove the example service, disable it before deleting its unit:

```sh
systemctl --user disable --now mailwake.service
make uninstall-systemd
systemctl --user daemon-reload
```

## Hardened systemd example

Install the alternative unit with:

```sh
make install-systemd-hardened
systemctl --user daemon-reload
systemctl --user restart mailwake.service
```

It adds:

```ini
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=read-only
ReadWritePaths=-%h/.cache -%h/.local/state
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX
LockPersonality=true
MemoryDenyWriteExecute=true
```

This is a starting point, not a universal sandbox. `ProtectHome=read-only`
prevents configured commands from modifying arbitrary home paths. Add the
narrowest `ReadWritePaths=` entries needed by your commands, and remove cache or
state exceptions that are not needed. A leading `-` makes an optional path
non-fatal when it does not exist.

Some password managers, OAuth helpers, desktop keyrings, and IPC mechanisms need
additional sockets, address families, or writable paths. Review the complete
command chain before enabling the hardened unit.

## Shutdown

SIGINT and SIGTERM request a clean shutdown. Running configured commands are
cancelled through their process groups, source tasks are asked to stop and are
given a bounded per-task grace period, and systemd receives `STOPPING=1` when
available. An in-flight credential helper is not explicitly shutdown-cancelled;
it remains bounded by `auth_helper_timeout_seconds`. SIGHUP does not reload
configuration.
