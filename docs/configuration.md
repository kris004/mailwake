# Configuration reference

`mailwake` reads TOML from `~/.config/mailwake/config.toml` by default. Override
that path with `--config PATH`.

Unknown fields are rejected. The file is also trusted executable input because
commands and helpers run through `sh -c`; keep it private:

```sh
chmod 600 ~/.config/mailwake/config.toml
mailwake check-config --config ~/.config/mailwake/config.toml
```

`check-config` validates structure and references and checks simple auth-helper
executable paths when practical. It does not connect to remote services, install
filesystem watchers, notify systemd, or execute helpers, `state_cmd`, or
configured commands. Complex helper strings containing shell syntax are not
rejected merely because static validation cannot resolve an executable.

## Model

A modern configuration has three layers:

1. optional `[[accounts]]` entries hold IMAP connection and authentication
   settings;
2. `[[commands]]` entries define trusted shell commands and their execution
   lanes;
3. `[[sources]]` entries watch for changes and name the command to trigger.

```text
source -> debounce/coalesce/cooldown -> command lane -> shell command
```

Events are not persisted. Use idempotent reconciliation commands and enable
`run_on_startup` on supported sources when work missed during downtime must be
reconciled.

## Global settings

All fields are optional. Defaults are shown below.

| Field | Default | Constraint or meaning |
| --- | ---: | --- |
| `default_debounce_seconds` | `10` | Coalescing window before a source submits a command. Zero disables debounce. `fs_state` treats it as a quiet period; IMAP and Gmail measure it from the first event. |
| `default_max_debounce_seconds` | `60` | Maximum settling delay for `fs_state`; must be nonzero. |
| `auth_helper_timeout_seconds` | `30` | Credential-helper timeout; must be nonzero. |
| `auth_helper_max_output_bytes` | `65536` | Credential-helper output cap; must be nonzero. |
| `command_timeout_seconds` | `300` | Default command and `state_cmd` timeout; must be nonzero. |
| `command_output_max_bytes` | `1048576` | Default configured-command output cap; must be nonzero. |
| `command_output_tail_lines` | `100` | Default number of captured lines eligible for logging; must be nonzero. |
| `connect_timeout_seconds` | `30` | IMAP TCP connection timeout; must be nonzero. |
| `imap_operation_timeout_seconds` | `60` | TLS, greeting, auth, mailbox, and IDLE operation timeout; must be nonzero. |
| `idle_refresh_seconds` | `1740` | IMAP IDLE refresh interval; minimum `60`. |
| `watcher_stale_seconds` | `3600` | Watchdog stale threshold; at least twice `idle_refresh_seconds`. |
| `min_command_interval_seconds` | `60` | Default post-command cooldown. Zero disables the cooldown. |

Example:

```toml
default_debounce_seconds = 10
default_max_debounce_seconds = 60
auth_helper_timeout_seconds = 30
auth_helper_max_output_bytes = 65536
command_timeout_seconds = 300
command_output_max_bytes = 1048576
command_output_tail_lines = 100
connect_timeout_seconds = 30
imap_operation_timeout_seconds = 60
idle_refresh_seconds = 1740
watcher_stale_seconds = 3600
min_command_interval_seconds = 60
```

The deprecated global fields `capture_command_output` and
`log_command_output` still parse for compatibility. New configurations should
use per-command `output_mode`.

## Commands and lanes

```toml
[[commands]]
name = "refresh-state"
lane = "maintenance"
cmd = "/home/alice/.local/bin/refresh-example-state"
timeout_seconds = 300
min_interval_seconds = 60
output_mode = "failure_tail"
output_max_bytes = 1048576
output_tail_lines = 100
```

| Field | Required | Meaning |
| --- | --- | --- |
| `name` | yes | Unique command name referenced by sources. |
| `cmd` | yes | Trusted string executed with `sh -c`. |
| `lane` | no | Serialization lane. Defaults to the command's `name`. |
| `timeout_seconds` | no | Overrides `command_timeout_seconds`; must be nonzero. |
| `min_interval_seconds` | no | Overrides the global cooldown; zero disables it. |
| `output_mode` | no | `silent`, `failure_tail`, `tail`, `debug`, or `journal`. |
| `output_max_bytes` | no | Per-command capture cap; must be nonzero. |
| `output_tail_lines` | no | Per-command logged-tail limit; must be nonzero. |

Only one command in a lane runs at a time. A repeated request for the same
command while its lane is busy becomes one follow-up run. Commands in different
lanes may run concurrently.

Commands run with the daemon's environment and current working directory. Use
absolute paths or an explicit `cd` where behavior depends on either. On timeout
or clean shutdown, `mailwake` terminates the command's Unix process group and
reaps the shell.

### Command output

Credential-helper output is always secret and is never logged. Configured
command output follows `output_mode`:

- `silent`: do not capture stdout/stderr; log only lifecycle and status.
- `failure_tail`: default. Capture bounded output and log a bounded tail only on
  command failure, timeout, cancellation, or output-limit failure. Spawn/wait
  errors log failure metadata but have no captured tail to report.
- `tail`: log a bounded tail on success and failure.
- `debug`: make the bounded tail available only at debug log level.
- `journal`: stream stdout/stderr to the daemon's stdout/stderr. This bypasses
  tail capture and `output_max_bytes`, and may expose unbounded sensitive output
  in the journal.

If captured output exceeds `output_max_bytes`, the process group is terminated
and the run fails. Completion logs include generic command metadata and byte
counts, not the complete command environment. Use `silent` when command output
may contain credentials or private data. Command, source, account, and mailbox
names can appear in ordinary logs, as can error paths; do not put secrets or
private identities in those names.

## IMAP accounts

An `imap_idle` source references an account:

```toml
[[accounts]]
name = "example-imap"
host = "imap.example.com"
port = 993
username = "user@example.com"
auth = "password_cmd"
password_cmd = "/usr/bin/pass show mail/example-app-password"
```

| Field | Required | Meaning |
| --- | --- | --- |
| `name` | yes | Unique account name. |
| `host` | yes | IMAP server hostname. |
| `port` | no | Defaults to `993`. |
| `username` | yes | Authentication identity. |
| `auth` | yes | `xoauth2_cmd`, `password_cmd`, or `password`. |
| `xoauth2_cmd` | for `xoauth2_cmd` | Shell helper that prints one bearer token. |
| `password_cmd` | for `password_cmd` | Shell helper that prints one password. |
| `password` | for `password` | Literal password; discouraged except throwaway/local testing. |
| `insecure_plaintext` | no | Disable TLS; default `false`, logs a warning if enabled. |
| `danger_accept_invalid_certs` | no | Disable certificate verification; default `false`, logs a warning if enabled. |

Helper output has trailing CR/LF removed, is size- and time-bounded, and is never
logged. Helpers run only after the TCP/TLS connection and IMAP greeting succeed,
so an offline reconnect loop does not repeatedly refresh tokens before the
server is reachable. Prefer absolute helper paths under a service manager.

Usernames, mailbox names, and literal passwords containing CR/LF are rejected
before IMAP command construction.

## `imap_idle`

```toml
[[sources]]
name = "remote-inbox"
type = "imap_idle"
account = "example-imap"
mailbox = "INBOX"
on_event = "refresh-state"
run_on_startup = true
debounce_seconds = 10
```

Each source has its own IMAP connection so its mailbox can remain selected in
IDLE independently. Mailboxes are opened read-only with `EXAMINE`. Changes
submit `on_event`; message data is not passed to the command.

`run_on_startup` defaults to `false`. Enable it when the command should
reconcile changes that may have occurred while the daemon was offline.

Network and protocol timeouts reconnect with exponential backoff. IMAP
authentication rejection and credential-helper failures are fail-stop errors
rather than infinite reconnect conditions.

## `gmail_api_poll`

```toml
[[sources]]
name = "gmail-inbox"
type = "gmail_api_poll"
on_event = "refresh-state"
gmail_token_cmd = "/home/alice/.local/bin/gmail-api-token"
label_ids = ["INBOX"]
run_on_startup = true
debounce_seconds = 10
poll_interval_seconds = 60
api_timeout_seconds = 60
history_page_size = 100
```

This source polls Gmail over HTTPS. It requires an OAuth client/token source and
an external helper, but it does not require Pub/Sub, a webhook, or a public
endpoint. The intended scope is:

```text
https://www.googleapis.com/auth/gmail.metadata
```

It calls `users.getProfile` for the current history id. When that id advances,
it either triggers immediately or uses `users.history.list` to check configured
`label_ids`. It does not read message bodies, send, modify, or delete mail.

| Field | Default | Constraint or meaning |
| --- | ---: | --- |
| `gmail_token_cmd` | required | Helper that prints one OAuth bearer token. |
| `label_ids` | `[]` | Optional Gmail label-id filter. |
| `run_on_startup` | `false` | Submit the normal command once after startup readiness. |
| `debounce_seconds` | global default | Coalescing window measured from the first event; zero disables debounce. |
| `poll_interval_seconds` | `60` | Minimum `10`. |
| `api_timeout_seconds` | `60` | Must be nonzero. |
| `history_page_size` | `100` | Range `1..=500`. |

The history baseline is in memory and is established again after every process
start. Use `run_on_startup = true` when the external command must reconcile
possible downtime. If Gmail says a history baseline is too old, the source
triggers once and rebaselines so the external command can repair local state.
An explicit helper exit status `78`, HTTP 401, or a known permission rejection
stops the daemon. Other helper failures and Gmail quota or rate-limit responses
retry with exponential backoff.

See [Gmail/IMAP integration](gmail.md) for OAuth and sync-command examples.

## `fs_state`

```toml
[[sources]]
name = "local-state"
type = "fs_state"
watch_paths = ["/home/alice/.local/state/example"]
recursive = false
state_cmd = "cat /home/alice/.local/state/example/version"
on_change = "refresh-state"
run_on_startup = false
debounce_seconds = 5
max_debounce_seconds = 60
```

`watch_paths` must contain at least one path. Every path must already exist and
be accessible to the daemon when it starts; `check-config` validates the shape,
not live filesystem access. Watcher setup fails if a path cannot be watched.
`recursive` defaults to `false`. Path fields are filesystem paths, not shell
strings; use absolute paths rather than relying on shell tilde expansion.

Filesystem notifications are coalesced. If `state_cmd` is absent, any settled
batch submits `on_change`. If it is present:

1. startup captures its trimmed stdout as an opaque baseline;
2. a settled batch runs it once and compares the result;
3. a changed result submits `on_change`;
4. after a successful command, `mailwake` settles briefly, runs `state_cmd`
   again, and stores the new baseline.

The rebaseline step suppresses loops when the command itself changes watched
state. `state_cmd` output is not interpreted or logged, must be UTF-8, and is
capped at 1 MiB. It uses `command_timeout_seconds`.

Avoid recursive watches over large generated trees. Watch a small database,
version, or state path and let `state_cmd` summarize it.

## `system_resume`

```toml
[[sources]]
name = "system-resume"
type = "system_resume"
on_resume = "refresh-state"
settle_seconds = 15
```

On Linux with systemd/logind, this subscribes to
`org.freedesktop.login1.Manager.PrepareForSleep`. The transition back from sleep
waits `settle_seconds` (default `15`) and submits `on_resume`. Close-together
resume signals are coalesced. This source installs no system-sleep hooks and
requires no root privileges.

If the D-Bus subscription cannot be installed, the source is unhealthy. With
`--initial-connect-required`, startup fails instead of reporting ready.

## Startup commands

`imap_idle`, `gmail_api_poll`, and `fs_state` accept
`run_on_startup = true`. Startup requests use the same lane, timeout, cooldown,
and coalescing behavior as later events. They are released after watcher tasks
are supervised and `READY=1` can be reported.

For `fs_state`, the initial baseline is captured before its startup command. The
source then rebaselines after the command, preventing command-caused filesystem
events from creating a loop.

## Legacy mailbox form

The original mailbox form remains supported:

```toml
[[accounts]]
name = "example-imap"
host = "imap.example.com"
username = "user@example.com"
auth = "password_cmd"
password_cmd = "/usr/bin/pass show mail/example-app-password"

[[accounts.mailboxes]]
name = "INBOX"
on_notify = "cd ~/.mail/example && sync-command"
debounce_seconds = 10
```

New configurations should use named `[[commands]]` and top-level `[[sources]]`.
The legacy command name is derived from the account and mailbox and can be run
with:

```sh
mailwake test-command --account example-imap --mailbox INBOX
```

## Testing a command

Run a named command exactly as configured, including its timeout and output
policy:

```sh
mailwake test-command \
  --config ~/.config/mailwake/config.toml \
  --command refresh-state
```

Do not mix `--command` with the legacy `--account`/`--mailbox` target.
