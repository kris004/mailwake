# mailwake

`mailwake` is a tiny event-driven command trigger. It watches configured event
sources, debounces/coalesces noisy notifications, and runs configured shell
commands.

```text
event source -> debounce/coalesce/cooldown -> command lane -> run configured command
```

IMAP IDLE, Gmail API polling, `fs_state`, and `system_resume` are source types.
The daemon intentionally treats all source events as wake-up signals, not as
work-item queues.

## Non-goals

`mailwake` does **not**:

- sync mail directly;
- store mail;
- implement a mail client;
- know about lieer, notmuch, mbsync, Gmail labels, Maildir, or aerc internals;
- implement a browser/device-code OAuth flow;
- act as a general automation framework;
- recursively crawl or sync local filesystem trees;
- install system sleep hooks or require root privileges for resume events.

Keep the daemon boring: event in, command out.

## Authentication model

Secrets are not stored by `mailwake` by default. Long-running credential refresh
is delegated to external commands:

- `xoauth2_cmd` prints a fresh OAuth2 bearer token to stdout.
- `gmail_token_cmd` prints a fresh Gmail API OAuth2 bearer token to stdout.
- `password_cmd` prints a password or app password to stdout.
- direct `password` exists only for local tests and throwaway accounts; it logs a
  loud warning without printing the value.

The daemon trims trailing CR/LF from helper output and never logs helper output,
OAuth tokens, passwords, command stdout/stderr, environment variables, or full
systemd status details. IMAP auth helpers are invoked only after TCP/TLS
connection and the server greeting succeed, so offline/reconnect loops do not
repeatedly refresh OAuth tokens before the IMAP server is reachable. Gmail API
token helpers run only when establishing the poll baseline or making a poll
request. Helpers run with `auth_helper_timeout_seconds`; if they time out or
exceed `auth_helper_max_output_bytes`, `mailwake` terminates the helper's Unix
process group so helper children are not left behind. OAuth token storage and
refresh should be handled by the helper command. If an auth helper exits with
status `78`, `mailwake` treats that as a user-action-required auth failure,
exits with status `78`, and lets systemd keep the unit failed instead of
retrying forever.

For Gmail, `imap_idle` with `xoauth2_cmd` is the true event-driven option but
uses Google's broad IMAP/SMTP Gmail scope. `gmail_api_poll` is the lower-scope
local option; it uses Gmail API metadata/history polling and does not require
any Google Cloud setup. App-password based accounts can
use `password_cmd`. A full OAuth browser/device-code flow is intentionally
outside this daemon.

## TLS defaults

TLS is required by default and certificates are verified by default. Plaintext
IMAP requires an explicit opt-in:

```toml
insecure_plaintext = true
```

Disabling certificate verification also requires an explicit opt-in:

```toml
danger_accept_invalid_certs = true
```

Both options log warnings and should be avoided unless you know why you need
them.

## Configuration

Default config path:

```text
~/.config/mailwake/config.toml
```

Preferred Gmail XOAUTH2 example:

```toml
default_debounce_seconds = 10

[[accounts]]
name = "gmail"
host = "imap.gmail.com"
port = 993
username = "user@example.com"
auth = "xoauth2_cmd"
xoauth2_cmd = "/home/alice/.local/bin/gmail-oauth-token"

[[accounts.mailboxes]]
name = "INBOX"
on_notify = "cd ~/.mail/example && flock -n .sync.lock gmi sync && notmuch new"
debounce_seconds = 10
```

Password command example:

```toml
default_debounce_seconds = 10

[[accounts]]
name = "gmail"
host = "imap.gmail.com"
port = 993
username = "user@example.com"
auth = "password_cmd"
password_cmd = "/usr/bin/pass show mail/gmail-app-password"

[[accounts.mailboxes]]
name = "INBOX"
on_notify = "cd ~/.mail/example && flock -n .sync.lock gmi sync && notmuch new"
debounce_seconds = 10
```

Each configured mailbox gets its own IMAP connection so it can remain selected
and in IDLE independently.

Gmail API polling example:

```toml
[[commands]]
name = "remote-sync"
lane = "example-sync"
cmd = "cd /home/alice/.mail/example && flock -n .sync.lock sync-command"

[[sources]]
name = "gmail-inbox"
type = "gmail_api_poll"
on_event = "remote-sync"
gmail_token_cmd = "/home/alice/.local/bin/gmail-api-token"
label_ids = ["INBOX"]
run_on_startup = true
debounce_seconds = 10
poll_interval_seconds = 60
```

This source is not push/event based like IMAP IDLE. It polls Gmail metadata,
compares Gmail history ids, optionally checks whether changed history matches
configured labels, and then submits the normal `on_event` command.

Useful global knobs:

```toml
# Defaults shown.
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

`idle_refresh_seconds` must be at least 60. `watcher_stale_seconds` must be at
least twice `idle_refresh_seconds`. Auth-helper, command, connect, and IMAP
operation timeouts must be nonzero. `auth_helper_max_output_bytes` and
`command_output_max_bytes` must also be nonzero.

`min_command_interval_seconds` is a post-command cooldown that prevents obvious
self-trigger loops. If events arrive during the cooldown, they are coalesced and
one command run is scheduled after the cooldown/debounce rules allow it. Set it
to `0` only if you intentionally want no cooldown.

Named commands may override the global timeout/cooldown and may share a lane:

```toml
[[commands]]
name = "remote-sync"
lane = "mail-sync"
cmd = "sync-remote"
timeout_seconds = 300
min_interval_seconds = 60
output_mode = "failure_tail"
output_tail_lines = 100

[[commands]]
name = "local-push"
lane = "mail-sync"
cmd = "push-local"
timeout_seconds = 300
min_interval_seconds = 30
output_mode = "failure_tail"
output_tail_lines = 100
```

Only one command in a lane runs at a time. Repeated requests for the same
command while the lane is busy are coalesced into one follow-up run. Commands in
different lanes can run independently.

`debounce_seconds = 0` is allowed when explicitly configured, but it means every
observed source event can trigger command execution as soon as cooldown and the
previous run permit.

### Notification command output

Auth helper output is secret and is never logged. Notification command output is
not automatically a secret, but it may contain sensitive mail data, so command
output logging is configurable and capped.

Named commands support:

```toml
output_mode = "failure_tail"
output_max_bytes = 1048576
output_tail_lines = 100
```

`output_mode` values:

- `silent`: log command start, completion status, duration, and exit
  status/signal only. Do not capture or log stdout/stderr.
- `failure_tail`: the default. Capture stdout/stderr up to `output_max_bytes`
  and log only the last `output_tail_lines` on command failure, timeout,
  shutdown cancellation, output-limit failure, or spawn/wait error.
- `tail`: capture stdout/stderr up to `output_max_bytes` and log the last
  `output_tail_lines` on both success and failure.
- `debug`: capture stdout/stderr up to `output_max_bytes` and log the last
  `output_tail_lines` at debug level only.
- `journal`: stream child stdout/stderr to `mailwake` stdout/stderr so
  systemd/journald can capture it live. This is useful for debugging but may
  expose sensitive mail-related output.

If captured command output exceeds `output_max_bytes`, `mailwake` terminates the
command process group and reports a command failure. It still logs at most the
configured tail, never the entire captured output. Completion logs include the
command name, duration, status, output mode, byte counts, and whether output was
captured, truncated, suppressed, or streamed to the journal. `mailwake` does not
log the full command environment or put the full shell command in systemd status
messages.

Deprecated `capture_command_output` and `log_command_output` config fields still
parse for compatibility, but new configs should use per-command `output_mode`.

Example command with the safe default made explicit:

```toml
[[commands]]
name = "gmail-local-push"
lane = "gmail-sync"
cmd = "cd /home/alice/.mail/example && flock -n .sync.lock gmi push"
timeout_seconds = 300
output_mode = "failure_tail"
output_tail_lines = 100
```

Usernames, mailbox names, and direct LOGIN passwords must not contain CR/LF.
This prevents unsafe IMAP command construction before anything is sent to the
server.

Legacy `[[accounts.mailboxes]]` entries with `on_notify = "..."` remain
supported. New configs can instead define named `[[commands]]` and top-level
`[[sources]]` entries:

```toml
[[sources]]
name = "remote-inbox"
type = "imap_idle"
account = "gmail"
mailbox = "INBOX"
on_event = "remote-sync"
run_on_startup = false
debounce_seconds = 10
```

The `imap_idle`, `gmail_api_poll`, and `fs_state` source types can also set
`run_on_startup = true` to queue their normal configured command once when the
daemon starts. This is generic: `imap_idle` and `gmail_api_poll` sources queue
`on_event`, and `fs_state` sources queue `on_change`. Startup commands use the
same command lanes, timeouts, cooldowns, and coalescing as event-triggered
commands; commands in the same lane still do not overlap.

`mailwake` reports `READY=1` after source tasks are supervised. After that,
`run_on_startup` commands are released into the normal command system. For
`fs_state` sources with `state_cmd`, the startup baseline is captured before
the startup command runs. If that command changes watched paths, `fs_state`
uses the same self-trigger suppression and rebaseline behavior as a normal
source-owned command.

## `gmail_api_poll` sources

`gmail_api_poll` is the local-only Gmail API source for users who want narrower
OAuth than Gmail IMAP. It uses the Gmail API metadata scope:

```text
https://www.googleapis.com/auth/gmail.metadata
```

The poller calls Gmail `users.getProfile` to read the account's current
`historyId`. When that id advances, it either queues the configured `on_event`
command immediately or, when `label_ids` is configured, calls
`users.history.list` with each configured label id and only queues the event if
matching history exists. It does not read message bodies, send mail, modify
mail, delete mail, or understand what the sync command does.

Useful source knobs:

```toml
poll_interval_seconds = 60
api_timeout_seconds = 60
history_page_size = 100
```

`poll_interval_seconds` must be at least 10 seconds. Gmail history baselines can
expire; if Gmail returns that the stored history id is too old, `mailwake`
triggers once and rebaselines so the external sync command can repair local
state.

## `fs_state` sources

`fs_state` watches one or more configured files/directories for noisy filesystem
changes. Filesystem events are interrupts, not work items: many filesystem
events are coalesced into one settled state check.

```toml
[[sources]]
name = "local-state"
type = "fs_state"
watch_paths = ["/home/alice/.mail/example/.notmuch"]
recursive = false
state_cmd = "cd /home/alice/.mail/example && notmuch count --lastmod"
on_change = "local-push"
run_on_startup = false
debounce_seconds = 5
max_debounce_seconds = 60
```

On startup, if `state_cmd` is set, `mailwake` runs it once and saves stdout as an
opaque baseline after trimming trailing newlines. It does not interpret or log
the full output. After a settled filesystem batch, `mailwake` runs `state_cmd`
once and compares stdout to the previous baseline. If it changed, `mailwake`
runs `on_change`; after that command finishes, it waits briefly, runs
`state_cmd` once more, and saves the new baseline. This rebaseline step prevents
self-trigger loops when the command itself updates watched state.

If no `state_cmd` is configured, any settled filesystem event can trigger
`on_change`.

`state_cmd` runs from the daemon process environment and current working
directory. Under systemd, that may not match your interactive shell. Prefer
absolute paths or an explicit `cd` in commands whose result depends on cwd. A
plain `notmuch count --lastmod` is acceptable only when the service environment
already points at the intended database, or when the command is independent of
cwd.

Useful `state_cmd` examples:

```sh
cd /home/alice/.mail/example && notmuch count --lastmod
cat state/version
sqlite3 app.db 'select max(updated_at) from messages'
sha256sum important-state-file
```

`mailwake` does not understand any of these outputs; it only compares strings.

Warnings:

- Do not recursively watch large Maildir trees.
- For notmuch, watch `.notmuch` or another small database/state path, not every
  email file.
- For huge mailboxes, rely on coalescing: let the watcher see a small state path
  and run `state_cmd` once per settled batch.

## `system_resume` sources

`system_resume` is a generic wake/resume event source. On Linux systems with
systemd/logind, it subscribes to the D-Bus
`org.freedesktop.login1.Manager.PrepareForSleep` signal. `PrepareForSleep(true)`
records that the system is entering sleep; `PrepareForSleep(false)` is treated
as resume/wake. After resume, `mailwake` waits `settle_seconds` before queuing
the configured command.

This is useful on laptops because network sessions, including IMAP IDLE
connections, may be stale after suspend/resume even when no fresh IMAP event is
delivered. The source remains generic: it only observes a system resume event
and triggers `on_resume`.

```toml
[[commands]]
name = "remote-sync"
lane = "example-sync"
cmd = "cd /home/alice/.mail/example && flock -n .sync.lock gmi sync && notmuch new"
timeout_seconds = 300
min_interval_seconds = 60

[[sources]]
name = "system-resume"
type = "system_resume"
on_resume = "remote-sync"
settle_seconds = 20
```

`settle_seconds` defaults to 15. Multiple resume signals close together are
coalesced into one settled command request. The request uses the same command
lanes, cooldowns, timeouts, and shutdown behavior as every other source; it does
not bypass lane serialization and does not run overlapping commands.

`system_resume` is optional. If no `system_resume` source is configured,
`mailwake` does not connect to D-Bus. If a configured `system_resume` source
cannot connect to systemd/logind D-Bus, that source is unhealthy; with
`--initial-connect-required`, startup fails instead of reporting ready. It does
not use `/usr/lib/systemd/system-sleep` hooks and does not require root.

## CLI

```sh
mailwake --config ~/.config/mailwake/config.toml
mailwake check-config --config ~/.config/mailwake/config.toml
mailwake test-command --config ~/.config/mailwake/config.toml --command local-push
mailwake test-command --config ~/.config/mailwake/config.toml --account example-imap --mailbox INBOX
mailwake --no-systemd --config ~/.config/mailwake/config.toml
mailwake --initial-connect-required --config ~/.config/mailwake/config.toml
```

`check-config` parses and validates the config, checks simple auth-helper
executable paths when practical, and does not connect to IMAP, start filesystem
watchers, notify systemd, run auth helpers, run `state_cmd`, or run configured
commands. More complex helper commands using shell syntax such as `~`, pipes,
redirection, `&&`, or `;` are not rejected just because static validation cannot
prove the executable path.

Prefer absolute paths for `xoauth2_cmd` and `password_cmd`, especially under
systemd. A user service may not inherit the same `PATH` as your interactive
shell, so a helper that works in a terminal as `gmail-oauth-token` may fail in
the service unless configured as `/home/alice/.local/bin/gmail-oauth-token`
or the service explicitly sets a suitable `PATH`.

`test-command --command NAME` runs a named `[[commands]]` entry with its
configured timeout. The legacy `test-command --account NAME --mailbox NAME`
form still runs the configured command for one account/mailbox; for top-level
`imap_idle` sources, it resolves the source's named `on_event` command. Do not
mix `--command` with `--account`/`--mailbox`.

## Basic setup

```sh
mkdir -p ~/.config/mailwake
$EDITOR ~/.config/mailwake/config.toml

mailwake check-config --config ~/.config/mailwake/config.toml
mailwake test-command --config ~/.config/mailwake/config.toml --command local-push
mailwake test-command --config ~/.config/mailwake/config.toml --account example-imap --mailbox INBOX
mailwake --config ~/.config/mailwake/config.toml
```

See `examples/config.example.toml` for a complete placeholder config. Copy it to
a private config path and replace example email addresses, paths, and commands
with your own values before use.

## Gmail + lieer + notmuch

This is an example recipe, not the purpose of the tool. `mailwake` does not
understand Gmail, lieer, notmuch, Maildir, labels, or email semantics; it only
turns source events into command runs.

Use a Gmail OAuth helper that prints a valid bearer token:

```toml
auth = "xoauth2_cmd"
xoauth2_cmd = "/home/alice/.local/bin/gmail-oauth-token"
```

### Example oauth2l helper

Google's `oauth2l` can be used as the external token helper. `mailwake` does not
call `oauth2l` directly; install or adapt the example wrapper instead:

```sh
install -m 0755 contrib/oauth/gmail-oauth-token-oauth2l ~/.local/bin/gmail-oauth-token
mkdir -p ~/.config/mailwake ~/.local/state/mailwake
chmod 700 ~/.local/state/mailwake
$EDITOR ~/.config/mailwake/google-oauth-client.json
gmail-oauth-token --setup
gmail-oauth-token >/tmp/gmail-token-test
rm /tmp/gmail-token-test
```

The wrapper uses the IMAP/SMTP Gmail scope `https://mail.google.com/` by
default, stores the OAuth client JSON under `~/.config/mailwake/`, and stores
the `oauth2l` token cache under `~/.local/state/mailwake/` instead of oauth2l's
default `~/.oauth2l` file. Run `--setup` interactively once before starting the
systemd service. If Google reports that the cached refresh token is expired or
revoked, the wrapper exits `78`. The wrapper also supports `--reauth` for
systemd failure hooks: it resets the token cache, runs the OAuth flow, and
suppresses token output so successful reauth does not write access tokens to the
journal.

For `gmail_api_poll`, wrap the same helper with the narrower Gmail metadata
scope and a separate cache:

```sh
cat > /home/alice/.local/bin/gmail-api-token <<'EOF'
#!/bin/sh
set -eu
export MAILWAKE_GMAIL_SCOPE='https://www.googleapis.com/auth/gmail.metadata'
export MAILWAKE_OAUTH2L_CACHE="${XDG_STATE_HOME:-$HOME/.local/state}/mailwake/gmail-api-metadata-oauth2l-cache.json"
exec /home/alice/.local/bin/gmail-oauth-token "$@"
EOF
chmod 700 /home/alice/.local/bin/gmail-api-token
gmail-api-token --setup
gmail-api-token >/tmp/gmail-api-token-test
rm /tmp/gmail-api-token-test
```

For the legacy mailbox config shape, make the mailbox command run your existing
sync/index path:

```toml
on_notify = "cd ~/.mail/example && flock -n .sync.lock gmi sync && notmuch new"
```

`flock -n` keeps the sync stack from overlapping itself if something else is
already syncing. `mailwake` also serializes commands by lane and coalesces rapid
source events. The default post-command cooldown helps avoid loops where a sync
command itself causes immediate follow-up notifications.

## Command process handling

Notification commands run through `sh -c` in a separate Unix session/process
group. On command timeout, `mailwake` terminates that process group and reaps the
shell so child processes such as `sleep`, `gmi`, or `notmuch` are not orphaned by
the timeout path.

On clean shutdown, running notification commands are also terminated by process
group before the daemon exits. Command failure, timeout, and shutdown
cancellation are command outcomes; they do not by themselves kill the daemon.

Auth helpers use the same process-group timeout handling, but their stdout is
treated as secret material and is never logged.

## IMAP timeouts

`connect_timeout_seconds` bounds TCP connect. `imap_operation_timeout_seconds`
bounds TLS handshake, greeting read, authentication exchange, `EXAMINE`, IDLE
continuation, `DONE`, and tagged response waits. Timeout of an IMAP operation
causes that watcher to reconnect with exponential backoff. Secrets are not
included in timeout errors.

## systemd user service

Basic service example is in `contrib/systemd/mailwake.service`:

```sh
mkdir -p ~/.config/systemd/user
cp contrib/systemd/mailwake.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now mailwake.service
systemctl --user status mailwake.service
journalctl --user -u mailwake.service -f
```

If your config commands rely on programs in `~/.local/bin`, add or uncomment
this in the service:

```ini
Environment=PATH=%h/.local/bin:/usr/local/bin:/usr/bin:/bin
```

Absolute paths in `xoauth2_cmd`, `gmail_token_cmd`, and `password_cmd` are still
preferred because they make `check-config` and service startup behavior more
predictable.

The service uses `Type=notify`. With systemd available, `mailwake` sends
`READY=1` after config parsing, auth-helper executable checks, and watcher task
spawning. It does not execute OAuth/password helpers as a separate startup
preflight; helpers run when network-backed watchers connect and are bounded by
`auth_helper_timeout_seconds`. With `--initial-connect-required`, readiness waits
until every watcher completes initial setup before `READY=1` (for IMAP, that
means one successful login/examine/IDLE setup; for `gmail_api_poll`, it means
one successful metadata baseline; for `fs_state`, it means the filesystem
watcher was installed and any configured startup `state_cmd` attempt completed
successfully; for `system_resume`, it means the systemd/logind D-Bus
subscription is active). If an `fs_state` startup baseline fails or a
`system_resume` D-Bus subscription cannot be installed while
`--initial-connect-required` is set, startup fails instead of reporting ready.

Authentication failures are not treated like ordinary reconnectable network
errors. If an IMAP or Gmail API watcher cannot obtain credentials from an auth
helper, or the remote service rejects authentication/permission, `mailwake`
stops the daemon with exit status `78`. The packaged systemd units use
`RestartPreventExitStatus=78`, so reauth/config failures remain visible in
`systemctl --user --failed` instead of disappearing inside a restart loop.

Readiness is therefore not the same as "currently connected to Gmail" unless
`--initial-connect-required` is used. Without that flag, network failures are
handled by the reconnect loop after the service is considered ready.

`WatchdogSec` asks systemd to restart the service if it stops sending watchdog
pings. `mailwake` sends `WATCHDOG=1` only when source watcher tasks and command
lane runner tasks are healthy. Watchers must be alive and either idling or
progressing through expected reconnect/setup work. Command lanes must be alive
and must not have a command running past its configured timeout. If a task
crashes, appears stale, or wedges, watchdog pings stop so systemd can restart
the service.

The example units also include a bounded start limit. Unexpected non-`78`
failures can restart, but repeated failures eventually leave the service in a
failed state so desktop health checks can see it.

If `NOTIFY_SOCKET` and `WATCHDOG_USEC` are absent, `mailwake` runs normally from a
terminal, cron, OpenRC, or anything else.

## Hardened systemd example

`contrib/systemd/mailwake-hardened.service` adds sandboxing options:

```ini
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=read-only
ReadWritePaths=%h/.mail %h/.cache %h/.local/state %h/.config/mailwake
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX
LockPersonality=true
MemoryDenyWriteExecute=true
```

This is only an example. You may need to loosen it depending on your sync command,
password manager, OAuth helper, mail paths, notmuch database location, and IPC
requirements.

## Build and install

```sh
make build
make test
make install
```

By default `make install` installs `mailwake` to `~/.local/bin`. Override with
`PREFIX=/usr/local` or `BINDIR=/some/bin` if needed. Systemd examples can be
installed with:

```sh
make install-systemd
# or
make install-systemd-hardened
```

## Development

```sh
make fmt-check
make test
make clippy
```

The test suite covers config parsing/validation, auth helper trimming, timeouts
and redaction behavior, debounce/coalescing, non-overlapping command execution,
dirty-mailbox reruns, command success/failure/timeout handling, command runner
health, watchdog health, process-tree cleanup, shutdown cancellation, cooldown
coalescing, IMAP string validation, and systemd notification isolation.
