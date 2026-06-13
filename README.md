# mailwake

`mailwake` is a tiny event-driven command trigger. It watches configured event
sources, debounces/coalesces noisy notifications, and runs configured shell
commands.

```text
event source -> debounce/coalesce/cooldown -> command lane -> run configured command
```

IMAP IDLE is one source type. `fs_state` is another source type for local
filesystem/state changes. The daemon intentionally treats both as wake-up
signals, not as work-item queues.

## Non-goals

`mailwake` does **not**:

- sync mail directly;
- store mail;
- implement a mail client;
- know about lieer, notmuch, mbsync, Gmail labels, Maildir, or aerc internals;
- implement a browser/device-code OAuth flow;
- act as a general automation framework;
- recursively crawl or sync local filesystem trees.

Keep the daemon boring: event in, command out.

## Authentication model

Secrets are not stored by `mailwake` by default. Long-running credential refresh
is delegated to external commands:

- `xoauth2_cmd` prints a fresh OAuth2 bearer token to stdout.
- `password_cmd` prints a password or app password to stdout.
- direct `password` exists only for local tests and throwaway accounts; it logs a
  loud warning without printing the value.

The daemon trims trailing CR/LF from helper output and never logs helper output,
OAuth tokens, passwords, command stdout/stderr, environment variables, or full
systemd status details. Auth helpers are invoked only after TCP/TLS connection
and the server greeting succeed, so offline/reconnect loops do not repeatedly
refresh OAuth tokens before Gmail is reachable. Helpers run with
`auth_helper_timeout_seconds`; if they time out or exceed
`auth_helper_max_output_bytes`, `mailwake` terminates the helper's Unix process
group so helper children are not left behind. OAuth token storage and refresh
should be handled by the helper command.

For Gmail, prefer `xoauth2_cmd`. App-password based accounts can use
`password_cmd`. A full OAuth browser/device-code flow is intentionally outside
this daemon.

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

Useful global knobs:

```toml
# Defaults shown.
default_debounce_seconds = 10
default_max_debounce_seconds = 60
auth_helper_timeout_seconds = 30
auth_helper_max_output_bytes = 65536
command_timeout_seconds = 300
command_output_max_bytes = 1048576
connect_timeout_seconds = 30
imap_operation_timeout_seconds = 60
idle_refresh_seconds = 1740
watcher_stale_seconds = 3600
min_command_interval_seconds = 60
capture_command_output = false
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

[[commands]]
name = "local-push"
lane = "mail-sync"
cmd = "push-local"
timeout_seconds = 300
min_interval_seconds = 30
```

Only one command in a lane runs at a time. Repeated requests for the same
command while the lane is busy are coalesced into one follow-up run. Commands in
different lanes can run independently.

`debounce_seconds = 0` is allowed when explicitly configured, but it means every
observed source event can trigger command execution as soon as cooldown and the
previous run permit.

`capture_command_output` is normally false. If enabled, `mailwake` captures
notification command stdout/stderr and logs only byte counts. Captured output is
capped by `command_output_max_bytes` (default 1 MiB); if a command exceeds the
cap, `mailwake` kills the command process group and reports a command failure
without logging the output contents. The old `log_command_output` name still
parses for compatibility but logs a deprecation warning.

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
debounce_seconds = 10
```

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

The wrapper uses the IMAP/SMTP Gmail scope `https://mail.google.com/`, stores the
OAuth client JSON under `~/.config/mailwake/`, and stores the `oauth2l` token
cache under `~/.local/state/mailwake/` instead of oauth2l's default `~/.oauth2l`
file. Run `--setup` interactively once before starting the systemd service.

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
bounds TLS handshake, greeting read, authentication exchange, `SELECT`, IDLE
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

Absolute paths in `xoauth2_cmd`/`password_cmd` are still preferred because they
make `check-config` and service startup behavior more predictable.

The service uses `Type=notify`. With systemd available, `mailwake` sends
`READY=1` after config parsing, auth-helper executable checks, and watcher task
spawning. It does not execute OAuth/password helpers as a separate startup
preflight; helpers run when IMAP watchers connect and are bounded by
`auth_helper_timeout_seconds`. With `--initial-connect-required`, readiness waits
until every watcher completes initial setup before `READY=1` (for IMAP, that
means one successful login/select/IDLE setup; for `fs_state`, it means the
filesystem watcher was installed and any configured startup `state_cmd` attempt
completed successfully). If an `fs_state` startup baseline fails while
`--initial-connect-required` is set, startup fails instead of reporting ready.

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
