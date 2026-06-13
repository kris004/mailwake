# mailwake

`mailwake` is a tiny IMAP IDLE daemon. It connects to one or more IMAP
accounts, waits for mailbox change notifications, debounces/coalesces those
notifications, and runs configured shell commands.

```text
IMAP IDLE event -> debounce/coalesce -> run configured command
```

The primary use case is Gmail + lieer/gmailieer + notmuch + aerc: let Gmail wake
up the local sync/index stack when new mail or mailbox state changes arrive.

## Non-goals

`mailwake` does **not**:

- sync mail directly;
- store mail;
- implement a mail client;
- know about lieer, notmuch, mbsync, Gmail labels, Maildir, or aerc internals;
- implement a browser/device-code OAuth flow;
- act as a general automation framework.

Keep the daemon boring: IMAP IDLE in, command out.

## Authentication model

Secrets are not stored by `mailwake` by default. Long-running credential refresh
is delegated to external commands:

- `xoauth2_cmd` prints a fresh OAuth2 bearer token to stdout.
- `password_cmd` prints a password or app password to stdout.
- direct `password` exists only for local tests and throwaway accounts; it logs a
  loud warning without printing the value.

The daemon trims trailing CR/LF from helper output and never logs helper output,
OAuth tokens, passwords, command stdout/stderr, environment variables, or full
systemd status details. OAuth token storage and refresh should be handled by the
helper command.

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
xoauth2_cmd = "gmail-oauth-token"

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
password_cmd = "pass show mail/gmail-app-password"

[[accounts.mailboxes]]
name = "INBOX"
on_notify = "cd ~/.mail/example && flock -n .sync.lock gmi sync && notmuch new"
debounce_seconds = 10
```

Each configured mailbox gets its own IMAP connection so it can remain selected
and in IDLE independently.

## CLI

```sh
mailwake --config ~/.config/mailwake/config.toml
mailwake check-config --config ~/.config/mailwake/config.toml
mailwake test-command --config ~/.config/mailwake/config.toml --account gmail --mailbox INBOX
mailwake --no-systemd --config ~/.config/mailwake/config.toml
mailwake --initial-connect-required --config ~/.config/mailwake/config.toml
```

`check-config` parses and validates the config, checks auth-helper executable
paths when practical, and does not connect to IMAP, notify systemd, or run mailbox
commands.

`test-command` runs the configured command for one account/mailbox and reports the
exit status.

## Basic setup

```sh
mkdir -p ~/.config/mailwake
$EDITOR ~/.config/mailwake/config.toml

mailwake check-config --config ~/.config/mailwake/config.toml
mailwake test-command --config ~/.config/mailwake/config.toml --account gmail --mailbox INBOX
mailwake --config ~/.config/mailwake/config.toml
```

## Gmail + lieer + notmuch

Use a Gmail OAuth helper that prints a valid bearer token:

```toml
auth = "xoauth2_cmd"
xoauth2_cmd = "gmail-oauth-token"
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

Then make the mailbox command run your existing sync/index path:

```toml
on_notify = "cd ~/.mail/example && flock -n .sync.lock gmi sync && notmuch new"
```

`flock -n` keeps the sync stack from overlapping itself if something else is
already syncing. `mailwake` also serializes commands per mailbox and coalesces
rapid IMAP events.

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

The service uses `Type=notify`. With systemd available, `mailwake` sends
`READY=1` after config parsing, auth-helper checks, auth-helper startup preflight,
and watcher task spawning. With `--initial-connect-required`, it waits until every
watcher completes one successful login/select/IDLE setup before `READY=1`.

Readiness is therefore not the same as "currently connected to Gmail" unless
`--initial-connect-required` is used. Without that flag, network failures are
handled by the reconnect loop after the service is considered ready.

`WatchdogSec` asks systemd to restart the service if it stops sending watchdog
pings. `mailwake` sends `WATCHDOG=1` only when watcher tasks are alive and either
connected/idling or progressing through expected reconnect backoff. If a watcher
crashes or appears stale, watchdog pings stop so systemd can restart the service.

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

The test suite covers config parsing/validation, auth helper trimming and
redaction behavior, debounce/coalescing, non-overlapping command execution,
dirty-mailbox reruns, command success/failure handling, and systemd notification
isolation.
