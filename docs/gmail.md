# Gmail and IMAP integration

This is an integration guide, not the core model. `mailwake` does not sync mail,
interpret labels, manage Maildir, or know what lieer, notmuch, mbsync, or aerc
do. It turns a source signal into a configured command run.

Start from [`examples/gmail.example.toml`](../examples/gmail.example.toml).

## Choose a source

| Source | Advantages | Tradeoffs |
| --- | --- | --- |
| `imap_idle` | True server-driven notification and ordinary IMAP compatibility | Gmail's XOAUTH2 path uses the broad `https://mail.google.com/` scope. App passwords are also possible where the account permits them. |
| `gmail_api_poll` | Uses the narrower `gmail.metadata` scope and needs no inbound endpoint | Polls at an interval, is Gmail-specific, and requires a Google Cloud OAuth client with the Gmail API enabled. |

`gmail_api_poll` avoids Pub/Sub, a webhook, and a public endpoint. It does **not**
avoid Google Cloud OAuth setup. See Google's current [Gmail API quickstart](https://developers.google.com/workspace/gmail/api/quickstart/python)
for enabling the API, configuring consent, and creating a Desktop OAuth client.

OAuth configurations for both sources use an external token helper. IMAP may
instead use `password_cmd` where the provider permits app passwords. `mailwake`
does not implement or own a browser authorization flow.

## Example `oauth2l` helper

The repository includes a wrapper for Google's
[`oauth2l`](https://github.com/google/oauth2l) CLI. Install `oauth2l` separately,
then install the wrapper:

```sh
mkdir -p ~/.local/bin
install -m 0755 \
  contrib/oauth/gmail-oauth-token-oauth2l \
  ~/.local/bin/gmail-oauth-token
```

Store the Desktop OAuth client and token cache outside the repository with
private permissions:

```sh
umask 077
mkdir -p ~/.config/mailwake ~/.local/state/mailwake
chmod 700 ~/.config/mailwake ~/.local/state/mailwake
$EDITOR ~/.config/mailwake/google-oauth-client.json
chmod 600 ~/.config/mailwake/google-oauth-client.json
```

Complete the browser flow interactively. Redirect stdout so the bearer token is
not printed or written to a temporary file:

```sh
~/.local/bin/gmail-oauth-token --setup >/dev/null
~/.local/bin/gmail-oauth-token >/dev/null
```

By default the wrapper:

- requests `https://mail.google.com/` for Gmail IMAP/SMTP;
- reads `~/.config/mailwake/google-oauth-client.json`;
- stores its refresh-token cache at
  `~/.local/state/mailwake/oauth2l-cache.json`;
- exits `78` when a cached refresh token is expired or revoked;
- never includes failed helper output in its own error messages.

`--reauth` resets the cache, performs authorization, and suppresses the access
token on success. Run interactive reauthorization manually unless the service
environment is deliberately able to present the browser flow.

Configure IMAP XOAUTH2 with an absolute helper path:

```toml
[[accounts]]
name = "gmail"
host = "imap.gmail.com"
port = 993
username = "user@example.com"
auth = "xoauth2_cmd"
xoauth2_cmd = "/home/alice/.local/bin/gmail-oauth-token"
```

## Gmail API metadata helper

Use a separate cache and the metadata scope so an IMAP token and an API token do
not share authorization state:

```sh
umask 077
cat > /home/alice/.local/bin/gmail-api-token <<'EOF_HELPER'
#!/bin/sh
set -eu
export MAILWAKE_GMAIL_SCOPE='https://www.googleapis.com/auth/gmail.metadata'
export MAILWAKE_OAUTH2L_CACHE="${XDG_STATE_HOME:-$HOME/.local/state}/mailwake/gmail-api-metadata-oauth2l-cache.json"
exec /home/alice/.local/bin/gmail-oauth-token "$@"
EOF_HELPER
chmod 700 /home/alice/.local/bin/gmail-api-token

~/.local/bin/gmail-api-token --setup >/dev/null
~/.local/bin/gmail-api-token >/dev/null
```

The metadata scope is accepted by the Gmail `users.getProfile` and history
endpoints used by `mailwake`. The poller does not read message bodies, send,
modify, or delete mail.

## Command recipe

A sync/index command remains external. For example:

```toml
[[commands]]
name = "remote-sync"
lane = "mail-sync"
cmd = "cd /home/alice/.mail/example && flock -n .sync.lock gmi sync && notmuch new"
timeout_seconds = 300
min_interval_seconds = 60
output_mode = "silent"
```

Use an idempotent reconciliation command: notifications are non-durable
wake-up signals, not one event per message. `run_on_startup = true` is
recommended when the command must recover changes that occurred while
`mailwake` was offline.

A shared lane prevents `mailwake` commands from overlapping each other.
`flock -n` can additionally prevent overlap with the same sync tool started by
another process. Increase `timeout_seconds` to cover the sync tool's legitimate
worst-case runtime.

Mail tooling may print subjects, addresses, or paths. The example uses
`output_mode = "silent"` so those values do not enter the journal. Change the
mode only after reviewing the output policy in the
[configuration reference](configuration.md#command-output).

## Gmail API polling example

```toml
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

This polls the current Gmail history id. When it advances, optional label
filtering uses `users.history.list` before the command is submitted. The
baseline is in memory and is recreated at process start; `run_on_startup` makes
the sync command reconcile that gap.

## IMAP IDLE example

```toml
[[sources]]
name = "gmail-inbox"
type = "imap_idle"
account = "gmail"
mailbox = "INBOX"
on_event = "remote-sync"
run_on_startup = true
debounce_seconds = 10
```

The mailbox is opened read-only with `EXAMINE`. Each configured mailbox uses its
own IMAP connection so it can remain selected in IDLE. Network failures
reconnect with backoff; credential-helper failure or authentication rejection
stops the daemon with exit status `78`.

## App passwords

Where the provider and account allow app passwords, use a password manager
helper instead of placing the password in TOML:

```toml
auth = "password_cmd"
password_cmd = "/usr/bin/pass show mail/example-app-password"
```

Literal `password` is intended only for local tests and throwaway accounts and
logs a warning without printing its value.
