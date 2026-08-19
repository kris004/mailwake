# mailwake design

`mailwake` is intentionally a narrow bridge:

```text
event source -> debounce/coalesce/cooldown -> command lane -> shell command
```

It does not sync mail, store mail, understand Gmail labels, interpret local
state, or manage OAuth. Sync tools, password managers, state fingerprint
commands, and OAuth helpers stay outside the daemon. Provider-specific sources
may use provider cursors or filters, but they still emit only generic wake-up
events.

Those events are deliberately non-durable. Sources do not append work to a
persistent queue, so configured commands must reconcile current state rather
than rely on one invocation per upstream event.

## Shape

- One Tokio task watches each configured source.
- Each watcher uses one IMAP connection because IMAP IDLE operates on the
  selected mailbox for that connection.
- `fs_state` watchers use `notify` as a wake-up signal. Filesystem events are
  coalesced before an optional opaque `state_cmd` comparison.
- Gmail API poll watchers use metadata/history ids as a wake-up signal without
  reading message bodies or managing provider-side notification resources.
- IMAP source debounce, Gmail API history advancement, and fs_state state
  comparison submit command requests to command lanes.
- Commands in the same lane are serialized and repeated requests for the same
  command are coalesced.
- Authentication secrets are obtained with a bounded helper timeout/output cap
  and wrapped in a redacted secret type. IMAP helpers run only after TCP/TLS
  connect and the server greeting succeed; Gmail API helpers run when a
  baseline or poll request needs a token.
- Configured commands have a bounded runtime. Timeout is a command outcome, not
  a daemon-fatal error.
- Auth helpers and configured commands run in separate Unix process groups.
  Timeout and shutdown paths terminate the whole process group and reap the
  shell.
- Per-command cooldowns coalesce events that arrive immediately after a command
  run. `fs_state` sources also rebaseline after their own command finishes so
  command-caused filesystem writes do not self-trigger.

## Authentication

`xoauth2_cmd`, `gmail_token_cmd`, and `password_cmd` are shell commands that
print a secret to stdout. `mailwake` trims only trailing CR/LF and never logs
stdout/stderr from these helpers. Helper output is capped by
`auth_helper_max_output_bytes`; if the cap is exceeded, the helper process group
is killed and no output is included in the error. OAuth refresh belongs in the
helper, not in the daemon.

IMAP helper failures and authentication rejection are fail-stop events. For the
Gmail API source, an explicit helper exit `78`, HTTP 401, and known permission
rejections are fail-stop; other helper failures and quota/rate-limit responses
retry with backoff. This avoids turning a temporary Google limit or helper
timeout into a false reauthorization request. The daemon propagates fail-stop
authentication errors as exit `78` so supervisors can avoid useless restarts.

Direct `password` exists only for local testing and intentionally emits a warning
without printing the password value.

Static auth-helper validation is deliberately conservative. Simple commands are
checked against PATH when practical. Complex shell commands are not rejected just
because static validation cannot prove the target executable.

## IMAP safety

TCP connect, TLS handshake, greeting read, authentication, read-only mailbox
open, IDLE continuation, DONE, and tagged response waits are bounded by config
timeouts.
Network and protocol timeouts return the watcher to the reconnect/backoff loop.
Authentication failures stop the daemon instead of leaving it running without a
working IMAP source.

Usernames, mailbox names, and direct LOGIN passwords containing CR/LF are
rejected before command construction. This keeps IMAP command strings single-line
and avoids logging secret values in validation errors.

## Gmail API poll safety

The Gmail API poll source is a lower-scope polling alternative for Gmail users
who do not want the broad Gmail IMAP scope. It uses `users.getProfile` for the
current history id and, when label filters are configured,
`users.history.list` to check whether changed history intersects those labels.
It does not call Gmail APIs that read message bodies, send mail, modify mail, or
delete mail.

The source is deliberately local-only: it calls Gmail HTTPS APIs from the
daemon and requires no Pub/Sub topic, webhook, or public endpoint. It does not
manage the required OAuth client registration or token acquisition.
If Gmail reports that the stored history baseline is too old, the source emits
one wake-up event and rebaselines so the external sync command can repair local
state. The baseline is memory-only and is recreated at process startup.

## Readiness and watchdog

Config parsing and auth-helper path checks happen before `READY=1`. The daemon
intentionally avoids a separate auth-helper execution preflight so startup does
not double-run OAuth/password helpers. Helper execution happens in watcher tasks
and is bounded by `auth_helper_timeout_seconds`. With `--initial-connect-required`,
readiness also waits for every watcher to complete initial setup: IMAP must
login, examine, and enter IDLE; Gmail API polling must establish one metadata
baseline; `fs_state` must install its watcher and complete any startup baseline;
and `system_resume` must subscribe to systemd/logind D-Bus.

Systemd notification is opportunistic. Without `NOTIFY_SOCKET`, `mailwake` runs
normally. Watchdog pings are sent only while the supervisor believes every source
watcher task is alive and making progress, every command lane task is alive, and
no command has exceeded its configured timeout. The human-readable systemd
status includes a stable `running commands: N` fragment and is refreshed as
command lanes start or finish, allowing local status consumers to reflect
generic activity without learning command names or command lines.
