# mailwake design

`mailwake` is intentionally a narrow bridge:

```text
IMAP IDLE event -> debounce/coalesce/cooldown -> shell command
```

It does not sync mail, store mail, understand Gmail labels, or manage OAuth. Mail
sync tools, password managers, and OAuth helpers stay outside the daemon.

## Shape

- One Tokio task watches each configured account/mailbox pair.
- Each watcher uses one IMAP connection because IMAP IDLE operates on the
  selected mailbox for that connection.
- Each mailbox has a dedicated debounce/command task.
- Commands for the same mailbox are serialized by that task; overlapping runs are
  impossible in normal operation.
- Authentication secrets are obtained on connection/reconnection with a bounded
  helper timeout/output cap and wrapped in a redacted secret type. Helpers run
  only after TCP/TLS connect and server greeting succeed.
- Notification commands have a bounded runtime. Timeout is a command outcome, not
  a daemon-fatal error.
- Auth helpers and notification commands run in separate Unix process groups.
  Timeout and shutdown paths terminate the whole process group and reap the
  shell.
- A post-command cooldown coalesces events that arrive immediately after a
  command run, which helps avoid self-trigger loops from sync/index commands.

## Authentication

`xoauth2_cmd` and `password_cmd` are shell commands that print a secret to
stdout. `mailwake` trims only trailing CR/LF and never logs stdout/stderr from
these helpers. Helper output is capped by `auth_helper_max_output_bytes`; if the
cap is exceeded, the helper process group is killed and no output is included in
the error. OAuth refresh belongs in the helper, not in the daemon.

Direct `password` exists only for local testing and intentionally emits a warning
without printing the password value.

Static auth-helper validation is deliberately conservative. Simple commands are
checked against PATH when practical. Complex shell commands are not rejected just
because static validation cannot prove the target executable.

## IMAP safety

TCP connect, TLS handshake, greeting read, authentication, mailbox selection, IDLE
continuation, DONE, and tagged response waits are bounded by config timeouts.
Timeouts return the watcher to the reconnect/backoff loop.

Usernames, mailbox names, and direct LOGIN passwords containing CR/LF are
rejected before command construction. This keeps IMAP command strings single-line
and avoids logging secret values in validation errors.

## Readiness and watchdog

Config parsing and auth-helper path checks happen before `READY=1`. The daemon
intentionally avoids a separate auth-helper execution preflight so startup does
not double-run OAuth/password helpers. Helper execution happens in watcher tasks
and is bounded by `auth_helper_timeout_seconds`. With `--initial-connect-required`,
readiness also waits for every watcher to complete one successful
login/select/IDLE setup.

Systemd notification is opportunistic. Without `NOTIFY_SOCKET`, `mailwake` runs
normally. Watchdog pings are sent only while the supervisor believes every IMAP
watcher task is either connected/idling or progressing through its reconnect
loop, every command runner task is alive, and no command has exceeded its
configured timeout.
