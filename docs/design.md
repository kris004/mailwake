# mailwake design

`mailwake` is intentionally a narrow bridge:

```text
IMAP IDLE event -> debounce/coalesce -> shell command
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
- Authentication secrets are obtained on connection/reconnection and wrapped in a
  redacted secret type.

## Authentication

`xoauth2_cmd` and `password_cmd` are shell commands that print a secret to
stdout. `mailwake` trims only trailing CR/LF and never logs stdout/stderr from
these helpers. OAuth refresh belongs in the helper, not in the daemon.

Direct `password` exists only for local testing and intentionally emits a warning
without printing the password value.

## Readiness and watchdog

Config parsing, auth-helper path checks, and initial auth-helper execution happen
before `READY=1`. Network connection setup is supervised by watcher tasks. With
`--initial-connect-required`, readiness also waits for every watcher to complete
one successful login/select/IDLE setup.

Systemd notification is opportunistic. Without `NOTIFY_SOCKET`, `mailwake` runs
normally. Watchdog pings are sent only while the supervisor believes every
watcher task is either connected/idling or progressing through its reconnect
loop.
