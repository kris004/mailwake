# mailwake design

`mailwake` is intentionally a narrow bridge:

```text
event source -> debounce/coalesce/cooldown -> command lane -> shell command
```

It does not sync mail, store mail, understand Gmail labels, interpret local
state, or manage OAuth. Sync tools, password managers, state fingerprint
commands, and OAuth helpers stay outside the daemon.

## Shape

- One Tokio task watches each configured source.
- Each watcher uses one IMAP connection because IMAP IDLE operates on the
  selected mailbox for that connection.
- `fs_state` watchers use `notify` as a wake-up signal. Filesystem events are
  coalesced before an optional opaque `state_cmd` comparison.
- IMAP source debounce and fs_state state comparison submit command requests to
  command lanes.
- Commands in the same lane are serialized and repeated requests for the same
  command are coalesced.
- Authentication secrets are obtained on connection/reconnection with a bounded
  helper timeout/output cap and wrapped in a redacted secret type. Helpers run
  only after TCP/TLS connect and server greeting succeed.
- Notification commands have a bounded runtime. Timeout is a command outcome, not
  a daemon-fatal error.
- Auth helpers and notification commands run in separate Unix process groups.
  Timeout and shutdown paths terminate the whole process group and reap the
  shell.
- Per-command cooldowns coalesce events that arrive immediately after a command
  run. `fs_state` sources also rebaseline after their own command finishes so
  command-caused filesystem writes do not self-trigger.

## Authentication

`xoauth2_cmd` and `password_cmd` are shell commands that print a secret to
stdout. `mailwake` trims only trailing CR/LF and never logs stdout/stderr from
these helpers. Helper output is capped by `auth_helper_max_output_bytes`; if the
cap is exceeded, the helper process group is killed and no output is included in
the error. OAuth refresh belongs in the helper, not in the daemon.

Auth failures are fail-stop, not reconnect-loop events. Helper failures and IMAP
authentication rejection stop the daemon so an operator can see and fix the
problem. A helper may exit `78` to classify the failure as "user action or
reauthorization required"; the daemon propagates that exit status so supervisors
can avoid useless restarts.

Direct `password` exists only for local testing and intentionally emits a warning
without printing the password value.

Static auth-helper validation is deliberately conservative. Simple commands are
checked against PATH when practical. Complex shell commands are not rejected just
because static validation cannot prove the target executable.

## IMAP safety

TCP connect, TLS handshake, greeting read, authentication, mailbox selection, IDLE
continuation, DONE, and tagged response waits are bounded by config timeouts.
Network and protocol timeouts return the watcher to the reconnect/backoff loop.
Authentication failures stop the daemon instead of leaving it running without a
working IMAP source.

Usernames, mailbox names, and direct LOGIN passwords containing CR/LF are
rejected before command construction. This keeps IMAP command strings single-line
and avoids logging secret values in validation errors.

## Readiness and watchdog

Config parsing and auth-helper path checks happen before `READY=1`. The daemon
intentionally avoids a separate auth-helper execution preflight so startup does
not double-run OAuth/password helpers. Helper execution happens in watcher tasks
and is bounded by `auth_helper_timeout_seconds`. With `--initial-connect-required`,
readiness also waits for every watcher to complete initial setup; for IMAP that
means one successful login/select/IDLE setup.

Systemd notification is opportunistic. Without `NOTIFY_SOCKET`, `mailwake` runs
normally. Watchdog pings are sent only while the supervisor believes every source
watcher task is alive and making progress, every command lane task is alive, and
no command has exceeded its configured timeout.
