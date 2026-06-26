# Gmail API watch source plan

This is a staged implementation plan for adding a Gmail-specific watch source to
`mailwake` without changing the generic IMAP source. The goal is to avoid using
Gmail IMAP XOAUTH2 just to receive wake-up notifications.

## Goal

Add an optional Gmail API source that turns Gmail mailbox changes into the same
kind of debounced command trigger used by existing sources:

```text
Gmail API / Pub/Sub event -> debounce/coalesce/cooldown -> command lane
```

The source must not sync mail, read message bodies, understand a sync tool, or
make Gmail semantics part of the core command model.

## Permission target

The Gmail watcher should use the narrowest Gmail scope that supports mailbox
change notifications:

```text
https://www.googleapis.com/auth/gmail.metadata
```

This is narrower than the IMAP/SMTP scope:

```text
https://mail.google.com/
```

The Gmail API source will still need Pub/Sub access for the subscription pull and
ack path. That token should be supplied by a helper too, rather than stored by
`mailwake`.

## Proposed config shape

```toml
[[commands]]
name = "remote-sync"
cmd = "cd /home/alice/.mail/example && flock -n .sync.lock sync-command"

[[sources]]
name = "gmail-inbox"
type = "gmail_api_watch"
on_event = "remote-sync"
gmail_token_cmd = "/home/alice/.local/bin/gmail-api-token"
pubsub_token_cmd = "/home/alice/.local/bin/google-pubsub-token"
topic_name = "projects/example-project/topics/mailwake-gmail"
subscription = "projects/example-project/subscriptions/mailwake-gmail"
label_ids = ["INBOX"]
label_filter_behavior = "include"
run_on_startup = true
debounce_seconds = 10
```

The exact field names can still change during implementation, but the source
should stay self-contained and should not reuse IMAP account configuration.

## Implementation slices and gates

### 1. Config-only slice

- Add `SourceConfig::GmailApiWatch`.
- Parse and validate source fields.
- Validate `on_event` references a configured command.
- Validate non-empty token helper commands and Pub/Sub resource names.
- Keep all existing source types and legacy mailbox config unchanged.

Gate:

```sh
cargo fmt --check
cargo test --locked config::tests::
cargo test --locked
```

### 2. Secret-helper and HTTP foundation

- Reuse the existing bounded secret helper behavior for bearer-token helpers.
- Add a small internal HTTP client boundary for Gmail and Pub/Sub REST calls.
- Ensure tokens, helper stdout/stderr, request bodies with bearer tokens, and
  response bodies that might contain sensitive data are not logged.
- Classify helper exit `78`, HTTP `401`, and relevant HTTP `403` auth/permission
  failures as user-action-required failures.

Gate:

```sh
cargo fmt --check
cargo test --locked gmail_api::tests::
cargo test --locked auth::tests::
```

### 3. Gmail watch registration and renewal

- Call `users.watch` with the configured topic and label filter.
- Record the returned `historyId` and `expiration` in memory.
- Renew the watch daily by default and before the returned expiration.
- Suppress the immediate watch-registration notification unless
  `run_on_startup = true` requests an explicit startup event.
- Treat transient HTTP/network errors as reconnectable with bounded backoff.
- Treat auth/permission failures as fail-stop exit `78`.

Gate:

```sh
cargo fmt --check
cargo test --locked gmail_api::tests::watch
```

### 4. Pub/Sub pull and ack loop

- Pull from the configured subscription.
- Decode Gmail notification payloads and compare `historyId` values.
- Queue one source event when a newer history id is observed.
- Ack messages after they are accepted as wake-up signals.
- Coalesce duplicate/out-of-order messages without running duplicate commands.
- Keep Pub/Sub and Gmail payload details out of logs unless they are known safe.

Gate:

```sh
cargo fmt --check
cargo test --locked gmail_api::tests::pubsub
```

### 5. Daemon integration

- Register the Gmail API source with `RuntimeState` like other watchers.
- Feed Gmail events into the existing `DebounceRunner` and command lanes.
- Support `--initial-connect-required` by waiting for watch setup and Pub/Sub
  pull readiness.
- Send fatal exit `78` for reauth-required auth or permission failures.
- Leave `imap_idle`, `fs_state`, `system_resume`, and legacy mailbox behavior
  untouched.

Gate:

```sh
cargo fmt --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --locked
```

### 6. Docs and examples

- Keep IMAP documented as the generic IMAP option.
- Add a Gmail API watch example with fake placeholders only.
- Document the Gmail metadata and Pub/Sub scopes separately.
- Explain that Gmail API watch renewal is separate from OAuth refresh-token
  lifetime.
- Avoid documenting maintainer-specific paths, project IDs, account names, or
  email addresses.

Gate:

```sh
cargo fmt --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --locked
```

Also run a targeted private-info scan for the maintainer's known private
usernames, email addresses, account names, and local paths before committing.

## Non-goals

- Do not replace generic IMAP support.
- Do not add Gmail message-body reads.
- Do not couple `mailwake` to lieer, notmuch, Maildir, aerc, or any sync tool.
- Do not store OAuth refresh tokens in `mailwake` itself.
- Do not create or manage Google Cloud topics/subscriptions automatically in the
  daemon.
