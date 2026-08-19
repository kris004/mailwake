use crate::auth::{self, AuthError, Credentials};
use crate::config::{AccountConfig, MailboxConfig, SecretString};
use crate::state::{RuntimeState, WatcherPhase};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use std::future::Future;
use std::time::Duration;
use std::{fmt, sync::Arc};
use thiserror::Error;
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{sleep, timeout};
use tokio_native_tls::TlsConnector;
use tracing::{debug, info, warn};

const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(300);
const MAX_RESPONSE_LINE_BYTES: usize = 64 * 1024;
pub const INITIAL_AUTH_FAILURE_PREFIX: &str = "IMAP watcher authentication failed";

#[derive(Clone, Copy, Debug)]
pub struct WatcherSettings {
    pub idle_refresh: Duration,
    pub auth_helper_timeout: Duration,
    pub auth_helper_max_output_bytes: usize,
    pub connect_timeout: Duration,
    pub operation_timeout: Duration,
}

trait ImapStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T> ImapStream for T where T: AsyncRead + AsyncWrite + Unpin + Send {}
type BoxedImapStream = Box<dyn ImapStream>;

#[derive(Debug, Error)]
pub enum ImapError {
    #[error("auth helper failed: {0}")]
    Auth(#[from] AuthError),
    #[error("network or IMAP I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("TLS setup failed: {0}")]
    Tls(#[from] native_tls::Error),
    #[error("server closed the connection")]
    ServerClosed,
    #[error("server rejected authentication")]
    AuthRejected,
    #[error("server rejected IMAP command {command}")]
    CommandRejected { command: &'static str },
    #[error("unexpected IMAP protocol response while {context}")]
    Protocol { context: &'static str },
    #[error("{operation} timed out after {seconds} seconds")]
    OperationTimedOut {
        operation: &'static str,
        seconds: u64,
    },
    #[error("{field} contains CR or LF characters and cannot be sent as an IMAP string")]
    UnsafeImapString { field: &'static str },
}

impl ImapError {
    pub fn is_permanent_auth_failure(&self) -> bool {
        matches!(self, Self::Auth(_) | Self::AuthRejected)
    }
}

#[derive(Debug, Eq, PartialEq)]
enum RunEnd {
    Shutdown,
}

struct MailboxRunContext<'a> {
    account: &'a AccountConfig,
    mailbox: &'a MailboxConfig,
    events: &'a mpsc::Sender<()>,
    state: &'a Arc<RuntimeState>,
    watcher_id: &'a str,
    initial_ready: &'a mut Option<oneshot::Sender<Result<(), String>>>,
    established: &'a mut bool,
    shutdown: &'a mut watch::Receiver<bool>,
    settings: WatcherSettings,
}

pub struct MailboxWatchTask {
    pub account: AccountConfig,
    pub mailbox: MailboxConfig,
    pub events: mpsc::Sender<()>,
    pub state: Arc<RuntimeState>,
    pub watcher_id: String,
    pub initial_ready: Option<oneshot::Sender<Result<(), String>>>,
    pub shutdown: watch::Receiver<bool>,
    pub settings: WatcherSettings,
}

pub async fn watch_mailbox_forever(task: MailboxWatchTask) -> Result<(), ImapError> {
    let MailboxWatchTask {
        account,
        mailbox,
        events,
        state,
        watcher_id,
        mut initial_ready,
        mut shutdown,
        settings,
    } = task;
    let mut backoff = INITIAL_BACKOFF;

    loop {
        if *shutdown.borrow() {
            break;
        }

        state.mark_watcher(&watcher_id, WatcherPhase::Connecting);
        let mut established = false;
        let run_result = {
            let mut context = MailboxRunContext {
                account: &account,
                mailbox: &mailbox,
                events: &events,
                state: &state,
                watcher_id: &watcher_id,
                initial_ready: &mut initial_ready,
                established: &mut established,
                shutdown: &mut shutdown,
                settings,
            };
            run_mailbox_once(&mut context).await
        };
        match run_result {
            Ok(RunEnd::Shutdown) => break,
            Err(error) if error.is_permanent_auth_failure() => {
                let error_text = error.to_string();
                state.mark_watcher(&watcher_id, WatcherPhase::Crashed);
                if let Some(sender) = initial_ready.take() {
                    let _ =
                        sender.send(Err(format!("{INITIAL_AUTH_FAILURE_PREFIX}: {error_text}")));
                }
                warn!(
                    account = %account.name,
                    mailbox = %mailbox.name,
                    %error,
                    "IMAP watcher authentication failed; stopping"
                );
                return Err(error);
            }
            Err(error) => {
                warn!(
                    account = %account.name,
                    mailbox = %mailbox.name,
                    %error,
                    "IMAP watcher disconnected; reconnecting"
                );
            }
        }

        if established {
            backoff = INITIAL_BACKOFF;
        }
        state.mark_watcher(&watcher_id, WatcherPhase::Reconnecting);
        if !sleep_or_shutdown(backoff, &mut shutdown).await {
            break;
        }
        backoff = std::cmp::min(backoff * 2, MAX_BACKOFF);
    }

    state.mark_watcher(&watcher_id, WatcherPhase::Stopped);
    info!(
        account = %account.name,
        mailbox = %mailbox.name,
        "IMAP watcher stopped"
    );
    Ok(())
}

async fn run_mailbox_once(context: &mut MailboxRunContext<'_>) -> Result<RunEnd, ImapError> {
    validate_imap_string("username", &context.account.username)?;
    validate_imap_string("mailbox name", &context.mailbox.name)?;

    let stream = connect(
        context.account,
        context.settings.connect_timeout,
        context.settings.operation_timeout,
    )
    .await?;
    let (read_half, mut write_half) = tokio::io::split(stream);
    let mut reader = BufReader::new(read_half);
    let mut tags = Tags::default();

    with_imap_timeout(
        context.settings.operation_timeout,
        "IMAP greeting",
        read_greeting(&mut reader),
    )
    .await?;
    let credentials = auth::credentials_for(
        context.account,
        context.settings.auth_helper_timeout,
        context.settings.auth_helper_max_output_bytes,
    )
    .await?;
    with_imap_timeout(
        context.settings.operation_timeout,
        "IMAP authentication",
        authenticate(
            &mut reader,
            &mut write_half,
            &mut tags,
            &context.account.username,
            credentials,
        ),
    )
    .await?;
    with_imap_timeout(
        context.settings.operation_timeout,
        "EXAMINE",
        examine_mailbox(
            &mut reader,
            &mut write_half,
            &mut tags,
            &context.mailbox.name,
        ),
    )
    .await?;

    info!(
        account = %context.account.name,
        mailbox = %context.mailbox.name,
        "IMAP login/examine succeeded; entering IDLE"
    );
    idle_forever(&mut reader, &mut write_half, &mut tags, context).await
}

async fn connect(
    account: &AccountConfig,
    connect_timeout: Duration,
    operation_timeout: Duration,
) -> Result<BoxedImapStream, ImapError> {
    let address = (account.host.as_str(), account.port());
    let tcp =
        with_imap_timeout(connect_timeout, "TCP connect", TcpStream::connect(address)).await?;
    if account.insecure_plaintext {
        return Ok(Box::new(tcp));
    }

    let mut builder = native_tls::TlsConnector::builder();
    if account.danger_accept_invalid_certs {
        builder.danger_accept_invalid_certs(true);
    }
    let connector = TlsConnector::from(builder.build()?);
    let tls = with_imap_timeout(
        operation_timeout,
        "TLS handshake",
        connector.connect(&account.host, tcp),
    )
    .await?;
    Ok(Box::new(tls))
}

async fn read_greeting<R>(reader: &mut R) -> Result<(), ImapError>
where
    R: AsyncBufRead + Unpin,
{
    let line = read_line(reader).await?;
    if line.to_ascii_uppercase().starts_with("* OK") {
        return Ok(());
    }
    if line.to_ascii_uppercase().starts_with("* PREAUTH") {
        return Ok(());
    }
    Err(ImapError::Protocol {
        context: "reading server greeting",
    })
}

async fn authenticate<R, W>(
    reader: &mut R,
    writer: &mut W,
    tags: &mut Tags,
    username: &str,
    credentials: Credentials,
) -> Result<(), ImapError>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    match credentials {
        Credentials::Xoauth2 { token } => {
            authenticate_xoauth2(reader, writer, tags, username, &token).await
        }
        Credentials::Password { password } => {
            authenticate_password(reader, writer, tags, username, &password).await
        }
    }
}

async fn authenticate_xoauth2<R, W>(
    reader: &mut R,
    writer: &mut W,
    tags: &mut Tags,
    username: &str,
    token: &SecretString,
) -> Result<(), ImapError>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let tag = tags.next();
    write_raw(writer, format!("{tag} AUTHENTICATE XOAUTH2\r\n").as_bytes()).await?;

    loop {
        let line = read_line(reader).await?;
        if line.starts_with('+') {
            let response = xoauth2_response(username, token.expose_secret());
            write_raw(writer, response.as_bytes()).await?;
            write_raw(writer, b"\r\n").await?;
            break;
        }
        if is_tagged_completion(&line, &tag) {
            return Err(ImapError::AuthRejected);
        }
    }

    loop {
        let line = read_line(reader).await?;
        if line.starts_with('+') {
            // On SASL failure, some servers send a challenge and wait for an
            // empty response before the tagged NO/BAD. Do not log the challenge.
            write_raw(writer, b"\r\n").await?;
            continue;
        }
        if is_tagged_ok(&line, &tag) {
            return Ok(());
        }
        if is_tagged_completion(&line, &tag) {
            return Err(ImapError::AuthRejected);
        }
    }
}

async fn authenticate_password<R, W>(
    reader: &mut R,
    writer: &mut W,
    tags: &mut Tags,
    username: &str,
    password: &SecretString,
) -> Result<(), ImapError>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    validate_imap_string("username", username)?;
    validate_imap_string("password", password.expose_secret())?;
    let tag = tags.next();
    let command = format!(
        "{tag} LOGIN {} {}\r\n",
        quote_imap(username),
        quote_imap(password.expose_secret())
    );
    write_raw(writer, command.as_bytes()).await?;
    wait_tagged(reader, &tag, "LOGIN")
        .await
        .map_err(|error| match error {
            ImapError::CommandRejected { .. } => ImapError::AuthRejected,
            other => other,
        })
}

async fn examine_mailbox<R, W>(
    reader: &mut R,
    writer: &mut W,
    tags: &mut Tags,
    mailbox: &str,
) -> Result<(), ImapError>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    validate_imap_string("mailbox name", mailbox)?;
    let tag = tags.next();
    let command = format!("{tag} EXAMINE {}\r\n", quote_imap(mailbox));
    write_raw(writer, command.as_bytes()).await?;
    wait_tagged(reader, &tag, "EXAMINE").await
}

async fn idle_forever<R, W>(
    reader: &mut R,
    writer: &mut W,
    tags: &mut Tags,
    context: &mut MailboxRunContext<'_>,
) -> Result<RunEnd, ImapError>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    loop {
        let tag = tags.next();
        let idle_command = format!("{tag} IDLE\r\n");
        with_imap_timeout(
            context.settings.operation_timeout,
            "IDLE command",
            write_raw(writer, idle_command.as_bytes()),
        )
        .await?;
        with_imap_timeout(
            context.settings.operation_timeout,
            "IDLE continuation",
            wait_idle_continuation(reader, &tag),
        )
        .await?;
        context
            .state
            .mark_watcher(context.watcher_id, WatcherPhase::Idling);
        if !*context.established {
            *context.established = true;
            if let Some(sender) = context.initial_ready.take() {
                let _ = sender.send(Ok(()));
            }
        }
        info!(
            account = %context.account.name,
            mailbox = %context.mailbox.name,
            "IMAP IDLE started"
        );

        let refresh = sleep(context.settings.idle_refresh);
        tokio::pin!(refresh);
        loop {
            let mut line = String::new();
            tokio::select! {
                changed = context.shutdown.changed() => {
                    if changed.is_ok() && *context.shutdown.borrow() {
                        let _ = finish_idle(
                            reader,
                            writer,
                            &tag,
                            context.settings.operation_timeout,
                            || {
                                queue_mailbox_event(
                                    context.events,
                                    context.state,
                                    &context.account.name,
                                    &context.mailbox.name,
                                );
                            },
                        )
                        .await;
                        return Ok(RunEnd::Shutdown);
                    }
                }
                () = &mut refresh => {
                    info!(
                        account = %context.account.name,
                        mailbox = %context.mailbox.name,
                        "IMAP IDLE refresh"
                    );
                    finish_idle(
                        reader,
                        writer,
                        &tag,
                        context.settings.operation_timeout,
                        || {
                            queue_mailbox_event(
                                context.events,
                                context.state,
                                &context.account.name,
                                &context.mailbox.name,
                            );
                        },
                    )
                    .await?;
                    context
                        .state
                        .mark_watcher(context.watcher_id, WatcherPhase::Idling);
                    break;
                }
                read = reader.read_line(&mut line) => {
                    let bytes = read?;
                    if bytes == 0 {
                        return Err(ImapError::ServerClosed);
                    }
                    let line = trim_line(&line);
                    match classify_idle_response(line, &tag)? {
                        IdleResponse::Completed => {
                            debug!(
                                account = %context.account.name,
                                mailbox = %context.mailbox.name,
                                "server ended IDLE; re-entering"
                            );
                            break;
                        }
                        IdleResponse::MailboxChange => {
                            context
                                .state
                                .mark_watcher(context.watcher_id, WatcherPhase::Idling);
                            queue_mailbox_event(
                                context.events,
                                context.state,
                                &context.account.name,
                                &context.mailbox.name,
                            );
                        }
                        IdleResponse::Other => {
                            context
                                .state
                                .mark_watcher(context.watcher_id, WatcherPhase::Idling);
                        }
                    }
                }
            }
        }
    }
}

async fn wait_idle_continuation<R>(reader: &mut R, tag: &str) -> Result<(), ImapError>
where
    R: AsyncBufRead + Unpin,
{
    loop {
        let line = read_line(reader).await?;
        if line.starts_with('+') {
            return Ok(());
        }
        if is_tagged_completion(&line, tag) {
            return Err(ImapError::CommandRejected { command: "IDLE" });
        }
    }
}

async fn wait_tagged<R>(reader: &mut R, tag: &str, command: &'static str) -> Result<(), ImapError>
where
    R: AsyncBufRead + Unpin,
{
    loop {
        let line = read_line(reader).await?;
        if is_tagged_ok(&line, tag) {
            return Ok(());
        }
        if is_tagged_completion(&line, tag) {
            return Err(ImapError::CommandRejected { command });
        }
    }
}

async fn finish_idle<R, W, F>(
    reader: &mut R,
    writer: &mut W,
    tag: &str,
    operation_timeout: Duration,
    mut on_mailbox_change: F,
) -> Result<(), ImapError>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
    F: FnMut(),
{
    with_imap_timeout(
        operation_timeout,
        "IDLE DONE",
        write_raw(writer, b"DONE\r\n"),
    )
    .await?;
    with_imap_timeout(
        operation_timeout,
        "IDLE tagged response",
        wait_idle_completion(reader, tag, &mut on_mailbox_change),
    )
    .await
}

async fn wait_idle_completion<R, F>(
    reader: &mut R,
    tag: &str,
    on_mailbox_change: &mut F,
) -> Result<(), ImapError>
where
    R: AsyncBufRead + Unpin,
    F: FnMut(),
{
    let mut event_queued = false;
    loop {
        let line = read_line(reader).await?;
        match classify_idle_response(&line, tag)? {
            IdleResponse::Completed => return Ok(()),
            IdleResponse::MailboxChange if !event_queued => {
                on_mailbox_change();
                event_queued = true;
            }
            IdleResponse::MailboxChange => {}
            IdleResponse::Other => {}
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IdleResponse {
    Completed,
    MailboxChange,
    Other,
}

fn classify_idle_response(line: &str, tag: &str) -> Result<IdleResponse, ImapError> {
    if line.to_ascii_uppercase().starts_with("* BYE") {
        return Err(ImapError::ServerClosed);
    }
    if is_tagged_ok(line, tag) {
        return Ok(IdleResponse::Completed);
    }
    if is_tagged_completion(line, tag) {
        return Err(ImapError::CommandRejected { command: "IDLE" });
    }
    if is_mailbox_change(line) {
        return Ok(IdleResponse::MailboxChange);
    }
    Ok(IdleResponse::Other)
}

fn queue_mailbox_event(
    events: &mpsc::Sender<()>,
    state: &RuntimeState,
    account_name: &str,
    mailbox_name: &str,
) {
    state.mark_event();
    match events.try_send(()) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(())) => {
            debug!(
                account = %account_name,
                mailbox = %mailbox_name,
                "mailbox event queue is full; coalescing event"
            );
        }
        Err(mpsc::error::TrySendError::Closed(())) => {
            warn!(
                account = %account_name,
                mailbox = %mailbox_name,
                "mailbox event queue is closed"
            );
        }
    }
}

async fn read_line<R>(reader: &mut R) -> Result<String, ImapError>
where
    R: AsyncBufRead + Unpin,
{
    let mut line = String::new();
    let mut limited = reader.take((MAX_RESPONSE_LINE_BYTES + 1) as u64);
    let bytes = limited.read_line(&mut line).await?;
    if bytes == 0 {
        return Err(ImapError::ServerClosed);
    }
    if bytes > MAX_RESPONSE_LINE_BYTES {
        return Err(ImapError::Protocol {
            context: "reading oversized IMAP response line",
        });
    }
    Ok(trim_line(&line).to_string())
}

async fn write_raw<W>(writer: &mut W, bytes: &[u8]) -> Result<(), ImapError>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(bytes).await?;
    writer.flush().await?;
    Ok(())
}

async fn with_imap_timeout<F, T, E>(
    duration: Duration,
    operation: &'static str,
    future: F,
) -> Result<T, ImapError>
where
    F: Future<Output = Result<T, E>>,
    E: Into<ImapError>,
{
    match timeout(duration, future).await {
        Ok(result) => result.map_err(Into::into),
        Err(_) => Err(ImapError::OperationTimedOut {
            operation,
            seconds: duration.as_secs(),
        }),
    }
}

fn validate_imap_string(field: &'static str, value: &str) -> Result<(), ImapError> {
    if value.chars().any(|ch| matches!(ch, '\r' | '\n')) {
        return Err(ImapError::UnsafeImapString { field });
    }
    Ok(())
}

async fn sleep_or_shutdown(duration: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        () = sleep(duration) => true,
        changed = shutdown.changed() => !(changed.is_ok() && *shutdown.borrow()),
    }
}

fn xoauth2_response(username: &str, token: &str) -> String {
    let raw = format!("user={username}\x01auth=Bearer {token}\x01\x01");
    BASE64.encode(raw)
}

fn quote_imap(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    for ch in value.chars() {
        match ch {
            '\\' => quoted.push_str("\\\\"),
            '"' => quoted.push_str("\\\""),
            _ => quoted.push(ch),
        }
    }
    quoted.push('"');
    quoted
}

fn trim_line(line: &str) -> &str {
    line.trim_end_matches(['\r', '\n'])
}

fn is_tagged_completion(line: &str, tag: &str) -> bool {
    line.strip_prefix(tag)
        .and_then(|rest| rest.strip_prefix(' '))
        .is_some()
}

fn is_tagged_ok(line: &str, tag: &str) -> bool {
    let Some(rest) = line
        .strip_prefix(tag)
        .and_then(|rest| rest.strip_prefix(' '))
    else {
        return false;
    };
    rest.split_whitespace()
        .next()
        .is_some_and(|status| status.eq_ignore_ascii_case("OK"))
}

fn is_mailbox_change(line: &str) -> bool {
    let upper = line.to_ascii_uppercase();
    if !upper.starts_with("* ") {
        return false;
    }
    upper.contains(" EXISTS")
        || upper.contains(" EXPUNGE")
        || upper.contains(" RECENT")
        || upper.contains(" FETCH")
        || upper.contains(" VANISHED")
}

#[derive(Default)]
struct Tags {
    next: u64,
}

impl Tags {
    fn next(&mut self) -> String {
        self.next += 1;
        format!("A{:04}", self.next)
    }
}

impl fmt::Debug for dyn ImapStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<imap stream>")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn imap_quoting_escapes_special_chars() {
        assert_eq!(quote_imap("INBOX"), "\"INBOX\"");
        assert_eq!(quote_imap("a\\b\"c"), "\"a\\\\b\\\"c\"");
    }

    #[test]
    fn xoauth2_response_does_not_contain_plain_token() {
        let response = xoauth2_response("user@example.com", "secret-token");
        assert!(!response.contains("secret-token"));
        let decoded = BASE64.decode(response).expect("valid base64");
        assert!(String::from_utf8(decoded).unwrap().contains("secret-token"));
    }

    #[test]
    fn detects_mailbox_change_lines() {
        assert!(is_mailbox_change("* 23 EXISTS"));
        assert!(is_mailbox_change("* 1 FETCH (FLAGS (\\Seen))"));
        assert!(is_mailbox_change("* 2 EXPUNGE"));
        assert!(!is_mailbox_change("* OK Still here"));
        assert!(!is_mailbox_change("A0001 OK IDLE completed"));
    }

    #[tokio::test]
    async fn rejects_oversized_response_lines() {
        let response = format!("* OK {}\r\n", "x".repeat(MAX_RESPONSE_LINE_BYTES));
        let mut reader = BufReader::new(response.as_bytes());
        assert!(matches!(
            read_line(&mut reader).await,
            Err(ImapError::Protocol {
                context: "reading oversized IMAP response line"
            })
        ));
    }

    #[tokio::test]
    async fn mailbox_open_uses_read_only_examine() {
        let (client, server) = tokio::io::duplex(1024);
        let (client_read, mut client_write) = tokio::io::split(client);
        let (server_read, mut server_write) = tokio::io::split(server);
        let mut client_reader = BufReader::new(client_read);
        let mut server_reader = BufReader::new(server_read);
        let mut tags = Tags::default();

        let server = tokio::spawn(async move {
            let mut command = String::new();
            server_reader.read_line(&mut command).await.unwrap();
            assert_eq!(command, "A0001 EXAMINE \"INBOX\"\r\n");
            server_write
                .write_all(b"A0001 OK [READ-ONLY] EXAMINE completed\r\n")
                .await
                .unwrap();
        });

        examine_mailbox(&mut client_reader, &mut client_write, &mut tags, "INBOX")
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn idle_completion_queues_untagged_mailbox_changes_once() {
        let (client_stream, server_stream) = tokio::io::duplex(1024);
        let (client_read, mut client_write) = tokio::io::split(client_stream);
        let (server_read, mut server_write) = tokio::io::split(server_stream);
        let mut server_reader = BufReader::new(server_read);
        let (events_tx, mut events_rx) = mpsc::channel(4);
        let events_keepalive = events_tx.clone();

        let client = tokio::spawn(async move {
            let mut client_reader = BufReader::new(client_read);
            let state =
                RuntimeState::new(1, 1, 0, Duration::from_secs(60), Duration::from_secs(60));
            finish_idle(
                &mut client_reader,
                &mut client_write,
                "A0001",
                Duration::from_secs(1),
                move || {
                    queue_mailbox_event(&events_tx, &state, "example-account", "INBOX");
                },
            )
            .await
        });

        let mut command = String::new();
        server_reader.read_line(&mut command).await.unwrap();
        assert_eq!(command, "DONE\r\n");
        server_write
            .write_all(b"* 2 EXISTS\r\n* 1 FETCH (FLAGS (\\Seen))\r\n")
            .await
            .unwrap();

        assert_eq!(
            timeout(Duration::from_secs(1), events_rx.recv())
                .await
                .unwrap(),
            Some(())
        );
        assert!(
            !client.is_finished(),
            "IDLE must wait for its tagged completion"
        );

        server_write
            .write_all(b"A0001 OK IDLE terminated\r\n")
            .await
            .unwrap();
        client.await.unwrap().unwrap();
        assert!(matches!(
            events_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        drop(events_keepalive);
    }

    #[test]
    fn rejects_crlf_in_imap_strings() {
        assert!(validate_imap_string("username", "user@example.com").is_ok());
        assert!(validate_imap_string("username", "user\n@example.com").is_err());
        assert!(validate_imap_string("mailbox name", "IN\rBOX").is_err());
        assert!(validate_imap_string("password", "secret\nnext").is_err());
    }
}
