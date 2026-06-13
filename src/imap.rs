use crate::auth::{self, AuthError, Credentials};
use crate::config::{AccountConfig, MailboxConfig, SecretString};
use crate::state::{RuntimeState, WatcherPhase};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use std::future::Future;
use std::time::Duration;
use std::{fmt, sync::Arc};
use thiserror::Error;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{sleep, timeout};
use tokio_native_tls::TlsConnector;
use tracing::{debug, info, warn};

const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(300);

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

pub async fn watch_mailbox_forever(task: MailboxWatchTask) {
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
        "SELECT",
        select_mailbox(
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
        "IMAP login/select succeeded; entering IDLE"
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

async fn select_mailbox<R, W>(
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
    let command = format!("{tag} SELECT {}\r\n", quote_imap(mailbox));
    write_raw(writer, command.as_bytes()).await?;
    wait_tagged(reader, &tag, "SELECT").await
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
                        let _ = finish_idle(reader, writer, &tag, context.settings.operation_timeout).await;
                        return Ok(RunEnd::Shutdown);
                    }
                }
                () = &mut refresh => {
                    info!(
                        account = %context.account.name,
                        mailbox = %context.mailbox.name,
                        "IMAP IDLE refresh"
                    );
                    finish_idle(reader, writer, &tag, context.settings.operation_timeout).await?;
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
                    if line.to_ascii_uppercase().starts_with("* BYE") {
                        return Err(ImapError::ServerClosed);
                    }
                    if is_tagged_ok(line, &tag) {
                        debug!(
                            account = %context.account.name,
                            mailbox = %context.mailbox.name,
                            "server ended IDLE; re-entering"
                        );
                        break;
                    }
                    if is_tagged_completion(line, &tag) {
                        return Err(ImapError::CommandRejected { command: "IDLE" });
                    }
                    context
                        .state
                        .mark_watcher(context.watcher_id, WatcherPhase::Idling);
                    if is_mailbox_change(line) {
                        context.state.mark_event();
                        match context.events.try_send(()) {
                            Ok(()) => {}
                            Err(mpsc::error::TrySendError::Full(())) => {
                                debug!(
                                    account = %context.account.name,
                                    mailbox = %context.mailbox.name,
                                    "mailbox event queue is full; coalescing event"
                                );
                            }
                            Err(mpsc::error::TrySendError::Closed(())) => {
                                warn!(
                                    account = %context.account.name,
                                    mailbox = %context.mailbox.name,
                                    "mailbox event queue is closed"
                                );
                            }
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

async fn finish_idle<R, W>(
    reader: &mut R,
    writer: &mut W,
    tag: &str,
    operation_timeout: Duration,
) -> Result<(), ImapError>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
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
        wait_tagged(reader, tag, "IDLE"),
    )
    .await
}

async fn read_line<R>(reader: &mut R) -> Result<String, ImapError>
where
    R: AsyncBufRead + Unpin,
{
    let mut line = String::new();
    let bytes = reader.read_line(&mut line).await?;
    if bytes == 0 {
        return Err(ImapError::ServerClosed);
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

    #[test]
    fn rejects_crlf_in_imap_strings() {
        assert!(validate_imap_string("username", "user@example.com").is_ok());
        assert!(validate_imap_string("username", "user\n@example.com").is_err());
        assert!(validate_imap_string("mailbox name", "IN\rBOX").is_err());
        assert!(validate_imap_string("password", "secret\nnext").is_err());
    }
}
