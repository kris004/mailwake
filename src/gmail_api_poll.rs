use crate::auth::{self, AuthError};
use crate::config::{GmailApiPollSourceConfig, SecretString};
use crate::state::{RuntimeState, WatcherPhase};
use reqwest::StatusCode;
use reqwest::header::CONTENT_TYPE;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::sleep;
use tracing::{debug, info, warn};

const GMAIL_API_ROOT: &str = "https://gmail.googleapis.com/gmail/v1/users/me";
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(300);
const MAX_GOOGLE_ERROR_BODY_BYTES: usize = 64 * 1024;

pub const INITIAL_AUTH_FAILURE_PREFIX: &str = "Gmail API poller authentication failed";

#[derive(Clone, Copy, Debug)]
pub struct GmailApiPollSettings {
    pub auth_helper_timeout: Duration,
    pub auth_helper_max_output_bytes: usize,
    pub poll_interval: Duration,
    pub api_timeout: Duration,
}

pub struct GmailApiPollTask {
    pub source: GmailApiPollSourceConfig,
    pub events: mpsc::Sender<()>,
    pub state: Arc<RuntimeState>,
    pub watcher_id: String,
    pub initial_ready: Option<oneshot::Sender<Result<(), String>>>,
    pub shutdown: watch::Receiver<bool>,
    pub settings: GmailApiPollSettings,
}

#[derive(Debug, Error)]
pub enum GmailApiPollError {
    #[error("auth helper failed: {0}")]
    Auth(#[from] AuthError),
    #[error("could not build Gmail API HTTP client: {source}")]
    HttpClient {
        #[source]
        source: reqwest::Error,
    },
    #[error("Gmail API {operation} request failed: {source}")]
    Request {
        operation: &'static str,
        #[source]
        source: reqwest::Error,
    },
    #[error("Gmail API {operation} returned HTTP {status}")]
    HttpStatus {
        operation: &'static str,
        status: StatusCode,
        reason: Option<GoogleErrorReason>,
    },
    #[error("Gmail history baseline is too old")]
    StaleHistory,
    #[error("unexpected Gmail API response while {context}")]
    Protocol { context: &'static str },
}

impl GmailApiPollError {
    pub fn is_permanent_auth_failure(&self) -> bool {
        matches!(
            self,
            Self::Auth(AuthError::HelperReauthRequired)
                | Self::HttpStatus {
                    status: StatusCode::UNAUTHORIZED,
                    ..
                }
                | Self::HttpStatus {
                    status: StatusCode::FORBIDDEN,
                    reason: Some(
                        GoogleErrorReason::InsufficientPermissions
                            | GoogleErrorReason::DomainPolicy
                    ),
                    ..
                }
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GoogleErrorReason {
    InsufficientPermissions,
    DomainPolicy,
    DailyLimitExceeded,
    RateLimitExceeded,
    UserRateLimitExceeded,
}

impl GoogleErrorReason {
    fn from_str(value: &str) -> Option<Self> {
        match value {
            "insufficientPermissions" => Some(Self::InsufficientPermissions),
            "domainPolicy" => Some(Self::DomainPolicy),
            "dailyLimitExceeded" => Some(Self::DailyLimitExceeded),
            "rateLimitExceeded" => Some(Self::RateLimitExceeded),
            "userRateLimitExceeded" => Some(Self::UserRateLimitExceeded),
            _ => None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct GoogleErrorEnvelope {
    error: GoogleErrorBody,
}

#[derive(Debug, Deserialize)]
struct GoogleErrorBody {
    #[serde(default)]
    errors: Vec<GoogleErrorDetail>,
}

#[derive(Debug, Deserialize)]
struct GoogleErrorDetail {
    reason: Option<String>,
}

#[derive(Clone)]
pub struct ReqwestGmailApiClient {
    client: reqwest::Client,
}

impl ReqwestGmailApiClient {
    pub fn new(timeout: Duration) -> Result<Self, GmailApiPollError> {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|source| GmailApiPollError::HttpClient { source })?;
        Ok(Self { client })
    }

    async fn get_profile(
        &self,
        token: &SecretString,
    ) -> Result<ProfileResponse, GmailApiPollError> {
        let response = self
            .client
            .get(format!("{GMAIL_API_ROOT}/profile"))
            .bearer_auth(token.expose_secret())
            .header(CONTENT_TYPE, "application/json")
            .send()
            .await
            .map_err(|source| GmailApiPollError::Request {
                operation: "getProfile",
                source,
            })?;
        decode_response(response, "getProfile").await
    }

    async fn list_history(
        &self,
        token: &SecretString,
        start_history_id: u64,
        label_id: &str,
        page_token: Option<&str>,
        max_results: u32,
    ) -> Result<HistoryListResponse, GmailApiPollError> {
        let query = history_query(start_history_id, label_id, page_token, max_results);
        let response = self
            .client
            .get(format!("{GMAIL_API_ROOT}/history"))
            .bearer_auth(token.expose_secret())
            .header(CONTENT_TYPE, "application/json")
            .query(&query)
            .send()
            .await
            .map_err(|source| GmailApiPollError::Request {
                operation: "history.list",
                source,
            })?;
        decode_response(response, "history.list").await
    }
}

#[derive(Debug, Deserialize)]
struct ProfileResponse {
    #[serde(rename = "historyId")]
    history_id: String,
}

#[derive(Debug, Deserialize)]
struct HistoryListResponse {
    #[serde(default)]
    history: Vec<HistoryRecord>,
    #[serde(rename = "nextPageToken")]
    next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct HistoryRecord {
    id: String,
}

#[derive(Debug, Eq, PartialEq)]
pub enum HistoryDecision {
    Unchanged,
    Advanced { from: u64, to: u64 },
}

#[derive(Default, Debug)]
pub struct GmailHistoryTracker {
    last_history_id: Option<u64>,
}

impl GmailHistoryTracker {
    pub fn baseline(&mut self, history_id: u64) {
        self.last_history_id = Some(history_id);
    }

    pub fn has_baseline(&self) -> bool {
        self.last_history_id.is_some()
    }

    pub fn compare(&self, latest_history_id: u64) -> Result<HistoryDecision, GmailApiPollError> {
        let Some(last_history_id) = self.last_history_id else {
            return Err(GmailApiPollError::Protocol {
                context: "comparing Gmail history before baseline",
            });
        };
        if latest_history_id > last_history_id {
            Ok(HistoryDecision::Advanced {
                from: last_history_id,
                to: latest_history_id,
            })
        } else {
            Ok(HistoryDecision::Unchanged)
        }
    }

    pub fn accept(&mut self, history_id: u64) {
        self.last_history_id = Some(history_id);
    }
}

pub async fn watch_gmail_api_poll_forever(task: GmailApiPollTask) -> Result<(), GmailApiPollError> {
    let GmailApiPollTask {
        source,
        events,
        state,
        watcher_id,
        mut initial_ready,
        mut shutdown,
        settings,
    } = task;
    let client = ReqwestGmailApiClient::new(settings.api_timeout)?;
    let mut tracker = GmailHistoryTracker::default();
    let mut backoff = INITIAL_BACKOFF;

    loop {
        if *shutdown.borrow() {
            break;
        }

        if !tracker.has_baseline() {
            state.mark_watcher(&watcher_id, WatcherPhase::Connecting);
            match baseline_history(&client, &source, settings).await {
                Ok(history_id) => {
                    tracker.baseline(history_id);
                    state.mark_watcher(&watcher_id, WatcherPhase::Idling);
                    if let Some(sender) = initial_ready.take() {
                        let _ = sender.send(Ok(()));
                    }
                    backoff = INITIAL_BACKOFF;
                    info!(source = %source.name, "Gmail API poll baseline established");
                }
                Err(error) if error.is_permanent_auth_failure() => {
                    fail_initial_ready(&mut initial_ready, &error);
                    state.mark_watcher(&watcher_id, WatcherPhase::Crashed);
                    warn!(source = %source.name, %error, "Gmail API poller authentication or permission failure; stopping");
                    return Err(error);
                }
                Err(error) => {
                    warn!(source = %source.name, %error, "Gmail API poll baseline failed; retrying");
                    state.mark_watcher(&watcher_id, WatcherPhase::Reconnecting);
                    if !sleep_or_shutdown(backoff, &mut shutdown).await {
                        break;
                    }
                    backoff = std::cmp::min(backoff * 2, MAX_BACKOFF);
                    continue;
                }
            }
        }

        if !sleep_or_shutdown(settings.poll_interval, &mut shutdown).await {
            break;
        }

        match poll_once(&client, &source, &events, &mut tracker, settings).await {
            Ok(()) => {
                state.mark_watcher_progress(&watcher_id);
                backoff = INITIAL_BACKOFF;
            }
            Err(error) if error.is_permanent_auth_failure() => {
                fail_initial_ready(&mut initial_ready, &error);
                state.mark_watcher(&watcher_id, WatcherPhase::Crashed);
                warn!(source = %source.name, %error, "Gmail API poller authentication or permission failure; stopping");
                return Err(error);
            }
            Err(error) => {
                warn!(source = %source.name, %error, "Gmail API poll failed; retrying");
                state.mark_watcher(&watcher_id, WatcherPhase::Reconnecting);
                if !sleep_or_shutdown(backoff, &mut shutdown).await {
                    break;
                }
                backoff = std::cmp::min(backoff * 2, MAX_BACKOFF);
                state.mark_watcher(&watcher_id, WatcherPhase::Idling);
            }
        }
    }

    state.mark_watcher(&watcher_id, WatcherPhase::Stopped);
    info!(source = %source.name, "Gmail API poller stopped");
    Ok(())
}

async fn baseline_history(
    client: &ReqwestGmailApiClient,
    source: &GmailApiPollSourceConfig,
    settings: GmailApiPollSettings,
) -> Result<u64, GmailApiPollError> {
    let token = token_from_helper(&source.gmail_token_cmd, settings).await?;
    let profile = client.get_profile(&token).await?;
    parse_history_id(&profile.history_id, "profile historyId")
}

async fn poll_once(
    client: &ReqwestGmailApiClient,
    source: &GmailApiPollSourceConfig,
    events: &mpsc::Sender<()>,
    tracker: &mut GmailHistoryTracker,
    settings: GmailApiPollSettings,
) -> Result<(), GmailApiPollError> {
    let token = token_from_helper(&source.gmail_token_cmd, settings).await?;
    let profile = client.get_profile(&token).await?;
    let latest_history_id = parse_history_id(&profile.history_id, "profile historyId")?;
    let decision = tracker.compare(latest_history_id)?;

    let HistoryDecision::Advanced { from, to } = decision else {
        debug!(source = %source.name, "Gmail API poll saw no new history");
        return Ok(());
    };

    let should_trigger = if source.label_ids.is_empty() {
        true
    } else {
        match has_relevant_history(client, source, &token, from).await? {
            RelevantHistory::Changed => true,
            RelevantHistory::NoChange => false,
            RelevantHistory::Stale => {
                warn!(source = %source.name, "Gmail history baseline is too old; triggering once before rebaseline");
                true
            }
        }
    };
    tracker.accept(to);

    if should_trigger {
        queue_source_event(&source.name, events);
    } else {
        debug!(source = %source.name, "Gmail API poll history did not match configured labels");
    }
    Ok(())
}

#[derive(Debug, Eq, PartialEq)]
enum RelevantHistory {
    Changed,
    NoChange,
    Stale,
}

async fn has_relevant_history(
    client: &ReqwestGmailApiClient,
    source: &GmailApiPollSourceConfig,
    token: &SecretString,
    start_history_id: u64,
) -> Result<RelevantHistory, GmailApiPollError> {
    for label_id in &source.label_ids {
        match label_has_history(client, source, token, start_history_id, label_id).await {
            Ok(true) => return Ok(RelevantHistory::Changed),
            Ok(false) => {}
            Err(GmailApiPollError::StaleHistory) => return Ok(RelevantHistory::Stale),
            Err(error) => return Err(error),
        }
    }
    Ok(RelevantHistory::NoChange)
}

async fn label_has_history(
    client: &ReqwestGmailApiClient,
    source: &GmailApiPollSourceConfig,
    token: &SecretString,
    start_history_id: u64,
    label_id: &str,
) -> Result<bool, GmailApiPollError> {
    let mut page_token = None;
    loop {
        let page = client
            .list_history(
                token,
                start_history_id,
                label_id,
                page_token.as_deref(),
                source.history_page_size(),
            )
            .await?;
        if history_page_has_changes(&page) {
            return Ok(true);
        }
        let Some(next_page_token) = page.next_page_token else {
            return Ok(false);
        };
        page_token = Some(next_page_token);
    }
}

async fn token_from_helper(
    command: &str,
    settings: GmailApiPollSettings,
) -> Result<SecretString, GmailApiPollError> {
    auth::run_secret_command(
        command,
        settings.auth_helper_timeout,
        settings.auth_helper_max_output_bytes,
    )
    .await
    .map_err(GmailApiPollError::Auth)
}

async fn decode_response<T: DeserializeOwned>(
    mut response: reqwest::Response,
    operation: &'static str,
) -> Result<T, GmailApiPollError> {
    let status = response.status();
    if status == StatusCode::NOT_FOUND && operation == "history.list" {
        return Err(GmailApiPollError::StaleHistory);
    }
    if !status.is_success() {
        let reason = if status == StatusCode::FORBIDDEN {
            read_google_error_reason(&mut response).await
        } else {
            None
        };
        return Err(GmailApiPollError::HttpStatus {
            operation,
            status,
            reason,
        });
    }
    response
        .json::<T>()
        .await
        .map_err(|source| GmailApiPollError::Request { operation, source })
}

async fn read_google_error_reason(response: &mut reqwest::Response) -> Option<GoogleErrorReason> {
    let mut body = Vec::new();
    loop {
        let chunk = match response.chunk().await {
            Ok(Some(chunk)) => chunk,
            Ok(None) => break,
            Err(_) => return None,
        };
        if body.len().saturating_add(chunk.len()) > MAX_GOOGLE_ERROR_BODY_BYTES {
            return None;
        }
        body.extend_from_slice(&chunk);
    }
    parse_google_error_reason(&body)
}

fn parse_google_error_reason(body: &[u8]) -> Option<GoogleErrorReason> {
    if body.len() > MAX_GOOGLE_ERROR_BODY_BYTES {
        return None;
    }

    let response = serde_json::from_slice::<GoogleErrorEnvelope>(body).ok()?;
    response
        .error
        .errors
        .iter()
        .filter_map(|error| error.reason.as_deref())
        .find_map(GoogleErrorReason::from_str)
}

fn parse_history_id(value: &str, context: &'static str) -> Result<u64, GmailApiPollError> {
    value
        .parse::<u64>()
        .map_err(|_| GmailApiPollError::Protocol { context })
}

fn history_query(
    start_history_id: u64,
    label_id: &str,
    page_token: Option<&str>,
    max_results: u32,
) -> Vec<(&'static str, String)> {
    let mut query = vec![
        ("startHistoryId", start_history_id.to_string()),
        ("labelId", label_id.to_string()),
        ("maxResults", max_results.to_string()),
    ];
    if let Some(page_token) = page_token {
        query.push(("pageToken", page_token.to_string()));
    }
    query
}

fn history_page_has_changes(page: &HistoryListResponse) -> bool {
    page.history.iter().any(|record| !record.id.is_empty())
}

fn queue_source_event(source_name: &str, events: &mpsc::Sender<()>) {
    match events.try_send(()) {
        Ok(()) => info!(source = %source_name, "queued Gmail API poll source event"),
        Err(mpsc::error::TrySendError::Full(())) => {
            debug!(source = %source_name, "Gmail API poll source event queue is full; coalescing event");
        }
        Err(mpsc::error::TrySendError::Closed(())) => {
            warn!(source = %source_name, "Gmail API poll source event queue is closed");
        }
    }
}

fn fail_initial_ready(
    initial_ready: &mut Option<oneshot::Sender<Result<(), String>>>,
    error: &GmailApiPollError,
) {
    if let Some(sender) = initial_ready.take() {
        let _ = sender.send(Err(format!("{INITIAL_AUTH_FAILURE_PREFIX}: {error}")));
    }
}

async fn sleep_or_shutdown(duration: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    if duration.is_zero() {
        return true;
    }
    tokio::select! {
        () = sleep(duration) => true,
        changed = shutdown.changed() => !(changed.is_ok() && *shutdown.borrow()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_tracker_detects_only_monotonic_advancement() {
        let mut tracker = GmailHistoryTracker::default();
        tracker.baseline(10);
        assert_eq!(tracker.compare(10).unwrap(), HistoryDecision::Unchanged);
        assert_eq!(tracker.compare(9).unwrap(), HistoryDecision::Unchanged);
        assert_eq!(
            tracker.compare(11).unwrap(),
            HistoryDecision::Advanced { from: 10, to: 11 }
        );
        tracker.accept(11);
        assert_eq!(tracker.compare(11).unwrap(), HistoryDecision::Unchanged);
    }

    #[test]
    fn parse_history_id_rejects_non_numeric_values() {
        assert_eq!(parse_history_id("42", "test").unwrap(), 42);
        assert!(parse_history_id("not-a-number", "test").is_err());
    }

    #[test]
    fn history_query_includes_label_page_token_and_limit() {
        let query = history_query(42, "INBOX", Some("next-page"), 100);
        assert_eq!(
            query,
            vec![
                ("startHistoryId", "42".to_string()),
                ("labelId", "INBOX".to_string()),
                ("maxResults", "100".to_string()),
                ("pageToken", "next-page".to_string())
            ]
        );
    }

    #[test]
    fn history_page_change_detection_ignores_empty_records() {
        let empty = HistoryListResponse {
            history: Vec::new(),
            next_page_token: None,
        };
        assert!(!history_page_has_changes(&empty));

        let changed = HistoryListResponse {
            history: vec![HistoryRecord { id: "12".into() }],
            next_page_token: None,
        };
        assert!(history_page_has_changes(&changed));
    }

    #[test]
    fn explicit_reauthorization_and_unauthorized_errors_are_permanent() {
        assert!(
            GmailApiPollError::Auth(AuthError::HelperReauthRequired).is_permanent_auth_failure()
        );
        assert!(
            GmailApiPollError::HttpStatus {
                operation: "getProfile",
                status: StatusCode::UNAUTHORIZED,
                reason: None,
            }
            .is_permanent_auth_failure()
        );
    }

    #[test]
    fn transient_and_malformed_helper_failures_are_retryable() {
        let errors = [
            AuthError::HelperTimedOut { seconds: 5 },
            AuthError::HelperFailed {
                status: "exit status: 1".to_string(),
            },
            AuthError::HelperOutputTooLarge { limit: 1024 },
            AuthError::HelperOutputUtf8,
            AuthError::EmptySecret,
        ];
        for error in errors {
            assert!(!GmailApiPollError::Auth(error).is_permanent_auth_failure());
        }

        let start_error = AuthError::HelperStart {
            source: std::io::Error::other("temporary failure"),
        };
        assert!(!GmailApiPollError::Auth(start_error).is_permanent_auth_failure());
    }

    #[test]
    fn known_google_permission_reasons_are_permanent() {
        for reason in [
            GoogleErrorReason::InsufficientPermissions,
            GoogleErrorReason::DomainPolicy,
        ] {
            assert!(
                GmailApiPollError::HttpStatus {
                    operation: "getProfile",
                    status: StatusCode::FORBIDDEN,
                    reason: Some(reason),
                }
                .is_permanent_auth_failure()
            );
        }
    }

    #[test]
    fn quota_rate_and_ambiguous_http_errors_are_retryable() {
        for reason in [
            GoogleErrorReason::DailyLimitExceeded,
            GoogleErrorReason::RateLimitExceeded,
            GoogleErrorReason::UserRateLimitExceeded,
        ] {
            assert!(
                !GmailApiPollError::HttpStatus {
                    operation: "getProfile",
                    status: StatusCode::FORBIDDEN,
                    reason: Some(reason),
                }
                .is_permanent_auth_failure()
            );
        }
        assert!(
            !GmailApiPollError::HttpStatus {
                operation: "getProfile",
                status: StatusCode::FORBIDDEN,
                reason: None,
            }
            .is_permanent_auth_failure()
        );
        assert!(
            !GmailApiPollError::HttpStatus {
                operation: "getProfile",
                status: StatusCode::TOO_MANY_REQUESTS,
                reason: None,
            }
            .is_permanent_auth_failure()
        );
        assert!(
            !GmailApiPollError::HttpStatus {
                operation: "getProfile",
                status: StatusCode::SERVICE_UNAVAILABLE,
                reason: None,
            }
            .is_permanent_auth_failure()
        );
    }

    #[test]
    fn google_error_reason_parser_accepts_only_bounded_known_reason_fields() {
        let cases = [
            (
                "insufficientPermissions",
                GoogleErrorReason::InsufficientPermissions,
            ),
            ("domainPolicy", GoogleErrorReason::DomainPolicy),
            ("dailyLimitExceeded", GoogleErrorReason::DailyLimitExceeded),
            ("rateLimitExceeded", GoogleErrorReason::RateLimitExceeded),
            (
                "userRateLimitExceeded",
                GoogleErrorReason::UserRateLimitExceeded,
            ),
        ];
        for (value, expected) in cases {
            let body = format!(
                r#"{{"error":{{"errors":[{{"domain":"usageLimits","reason":"{value}"}}]}}}}"#
            );
            assert_eq!(parse_google_error_reason(body.as_bytes()), Some(expected));
        }

        assert_eq!(
            parse_google_error_reason(br#"{"error":{"errors":[{"reason":"backendError"}]}}"#),
            None
        );
        assert_eq!(
            parse_google_error_reason(
                br#"{"error":{"message":"\"reason\": \"insufficientPermissions\""}}"#
            ),
            None
        );

        let oversized = vec![b' '; MAX_GOOGLE_ERROR_BODY_BYTES + 1];
        assert_eq!(parse_google_error_reason(&oversized), None);
    }
}
