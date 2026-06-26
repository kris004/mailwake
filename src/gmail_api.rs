use crate::auth::{self, AuthError};
use crate::config::{GmailApiWatchSourceConfig, GmailLabelFilterBehavior, SecretString};
use base64::Engine;
use base64::engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use thiserror::Error;

pub const INITIAL_AUTH_FAILURE_PREFIX: &str = "Gmail API watcher authentication failed";

pub type ApiFuture<T> = Pin<Box<dyn Future<Output = Result<T, GmailApiError>> + Send>>;

pub trait GmailApiClient: Send + Sync {
    fn watch(&self, token: SecretString, request: WatchRequest) -> ApiFuture<WatchResponse>;

    fn pull(
        &self,
        token: SecretString,
        subscription: String,
        max_messages: u32,
    ) -> ApiFuture<PullResponse>;

    fn acknowledge(
        &self,
        token: SecretString,
        subscription: String,
        ack_ids: Vec<String>,
    ) -> ApiFuture<()>;
}

#[derive(Clone, Copy, Debug)]
pub struct GmailApiSettings {
    pub auth_helper_timeout: Duration,
    pub auth_helper_max_output_bytes: usize,
    pub request_timeout: Duration,
    pub watch_renewal: Duration,
    pub pull_max_messages: u32,
    pub empty_pull_delay: Duration,
}

#[derive(Debug, Error)]
pub enum GmailApiError {
    #[error("auth helper failed: {0}")]
    Auth(#[from] AuthError),
    #[error("could not build Gmail API HTTP client")]
    ClientBuild {
        #[source]
        source: reqwest::Error,
    },
    #[error("{operation} request failed")]
    Transport {
        operation: &'static str,
        #[source]
        source: reqwest::Error,
    },
    #[error("{operation} returned HTTP {status}")]
    Http {
        operation: &'static str,
        status: StatusCode,
    },
    #[error("{operation} returned invalid JSON")]
    Json {
        operation: &'static str,
        #[source]
        source: reqwest::Error,
    },
    #[error("Gmail notification payload was invalid")]
    InvalidNotification,
    #[error("{field} history id {value:?} was not a valid integer")]
    InvalidHistoryId { field: &'static str, value: String },
}

impl GmailApiError {
    pub fn is_permanent_auth_failure(&self) -> bool {
        matches!(
            self,
            Self::Auth(_)
                | Self::Http {
                    status: StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN,
                    ..
                }
        )
    }
}

pub async fn token_from_helper(
    command: &str,
    settings: GmailApiSettings,
) -> Result<SecretString, GmailApiError> {
    auth::run_secret_command(
        command,
        settings.auth_helper_timeout,
        settings.auth_helper_max_output_bytes,
    )
    .await
    .map_err(GmailApiError::Auth)
}

#[derive(Clone)]
pub struct ReqwestGmailApiClient {
    client: reqwest::Client,
}

impl ReqwestGmailApiClient {
    pub fn new(request_timeout: Duration) -> Result<Self, GmailApiError> {
        let client = reqwest::Client::builder()
            .timeout(request_timeout)
            .build()
            .map_err(|source| GmailApiError::ClientBuild { source })?;
        Ok(Self { client })
    }
}

impl GmailApiClient for ReqwestGmailApiClient {
    fn watch(&self, token: SecretString, request: WatchRequest) -> ApiFuture<WatchResponse> {
        let client = self.client.clone();
        Box::pin(async move {
            let response = client
                .post("https://gmail.googleapis.com/gmail/v1/users/me/watch")
                .bearer_auth(token.expose_secret())
                .json(&request)
                .send()
                .await
                .map_err(|source| GmailApiError::Transport {
                    operation: "Gmail watch",
                    source,
                })?;
            decode_response(response, "Gmail watch").await
        })
    }

    fn pull(
        &self,
        token: SecretString,
        subscription: String,
        max_messages: u32,
    ) -> ApiFuture<PullResponse> {
        let client = self.client.clone();
        Box::pin(async move {
            let url = format!("https://pubsub.googleapis.com/v1/{subscription}:pull");
            let request = PullRequest { max_messages };
            let response = client
                .post(url)
                .bearer_auth(token.expose_secret())
                .json(&request)
                .send()
                .await
                .map_err(|source| GmailApiError::Transport {
                    operation: "Pub/Sub pull",
                    source,
                })?;
            decode_response(response, "Pub/Sub pull").await
        })
    }

    fn acknowledge(
        &self,
        token: SecretString,
        subscription: String,
        ack_ids: Vec<String>,
    ) -> ApiFuture<()> {
        let client = self.client.clone();
        Box::pin(async move {
            if ack_ids.is_empty() {
                return Ok(());
            }
            let url = format!("https://pubsub.googleapis.com/v1/{subscription}:acknowledge");
            let request = AcknowledgeRequest { ack_ids };
            let response = client
                .post(url)
                .bearer_auth(token.expose_secret())
                .json(&request)
                .send()
                .await
                .map_err(|source| GmailApiError::Transport {
                    operation: "Pub/Sub acknowledge",
                    source,
                })?;
            ensure_success(response, "Pub/Sub acknowledge").await
        })
    }
}

async fn decode_response<T>(
    response: reqwest::Response,
    operation: &'static str,
) -> Result<T, GmailApiError>
where
    T: for<'de> Deserialize<'de>,
{
    let response = ensure_success_response(response, operation).await?;
    response
        .json::<T>()
        .await
        .map_err(|source| GmailApiError::Json { operation, source })
}

async fn ensure_success(
    response: reqwest::Response,
    operation: &'static str,
) -> Result<(), GmailApiError> {
    let _ = ensure_success_response(response, operation).await?;
    Ok(())
}

async fn ensure_success_response(
    response: reqwest::Response,
    operation: &'static str,
) -> Result<reqwest::Response, GmailApiError> {
    let status = response.status();
    if status.is_success() {
        Ok(response)
    } else {
        Err(GmailApiError::Http { operation, status })
    }
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WatchRequest {
    pub topic_name: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub label_ids: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label_filter_behavior: Option<ApiLabelFilterBehavior>,
}

impl WatchRequest {
    pub fn from_source(source: &GmailApiWatchSourceConfig) -> Self {
        Self {
            topic_name: source.topic_name.clone(),
            label_ids: source.label_ids.clone(),
            label_filter_behavior: source.label_filter_behavior.map(Into::into),
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ApiLabelFilterBehavior {
    Include,
    Exclude,
}

impl From<GmailLabelFilterBehavior> for ApiLabelFilterBehavior {
    fn from(value: GmailLabelFilterBehavior) -> Self {
        match value {
            GmailLabelFilterBehavior::Include => Self::Include,
            GmailLabelFilterBehavior::Exclude => Self::Exclude,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WatchResponse {
    pub history_id: String,
    pub expiration: String,
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct PullRequest {
    max_messages: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PullResponse {
    #[serde(default)]
    pub received_messages: Vec<ReceivedMessage>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ReceivedMessage {
    #[serde(default)]
    pub ack_id: Option<String>,
    #[serde(default)]
    pub message: Option<PubsubMessage>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PubsubMessage {
    #[serde(default)]
    pub data: Option<String>,
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct AcknowledgeRequest {
    ack_ids: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GmailNotification {
    pub history_id: String,
    #[serde(default)]
    pub email_address: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GmailHistoryTracker {
    last_history_id: Option<u64>,
}

impl GmailHistoryTracker {
    pub fn set_baseline_from_watch(
        &mut self,
        response: &WatchResponse,
    ) -> Result<(), GmailApiError> {
        self.last_history_id = Some(parse_history_id("watch", &response.history_id)?);
        Ok(())
    }

    pub fn observe_notification(
        &mut self,
        notification: &GmailNotification,
    ) -> Result<bool, GmailApiError> {
        let history_id = parse_history_id("notification", &notification.history_id)?;
        let should_trigger = self
            .last_history_id
            .map(|last| history_id > last)
            .unwrap_or(true);
        if should_trigger {
            self.last_history_id = Some(history_id);
        }
        Ok(should_trigger)
    }

    pub fn last_history_id(&self) -> Option<u64> {
        self.last_history_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PubsubBatchDecision {
    pub ack_ids: Vec<String>,
    pub should_trigger: bool,
    pub malformed_count: usize,
}

pub fn classify_pubsub_batch(
    response: &PullResponse,
    tracker: &mut GmailHistoryTracker,
) -> PubsubBatchDecision {
    let mut ack_ids = Vec::new();
    let mut should_trigger = false;
    let mut malformed_count = 0;

    for received in &response.received_messages {
        if let Some(ack_id) = &received.ack_id {
            ack_ids.push(ack_id.clone());
        }
        let Some(message) = &received.message else {
            malformed_count += 1;
            continue;
        };
        let Some(data) = &message.data else {
            malformed_count += 1;
            continue;
        };
        let Ok(notification) = decode_gmail_notification_data(data) else {
            malformed_count += 1;
            continue;
        };
        match tracker.observe_notification(&notification) {
            Ok(true) => should_trigger = true,
            Ok(false) => {}
            Err(_) => malformed_count += 1,
        }
    }

    PubsubBatchDecision {
        ack_ids,
        should_trigger,
        malformed_count,
    }
}

pub fn decode_gmail_notification_data(data: &str) -> Result<GmailNotification, GmailApiError> {
    let decoded = URL_SAFE
        .decode(data)
        .or_else(|_| URL_SAFE_NO_PAD.decode(data))
        .map_err(|_| GmailApiError::InvalidNotification)?;
    serde_json::from_slice(&decoded).map_err(|_| GmailApiError::InvalidNotification)
}

fn parse_history_id(field: &'static str, value: &str) -> Result<u64, GmailApiError> {
    value
        .parse::<u64>()
        .map_err(|_| GmailApiError::InvalidHistoryId {
            field,
            value: value.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use serde_json::json;

    fn gmail_source() -> GmailApiWatchSourceConfig {
        let config = Config::parse_str(
            r#"
[[commands]]
name = "remote-sync"
cmd = "echo sync"

[[sources]]
name = "gmail-inbox"
type = "gmail_api_watch"
on_event = "remote-sync"
gmail_token_cmd = "gmail-api-token"
pubsub_token_cmd = "google-pubsub-token"
topic_name = "projects/example-project/topics/mailwake-gmail"
subscription = "projects/example-project/subscriptions/mailwake-gmail"
label_ids = ["INBOX"]
label_filter_behavior = "include"
"#,
        )
        .expect("config should parse");
        let crate::config::SourceConfig::GmailApiWatch(source) = config.sources[0].clone() else {
            panic!("expected gmail_api_watch source");
        };
        source
    }

    #[test]
    fn watch_request_serializes_gmail_api_fields() {
        let request = WatchRequest::from_source(&gmail_source());
        let value = serde_json::to_value(request).expect("serialize request");
        assert_eq!(
            value,
            json!({
                "topicName": "projects/example-project/topics/mailwake-gmail",
                "labelIds": ["INBOX"],
                "labelFilterBehavior": "include"
            })
        );
    }

    #[test]
    fn watch_request_omits_empty_optional_fields() {
        let mut source = gmail_source();
        source.label_ids.clear();
        source.label_filter_behavior = None;
        let value =
            serde_json::to_value(WatchRequest::from_source(&source)).expect("serialize request");
        assert_eq!(
            value,
            json!({"topicName": "projects/example-project/topics/mailwake-gmail"})
        );
    }

    #[test]
    fn decodes_gmail_notification_base64url() {
        let encoded =
            URL_SAFE_NO_PAD.encode(r#"{"emailAddress":"user@example.com","historyId":"42"}"#);
        let notification = decode_gmail_notification_data(&encoded).expect("decode notification");
        assert_eq!(notification.history_id, "42");
        assert_eq!(
            notification.email_address.as_deref(),
            Some("user@example.com")
        );
    }

    #[test]
    fn pubsub_batch_coalesces_duplicate_and_out_of_order_history_ids() {
        let notification = |history_id: &str| {
            URL_SAFE_NO_PAD.encode(format!(
                r#"{{"emailAddress":"user@example.com","historyId":"{history_id}"}}"#
            ))
        };
        let response = PullResponse {
            received_messages: vec![
                ReceivedMessage {
                    ack_id: Some("ack-1".to_string()),
                    message: Some(PubsubMessage {
                        data: Some(notification("101")),
                    }),
                },
                ReceivedMessage {
                    ack_id: Some("ack-2".to_string()),
                    message: Some(PubsubMessage {
                        data: Some(notification("100")),
                    }),
                },
                ReceivedMessage {
                    ack_id: Some("ack-3".to_string()),
                    message: Some(PubsubMessage {
                        data: Some(notification("101")),
                    }),
                },
            ],
        };
        let mut tracker = GmailHistoryTracker::default();
        tracker
            .set_baseline_from_watch(&WatchResponse {
                history_id: "100".to_string(),
                expiration: "9999999999999".to_string(),
            })
            .expect("baseline should parse");
        let decision = classify_pubsub_batch(&response, &mut tracker);
        assert_eq!(decision.ack_ids, ["ack-1", "ack-2", "ack-3"]);
        assert!(decision.should_trigger);
        assert_eq!(decision.malformed_count, 0);
        assert_eq!(tracker.last_history_id(), Some(101));

        let duplicate = classify_pubsub_batch(&response, &mut tracker);
        assert!(!duplicate.should_trigger);
        assert_eq!(tracker.last_history_id(), Some(101));
    }

    #[test]
    fn malformed_pubsub_messages_are_acked_but_not_triggered() {
        let response = PullResponse {
            received_messages: vec![ReceivedMessage {
                ack_id: Some("ack-bad".to_string()),
                message: Some(PubsubMessage {
                    data: Some("not base64".to_string()),
                }),
            }],
        };
        let mut tracker = GmailHistoryTracker::default();
        let decision = classify_pubsub_batch(&response, &mut tracker);
        assert_eq!(decision.ack_ids, ["ack-bad"]);
        assert!(!decision.should_trigger);
        assert_eq!(decision.malformed_count, 1);
        assert_eq!(tracker.last_history_id(), None);
    }

    #[test]
    fn auth_and_permission_errors_are_permanent_without_secret_output() {
        let err = GmailApiError::Http {
            operation: "Gmail watch",
            status: StatusCode::FORBIDDEN,
        };
        assert!(err.is_permanent_auth_failure());
        let text = err.to_string();
        assert!(text.contains("HTTP 403"));
        assert!(!text.contains("secret-token"));

        let transient = GmailApiError::Http {
            operation: "Pub/Sub pull",
            status: StatusCode::INTERNAL_SERVER_ERROR,
        };
        assert!(!transient.is_permanent_auth_failure());
    }
}
