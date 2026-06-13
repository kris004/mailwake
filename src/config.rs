use serde::Deserialize;
use std::collections::HashSet;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use thiserror::Error;
use tracing::warn;

pub const DEFAULT_DEBOUNCE_SECONDS: u64 = 10;
pub const DEFAULT_IMAPS_PORT: u16 = 993;
pub const DEFAULT_IDLE_REFRESH_SECONDS: u64 = 29 * 60;
pub const DEFAULT_WATCHER_STALE_SECONDS: u64 = 60 * 60;
pub const DEFAULT_AUTH_HELPER_TIMEOUT_SECONDS: u64 = 30;
pub const DEFAULT_COMMAND_TIMEOUT_SECONDS: u64 = 300;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub default_debounce_seconds: Option<u64>,
    #[serde(default)]
    pub log_command_output: bool,
    #[serde(default)]
    pub auth_helper_timeout_seconds: Option<u64>,
    #[serde(default)]
    pub command_timeout_seconds: Option<u64>,
    #[serde(default)]
    pub watcher_stale_seconds: Option<u64>,
    #[serde(default)]
    pub idle_refresh_seconds: Option<u64>,
    #[serde(default)]
    pub accounts: Vec<AccountConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountConfig {
    pub name: String,
    pub host: String,
    #[serde(default)]
    pub port: Option<u16>,
    pub username: String,
    pub auth: AuthMethod,
    #[serde(default)]
    pub xoauth2_cmd: Option<String>,
    #[serde(default)]
    pub password_cmd: Option<String>,
    #[serde(default)]
    pub password: Option<SecretString>,
    #[serde(default)]
    pub insecure_plaintext: bool,
    #[serde(default)]
    pub danger_accept_invalid_certs: bool,
    #[serde(default)]
    pub mailboxes: Vec<MailboxConfig>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AuthMethod {
    Xoauth2Cmd,
    PasswordCmd,
    Password,
}

impl fmt::Display for AuthMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Xoauth2Cmd => f.write_str("xoauth2_cmd"),
            Self::PasswordCmd => f.write_str("password_cmd"),
            Self::Password => f.write_str("password"),
        }
    }
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(transparent)]
pub struct SecretString(String);

impl SecretString {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MailboxConfig {
    pub name: String,
    pub on_notify: String,
    #[serde(default)]
    pub debounce_seconds: Option<u64>,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse TOML config {path}{location}")]
    Parse {
        path: PathBuf,
        location: ParseLocation,
        #[source]
        source: Box<toml::de::Error>,
    },
    #[error("invalid config: {0}")]
    Invalid(String),
}

#[derive(Debug)]
pub struct ParseLocation(Option<(usize, usize)>);

impl fmt::Display for ParseLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some((line, column)) => write!(f, " at line {line}, column {column}"),
            None => Ok(()),
        }
    }
}

fn parse_toml(path: PathBuf, contents: &str) -> Result<Config, ConfigError> {
    toml::from_str(contents).map_err(|source| {
        let location = source.span().map(|span| line_column(contents, span.start));
        ConfigError::Parse {
            path,
            location: ParseLocation(location),
            source: Box::new(source),
        }
    })
}

fn line_column(contents: &str, byte_index: usize) -> (usize, usize) {
    let mut line = 1;
    let mut column = 1;
    for (index, ch) in contents.char_indices() {
        if index >= byte_index {
            break;
        }
        if ch == '\n' {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }
    (line, column)
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let contents = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let config: Self = parse_toml(path.to_path_buf(), &contents)?;
        config.validate()?;
        Ok(config)
    }

    pub fn parse_str(contents: &str) -> Result<Self, ConfigError> {
        let config: Self = parse_toml(PathBuf::from("<inline>"), contents)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.accounts.is_empty() {
            return Err(ConfigError::Invalid(
                "at least one [[accounts]] entry is required".to_string(),
            ));
        }

        validate_nonzero_seconds(
            "auth_helper_timeout_seconds",
            self.auth_helper_timeout_seconds
                .unwrap_or(DEFAULT_AUTH_HELPER_TIMEOUT_SECONDS),
        )?;
        validate_nonzero_seconds(
            "command_timeout_seconds",
            self.command_timeout_seconds
                .unwrap_or(DEFAULT_COMMAND_TIMEOUT_SECONDS),
        )?;

        let idle_refresh_seconds = self
            .idle_refresh_seconds
            .unwrap_or(DEFAULT_IDLE_REFRESH_SECONDS);
        validate_nonzero_seconds("idle_refresh_seconds", idle_refresh_seconds)?;
        if idle_refresh_seconds < 60 {
            return Err(ConfigError::Invalid(format!(
                "idle_refresh_seconds must be at least 60 seconds, got {idle_refresh_seconds}"
            )));
        }

        let watcher_stale_seconds = self
            .watcher_stale_seconds
            .unwrap_or(DEFAULT_WATCHER_STALE_SECONDS);
        validate_nonzero_seconds("watcher_stale_seconds", watcher_stale_seconds)?;
        let minimum_watcher_stale = idle_refresh_seconds.saturating_mul(2);
        if watcher_stale_seconds < minimum_watcher_stale {
            return Err(ConfigError::Invalid(format!(
                "watcher_stale_seconds must be at least 2x idle_refresh_seconds ({minimum_watcher_stale} seconds), got {watcher_stale_seconds}"
            )));
        }

        let mut account_names = HashSet::new();
        for account in &self.accounts {
            validate_nonempty("account name", &account.name)?;
            validate_nonempty("account host", &account.host)?;
            validate_nonempty("account username", &account.username)?;
            if !account_names.insert(account.name.as_str()) {
                return Err(ConfigError::Invalid(format!(
                    "duplicate account name {:?}",
                    account.name
                )));
            }
            if account.port() == 0 {
                return Err(ConfigError::Invalid(format!(
                    "account {:?} has invalid port 0",
                    account.name
                )));
            }
            if account.mailboxes.is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "account {:?} must define at least one [[accounts.mailboxes]] entry",
                    account.name
                )));
            }
            account.validate_auth()?;

            let mut mailbox_names = HashSet::new();
            for mailbox in &account.mailboxes {
                validate_nonempty("mailbox name", &mailbox.name)?;
                validate_nonempty("mailbox on_notify", &mailbox.on_notify)?;
                if !mailbox_names.insert(mailbox.name.as_str()) {
                    return Err(ConfigError::Invalid(format!(
                        "account {:?} has duplicate mailbox {:?}",
                        account.name, mailbox.name
                    )));
                }
            }
        }

        Ok(())
    }

    pub fn default_debounce(&self) -> Duration {
        Duration::from_secs(
            self.default_debounce_seconds
                .unwrap_or(DEFAULT_DEBOUNCE_SECONDS),
        )
    }

    pub fn idle_refresh(&self) -> Duration {
        Duration::from_secs(
            self.idle_refresh_seconds
                .unwrap_or(DEFAULT_IDLE_REFRESH_SECONDS),
        )
    }

    pub fn watcher_stale(&self) -> Duration {
        Duration::from_secs(
            self.watcher_stale_seconds
                .unwrap_or(DEFAULT_WATCHER_STALE_SECONDS),
        )
    }

    pub fn auth_helper_timeout(&self) -> Duration {
        Duration::from_secs(
            self.auth_helper_timeout_seconds
                .unwrap_or(DEFAULT_AUTH_HELPER_TIMEOUT_SECONDS),
        )
    }

    pub fn command_timeout(&self) -> Duration {
        Duration::from_secs(
            self.command_timeout_seconds
                .unwrap_or(DEFAULT_COMMAND_TIMEOUT_SECONDS),
        )
    }

    pub fn mailbox_count(&self) -> usize {
        self.accounts
            .iter()
            .map(|account| account.mailboxes.len())
            .sum()
    }

    pub fn warn_for_insecure_options(&self) {
        if self.default_debounce_seconds == Some(0) {
            warn!("default_debounce_seconds=0 disables global debounce by explicit configuration");
        }
        for account in &self.accounts {
            if account.auth == AuthMethod::Password {
                warn!(
                    account = %account.name,
                    "direct password auth is insecure and intended only for local testing"
                );
            }
            if account.insecure_plaintext {
                warn!(
                    account = %account.name,
                    "plaintext IMAP is explicitly enabled; credentials and mail metadata may be exposed"
                );
            }
            if account.danger_accept_invalid_certs {
                warn!(
                    account = %account.name,
                    "TLS certificate verification is disabled by explicit configuration"
                );
            }
            for mailbox in &account.mailboxes {
                if mailbox.debounce_seconds == Some(0) {
                    warn!(
                        account = %account.name,
                        mailbox = %mailbox.name,
                        "debounce_seconds=0 disables debounce for this mailbox by explicit configuration"
                    );
                }
            }
        }
    }
}

impl AccountConfig {
    pub fn port(&self) -> u16 {
        self.port.unwrap_or(DEFAULT_IMAPS_PORT)
    }

    fn validate_auth(&self) -> Result<(), ConfigError> {
        match self.auth {
            AuthMethod::Xoauth2Cmd => {
                require_cmd(&self.xoauth2_cmd, "xoauth2_cmd", &self.name, self.auth)?;
                forbid_field(&self.password_cmd, "password_cmd", &self.name, self.auth)?;
                forbid_field(&self.password, "password", &self.name, self.auth)?;
            }
            AuthMethod::PasswordCmd => {
                require_cmd(&self.password_cmd, "password_cmd", &self.name, self.auth)?;
                forbid_field(&self.xoauth2_cmd, "xoauth2_cmd", &self.name, self.auth)?;
                forbid_field(&self.password, "password", &self.name, self.auth)?;
            }
            AuthMethod::Password => {
                if self.password.is_none() {
                    return Err(ConfigError::Invalid(format!(
                        "account {:?} uses auth=password but password is missing",
                        self.name
                    )));
                }
                forbid_field(&self.xoauth2_cmd, "xoauth2_cmd", &self.name, self.auth)?;
                forbid_field(&self.password_cmd, "password_cmd", &self.name, self.auth)?;
            }
        }
        Ok(())
    }
}

impl MailboxConfig {
    pub fn debounce(&self, config: &Config) -> Duration {
        self.debounce_seconds
            .map(Duration::from_secs)
            .unwrap_or_else(|| config.default_debounce())
    }
}

fn validate_nonempty(field: &str, value: &str) -> Result<(), ConfigError> {
    if value.trim().is_empty() {
        return Err(ConfigError::Invalid(format!("{field} must not be empty")));
    }
    Ok(())
}

fn validate_nonzero_seconds(field: &str, value: u64) -> Result<(), ConfigError> {
    if value == 0 {
        return Err(ConfigError::Invalid(format!(
            "{field} must be greater than 0 seconds"
        )));
    }
    Ok(())
}

fn require_cmd(
    value: &Option<String>,
    field: &str,
    account: &str,
    auth: AuthMethod,
) -> Result<(), ConfigError> {
    match value {
        Some(value) if !value.trim().is_empty() => Ok(()),
        _ => Err(ConfigError::Invalid(format!(
            "account {account:?} uses auth={auth} but {field} is missing"
        ))),
    }
}

fn forbid_field<T>(
    value: &Option<T>,
    field: &str,
    account: &str,
    auth: AuthMethod,
) -> Result<(), ConfigError> {
    if value.is_some() {
        return Err(ConfigError::Invalid(format!(
            "account {account:?} uses auth={auth}; remove unrelated field {field}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_XOAUTH2: &str = r#"
default_debounce_seconds = 10

[[accounts]]
name = "gmail"
host = "imap.gmail.com"
port = 993
username = "user@example.com"
auth = "xoauth2_cmd"
xoauth2_cmd = "gmail-oauth-token"

[[accounts.mailboxes]]
name = "INBOX"
on_notify = "echo sync"
debounce_seconds = 10
"#;

    #[test]
    fn parses_valid_config() {
        let config = Config::parse_str(VALID_XOAUTH2).expect("config should parse");
        assert_eq!(config.accounts.len(), 1);
        assert_eq!(config.mailbox_count(), 1);
        assert_eq!(config.accounts[0].auth, AuthMethod::Xoauth2Cmd);
        assert_eq!(config.accounts[0].port(), 993);
        assert_eq!(config.auth_helper_timeout().as_secs(), 30);
        assert_eq!(config.command_timeout().as_secs(), 300);
        assert_eq!(
            config.accounts[0].mailboxes[0].debounce(&config).as_secs(),
            10
        );
    }

    #[test]
    fn missing_required_fields_fail() {
        let err = Config::parse_str(
            r#"
[[accounts]]
name = "gmail"
auth = "xoauth2_cmd"
xoauth2_cmd = "gmail-oauth-token"
"#,
        )
        .expect_err("missing host and username should fail");
        assert!(err.to_string().contains("failed to parse TOML config"));
    }

    #[test]
    fn validates_auth_method_consistency() {
        let err = Config::parse_str(
            r#"
[[accounts]]
name = "gmail"
host = "imap.gmail.com"
username = "me@example.com"
auth = "xoauth2_cmd"
password_cmd = "pass show mail/gmail"

[[accounts.mailboxes]]
name = "INBOX"
on_notify = "echo sync"
"#,
        )
        .expect_err("xoauth2_cmd auth requires xoauth2_cmd");
        assert!(err.to_string().contains("xoauth2_cmd is missing"));
    }

    #[test]
    fn direct_password_debug_is_redacted() {
        let config = Config::parse_str(
            r#"
[[accounts]]
name = "local"
host = "localhost"
username = "me"
auth = "password"
password = "super-secret"

[[accounts.mailboxes]]
name = "INBOX"
on_notify = "echo sync"
"#,
        )
        .expect("direct password config should parse");
        let debug = format!("{:?}", config.accounts[0].password.as_ref().unwrap());
        assert_eq!(debug, "[REDACTED]");
        assert!(!debug.contains("super-secret"));
    }

    #[test]
    fn parse_errors_do_not_echo_config_values() {
        let err = Config::parse_str(
            r#"
[[accounts]]
name = "local"
host = "localhost"
username = "me"
auth = "password"
password = 123
"#,
        )
        .expect_err("wrong password type should fail");
        assert!(!err.to_string().contains("123"));
        assert!(!err.to_string().contains("password"));
    }

    #[test]
    fn rejects_invalid_runtime_timing_values() {
        for (field, value) in [
            ("auth_helper_timeout_seconds", "0"),
            ("command_timeout_seconds", "0"),
            ("idle_refresh_seconds", "0"),
            ("watcher_stale_seconds", "0"),
        ] {
            let config = format!(
                r#"
{field} = {value}

[[accounts]]
name = "gmail"
host = "imap.gmail.com"
username = "me@example.com"
auth = "xoauth2_cmd"
xoauth2_cmd = "gmail-oauth-token"

[[accounts.mailboxes]]
name = "INBOX"
on_notify = "echo sync"
"#
            );
            let err = Config::parse_str(&config).expect_err("zero timing value should fail");
            assert!(err.to_string().contains(field));
        }
    }

    #[test]
    fn rejects_too_small_idle_refresh_and_watcher_stale() {
        let idle_err = Config::parse_str(
            r#"
idle_refresh_seconds = 59
watcher_stale_seconds = 120

[[accounts]]
name = "gmail"
host = "imap.gmail.com"
username = "me@example.com"
auth = "xoauth2_cmd"
xoauth2_cmd = "gmail-oauth-token"

[[accounts.mailboxes]]
name = "INBOX"
on_notify = "echo sync"
"#,
        )
        .expect_err("too-small idle refresh should fail");
        assert!(idle_err.to_string().contains("at least 60"));

        let stale_err = Config::parse_str(
            r#"
idle_refresh_seconds = 60
watcher_stale_seconds = 119

[[accounts]]
name = "gmail"
host = "imap.gmail.com"
username = "me@example.com"
auth = "xoauth2_cmd"
xoauth2_cmd = "gmail-oauth-token"

[[accounts.mailboxes]]
name = "INBOX"
on_notify = "echo sync"
"#,
        )
        .expect_err("watcher_stale less than 2x idle refresh should fail");
        assert!(stale_err.to_string().contains("2x idle_refresh_seconds"));
    }

    #[test]
    fn allows_explicit_zero_debounce() {
        let config = Config::parse_str(
            r#"
default_debounce_seconds = 0

[[accounts]]
name = "gmail"
host = "imap.gmail.com"
username = "me@example.com"
auth = "xoauth2_cmd"
xoauth2_cmd = "gmail-oauth-token"

[[accounts.mailboxes]]
name = "INBOX"
on_notify = "echo sync"
debounce_seconds = 0
"#,
        )
        .expect("zero debounce is allowed when explicitly configured");
        assert_eq!(config.default_debounce().as_secs(), 0);
        assert_eq!(
            config.accounts[0].mailboxes[0].debounce(&config).as_secs(),
            0
        );
    }
}
