use crate::command::DEFAULT_COMMAND_OUTPUT_MAX_BYTES;
use serde::Deserialize;
use std::collections::HashSet;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use thiserror::Error;
use tracing::warn;

pub const DEFAULT_DEBOUNCE_SECONDS: u64 = 10;
pub const DEFAULT_MAX_DEBOUNCE_SECONDS: u64 = 60;
pub const DEFAULT_IMAPS_PORT: u16 = 993;
pub const DEFAULT_IDLE_REFRESH_SECONDS: u64 = 29 * 60;
pub const DEFAULT_WATCHER_STALE_SECONDS: u64 = 60 * 60;
pub const DEFAULT_AUTH_HELPER_TIMEOUT_SECONDS: u64 = 30;
pub const DEFAULT_AUTH_HELPER_MAX_OUTPUT_BYTES: usize = 65_536;
pub const DEFAULT_COMMAND_TIMEOUT_SECONDS: u64 = 300;
pub const DEFAULT_CONNECT_TIMEOUT_SECONDS: u64 = 30;
pub const DEFAULT_IMAP_OPERATION_TIMEOUT_SECONDS: u64 = 60;
pub const DEFAULT_MIN_COMMAND_INTERVAL_SECONDS: u64 = 60;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub default_debounce_seconds: Option<u64>,
    #[serde(default)]
    pub default_max_debounce_seconds: Option<u64>,
    #[serde(default)]
    pub capture_command_output: Option<bool>,
    #[serde(default)]
    pub log_command_output: Option<bool>,
    #[serde(default)]
    pub auth_helper_timeout_seconds: Option<u64>,
    #[serde(default)]
    pub auth_helper_max_output_bytes: Option<usize>,
    #[serde(default)]
    pub command_timeout_seconds: Option<u64>,
    #[serde(default)]
    pub command_output_max_bytes: Option<usize>,
    #[serde(default)]
    pub connect_timeout_seconds: Option<u64>,
    #[serde(default)]
    pub imap_operation_timeout_seconds: Option<u64>,
    #[serde(default)]
    pub min_command_interval_seconds: Option<u64>,
    #[serde(default)]
    pub watcher_stale_seconds: Option<u64>,
    #[serde(default)]
    pub idle_refresh_seconds: Option<u64>,
    #[serde(default)]
    pub accounts: Vec<AccountConfig>,
    #[serde(default)]
    pub commands: Vec<CommandConfig>,
    #[serde(default)]
    pub sources: Vec<SourceConfig>,
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

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandConfig {
    pub name: String,
    #[serde(default)]
    pub lane: Option<String>,
    pub cmd: String,
    #[serde(default)]
    pub timeout_seconds: Option<u64>,
    #[serde(default)]
    pub min_interval_seconds: Option<u64>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceConfig {
    ImapIdle(ImapIdleSourceConfig),
    FsState(FsStateSourceConfig),
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImapIdleSourceConfig {
    pub name: String,
    pub account: String,
    pub mailbox: String,
    pub on_event: String,
    #[serde(default)]
    pub debounce_seconds: Option<u64>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FsStateSourceConfig {
    pub name: String,
    pub watch_paths: Vec<PathBuf>,
    #[serde(default)]
    pub recursive: Option<bool>,
    #[serde(default)]
    pub state_cmd: Option<String>,
    pub on_change: String,
    #[serde(default)]
    pub debounce_seconds: Option<u64>,
    #[serde(default)]
    pub max_debounce_seconds: Option<u64>,
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
        if self.source_count() == 0 {
            return Err(ConfigError::Invalid(
                "at least one source is required: configure [[accounts.mailboxes]] or [[sources]]"
                    .to_string(),
            ));
        }

        validate_nonzero_seconds(
            "default_max_debounce_seconds",
            self.default_max_debounce_seconds
                .unwrap_or(DEFAULT_MAX_DEBOUNCE_SECONDS),
        )?;
        validate_nonzero_seconds(
            "auth_helper_timeout_seconds",
            self.auth_helper_timeout_seconds
                .unwrap_or(DEFAULT_AUTH_HELPER_TIMEOUT_SECONDS),
        )?;
        validate_nonzero_bytes(
            "auth_helper_max_output_bytes",
            self.auth_helper_max_output_bytes
                .unwrap_or(DEFAULT_AUTH_HELPER_MAX_OUTPUT_BYTES),
        )?;
        validate_nonzero_seconds(
            "command_timeout_seconds",
            self.command_timeout_seconds
                .unwrap_or(DEFAULT_COMMAND_TIMEOUT_SECONDS),
        )?;
        validate_nonzero_bytes(
            "command_output_max_bytes",
            self.command_output_max_bytes
                .unwrap_or(DEFAULT_COMMAND_OUTPUT_MAX_BYTES),
        )?;
        validate_nonzero_seconds(
            "connect_timeout_seconds",
            self.connect_timeout_seconds
                .unwrap_or(DEFAULT_CONNECT_TIMEOUT_SECONDS),
        )?;
        validate_nonzero_seconds(
            "imap_operation_timeout_seconds",
            self.imap_operation_timeout_seconds
                .unwrap_or(DEFAULT_IMAP_OPERATION_TIMEOUT_SECONDS),
        )?;
        if self.capture_command_output.is_some() && self.log_command_output.is_some() {
            return Err(ConfigError::Invalid(
                "use capture_command_output instead of deprecated log_command_output, not both"
                    .to_string(),
            ));
        }

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

        let command_names = self.validate_commands()?;

        let mut account_names = HashSet::new();
        let mut source_names = HashSet::new();
        for account in &self.accounts {
            validate_nonempty("account name", &account.name)?;
            validate_nonempty("account host", &account.host)?;
            validate_nonempty("account username", &account.username)?;
            validate_no_crlf("account username", &account.username)?;
            if !account_names.insert(account.name.clone()) {
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
            account.validate_auth()?;

            let mut mailbox_names = HashSet::new();
            for mailbox in &account.mailboxes {
                validate_nonempty("mailbox name", &mailbox.name)?;
                validate_no_crlf("mailbox name", &mailbox.name)?;
                validate_nonempty("mailbox on_notify", &mailbox.on_notify)?;
                let source_name = legacy_source_name(&account.name, &mailbox.name);
                if command_names.contains(&source_name) {
                    return Err(ConfigError::Invalid(format!(
                        "command name {source_name:?} collides with legacy mailbox source name"
                    )));
                }
                if !source_names.insert(source_name.clone()) {
                    return Err(ConfigError::Invalid(format!(
                        "duplicate source name {source_name:?}"
                    )));
                }
                if !mailbox_names.insert(mailbox.name.as_str()) {
                    return Err(ConfigError::Invalid(format!(
                        "account {:?} has duplicate mailbox {:?}",
                        account.name, mailbox.name
                    )));
                }
            }
        }

        for source in &self.sources {
            source.validate(&account_names, &command_names, &mut source_names, self)?;
        }

        Ok(())
    }

    fn validate_commands(&self) -> Result<HashSet<String>, ConfigError> {
        let mut command_names = HashSet::new();
        for command in &self.commands {
            validate_nonempty("command name", &command.name)?;
            validate_nonempty("command cmd", &command.cmd)?;
            if let Some(lane) = &command.lane {
                validate_nonempty("command lane", lane)?;
            }
            if let Some(timeout) = command.timeout_seconds {
                validate_nonzero_seconds("command timeout_seconds", timeout)?;
            }
            if !command_names.insert(command.name.clone()) {
                return Err(ConfigError::Invalid(format!(
                    "duplicate command name {:?}",
                    command.name
                )));
            }
        }
        Ok(command_names)
    }

    pub fn default_debounce(&self) -> Duration {
        Duration::from_secs(
            self.default_debounce_seconds
                .unwrap_or(DEFAULT_DEBOUNCE_SECONDS),
        )
    }

    pub fn default_max_debounce(&self) -> Duration {
        Duration::from_secs(
            self.default_max_debounce_seconds
                .unwrap_or(DEFAULT_MAX_DEBOUNCE_SECONDS),
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

    pub fn auth_helper_max_output_bytes(&self) -> usize {
        self.auth_helper_max_output_bytes
            .unwrap_or(DEFAULT_AUTH_HELPER_MAX_OUTPUT_BYTES)
    }

    pub fn command_timeout(&self) -> Duration {
        Duration::from_secs(
            self.command_timeout_seconds
                .unwrap_or(DEFAULT_COMMAND_TIMEOUT_SECONDS),
        )
    }

    pub fn command_output_max_bytes(&self) -> usize {
        self.command_output_max_bytes
            .unwrap_or(DEFAULT_COMMAND_OUTPUT_MAX_BYTES)
    }

    pub fn connect_timeout(&self) -> Duration {
        Duration::from_secs(
            self.connect_timeout_seconds
                .unwrap_or(DEFAULT_CONNECT_TIMEOUT_SECONDS),
        )
    }

    pub fn imap_operation_timeout(&self) -> Duration {
        Duration::from_secs(
            self.imap_operation_timeout_seconds
                .unwrap_or(DEFAULT_IMAP_OPERATION_TIMEOUT_SECONDS),
        )
    }

    pub fn min_command_interval(&self) -> Duration {
        Duration::from_secs(
            self.min_command_interval_seconds
                .unwrap_or(DEFAULT_MIN_COMMAND_INTERVAL_SECONDS),
        )
    }

    pub fn capture_command_output(&self) -> bool {
        self.capture_command_output
            .or(self.log_command_output)
            .unwrap_or(false)
    }

    pub fn mailbox_count(&self) -> usize {
        self.accounts
            .iter()
            .map(|account| account.mailboxes.len())
            .sum()
    }

    pub fn source_count(&self) -> usize {
        self.mailbox_count() + self.sources.len()
    }

    pub fn command_count(&self) -> usize {
        self.mailbox_count() + self.commands.len()
    }

    pub fn command_lane_count(&self) -> usize {
        let mut lanes = HashSet::new();
        for account in &self.accounts {
            for mailbox in &account.mailboxes {
                lanes.insert(legacy_source_name(&account.name, &mailbox.name));
            }
        }
        for command in &self.commands {
            lanes.insert(command.lane_name().to_string());
        }
        lanes.len()
    }

    pub fn warn_for_insecure_options(&self) {
        if self.default_debounce_seconds == Some(0) {
            warn!("default_debounce_seconds=0 disables global debounce by explicit configuration");
        }
        if self.min_command_interval_seconds == Some(0) {
            warn!(
                "min_command_interval_seconds=0 disables post-command cooldown by explicit configuration"
            );
        }
        if self.log_command_output.is_some() {
            warn!("log_command_output is deprecated; use capture_command_output instead");
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
        for source in &self.sources {
            match source {
                SourceConfig::ImapIdle(source) if source.debounce_seconds == Some(0) => {
                    warn!(
                        source = %source.name,
                        "debounce_seconds=0 disables debounce for this IMAP IDLE source by explicit configuration"
                    );
                }
                SourceConfig::FsState(source) => {
                    if source.debounce_seconds == Some(0) {
                        warn!(
                            source = %source.name,
                            "debounce_seconds=0 disables debounce for this fs_state source by explicit configuration"
                        );
                    }
                    if source.recursive() {
                        warn!(
                            source = %source.name,
                            "recursive filesystem watching is explicitly enabled; avoid huge trees"
                        );
                    }
                }
                SourceConfig::ImapIdle(_) => {}
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
                let password = self.password.as_ref().ok_or_else(|| {
                    ConfigError::Invalid(format!(
                        "account {:?} uses auth=password but password is missing",
                        self.name
                    ))
                })?;
                validate_no_crlf("direct password", password.expose_secret())?;
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

impl CommandConfig {
    pub fn lane_name(&self) -> &str {
        self.lane.as_deref().unwrap_or(&self.name)
    }

    pub fn timeout(&self, config: &Config) -> Duration {
        self.timeout_seconds
            .map(Duration::from_secs)
            .unwrap_or_else(|| config.command_timeout())
    }

    pub fn min_interval(&self, config: &Config) -> Duration {
        self.min_interval_seconds
            .map(Duration::from_secs)
            .unwrap_or_else(|| config.min_command_interval())
    }
}

impl SourceConfig {
    pub fn name(&self) -> &str {
        match self {
            Self::ImapIdle(source) => &source.name,
            Self::FsState(source) => &source.name,
        }
    }

    fn validate(
        &self,
        account_names: &HashSet<String>,
        command_names: &HashSet<String>,
        source_names: &mut HashSet<String>,
        config: &Config,
    ) -> Result<(), ConfigError> {
        validate_nonempty("source name", self.name())?;
        let source_name = self.name().to_string();
        if !source_names.insert(source_name.clone()) {
            return Err(ConfigError::Invalid(format!(
                "duplicate source name {source_name:?}"
            )));
        }

        match self {
            Self::ImapIdle(source) => {
                validate_nonempty("source account", &source.account)?;
                validate_nonempty("source mailbox", &source.mailbox)?;
                validate_no_crlf("source mailbox", &source.mailbox)?;
                validate_nonempty("source on_event", &source.on_event)?;
                if !account_names.contains(source.account.as_str()) {
                    return Err(ConfigError::Invalid(format!(
                        "source {:?} references unknown account {:?}",
                        source.name, source.account
                    )));
                }
                validate_command_reference("on_event", &source.on_event, command_names)?;
            }
            Self::FsState(source) => {
                validate_nonempty("source on_change", &source.on_change)?;
                validate_command_reference("on_change", &source.on_change, command_names)?;
                if source.watch_paths.is_empty() {
                    return Err(ConfigError::Invalid(format!(
                        "fs_state source {:?} must define at least one watch_paths entry",
                        source.name
                    )));
                }
                for path in &source.watch_paths {
                    if path.as_os_str().is_empty() {
                        return Err(ConfigError::Invalid(format!(
                            "fs_state source {:?} has an empty watch path",
                            source.name
                        )));
                    }
                }
                if let Some(command) = &source.state_cmd {
                    validate_nonempty("source state_cmd", command)?;
                }
                if let Some(max_debounce) = source.max_debounce_seconds {
                    validate_nonzero_seconds("source max_debounce_seconds", max_debounce)?;
                }
                let _ = source.max_debounce(config);
            }
        }
        Ok(())
    }
}

impl ImapIdleSourceConfig {
    pub fn debounce(&self, config: &Config) -> Duration {
        self.debounce_seconds
            .map(Duration::from_secs)
            .unwrap_or_else(|| config.default_debounce())
    }
}

impl FsStateSourceConfig {
    pub fn recursive(&self) -> bool {
        self.recursive.unwrap_or(false)
    }

    pub fn debounce(&self, config: &Config) -> Duration {
        self.debounce_seconds
            .map(Duration::from_secs)
            .unwrap_or_else(|| config.default_debounce())
    }

    pub fn max_debounce(&self, config: &Config) -> Duration {
        self.max_debounce_seconds
            .map(Duration::from_secs)
            .unwrap_or_else(|| config.default_max_debounce())
    }
}

fn validate_nonempty(field: &str, value: &str) -> Result<(), ConfigError> {
    if value.trim().is_empty() {
        return Err(ConfigError::Invalid(format!("{field} must not be empty")));
    }
    Ok(())
}

fn validate_command_reference(
    field: &str,
    command: &str,
    command_names: &HashSet<String>,
) -> Result<(), ConfigError> {
    if !command_names.contains(command) {
        return Err(ConfigError::Invalid(format!(
            "{field} references unknown command {command:?}"
        )));
    }
    Ok(())
}

pub fn legacy_source_name(account: &str, mailbox: &str) -> String {
    format!("{account}/{mailbox}")
}

fn validate_no_crlf(field: &str, value: &str) -> Result<(), ConfigError> {
    if value.chars().any(|ch| matches!(ch, '\r' | '\n')) {
        return Err(ConfigError::Invalid(format!(
            "{field} must not contain CR or LF characters"
        )));
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

fn validate_nonzero_bytes(field: &str, value: usize) -> Result<(), ConfigError> {
    if value == 0 {
        return Err(ConfigError::Invalid(format!(
            "{field} must be greater than 0 bytes"
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
        assert_eq!(config.source_count(), 1);
        assert_eq!(config.command_count(), 1);
        assert_eq!(config.command_lane_count(), 1);
        assert_eq!(config.accounts[0].auth, AuthMethod::Xoauth2Cmd);
        assert_eq!(config.accounts[0].port(), 993);
        assert_eq!(config.auth_helper_timeout().as_secs(), 30);
        assert_eq!(config.auth_helper_max_output_bytes(), 65_536);
        assert_eq!(config.command_timeout().as_secs(), 300);
        assert_eq!(config.command_output_max_bytes(), 1_048_576);
        assert_eq!(config.connect_timeout().as_secs(), 30);
        assert_eq!(config.imap_operation_timeout().as_secs(), 60);
        assert_eq!(config.min_command_interval().as_secs(), 60);
        assert!(!config.capture_command_output());
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
username = "user@example.com"
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
            ("auth_helper_max_output_bytes", "0"),
            ("command_timeout_seconds", "0"),
            ("command_output_max_bytes", "0"),
            ("connect_timeout_seconds", "0"),
            ("imap_operation_timeout_seconds", "0"),
            ("idle_refresh_seconds", "0"),
            ("watcher_stale_seconds", "0"),
        ] {
            let config = format!(
                r#"
{field} = {value}

[[accounts]]
name = "gmail"
host = "imap.gmail.com"
username = "user@example.com"
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
username = "user@example.com"
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
username = "user@example.com"
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
min_command_interval_seconds = 0

[[accounts]]
name = "gmail"
host = "imap.gmail.com"
username = "user@example.com"
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
        assert_eq!(config.min_command_interval().as_secs(), 0);
    }

    #[test]
    fn rejects_crlf_in_imap_command_strings() {
        for (field, bad_line) in [
            ("username", "username = \"user\\n@example.com\""),
            ("mailbox", "name = \"IN\\rBOX\""),
        ] {
            let config = match field {
                "username" => format!(
                    r#"
[[accounts]]
name = "gmail"
host = "imap.gmail.com"
{bad_line}
auth = "xoauth2_cmd"
xoauth2_cmd = "gmail-oauth-token"

[[accounts.mailboxes]]
name = "INBOX"
on_notify = "echo sync"
"#
                ),
                "mailbox" => format!(
                    r#"
[[accounts]]
name = "gmail"
host = "imap.gmail.com"
username = "user@example.com"
auth = "xoauth2_cmd"
xoauth2_cmd = "gmail-oauth-token"

[[accounts.mailboxes]]
{bad_line}
on_notify = "echo sync"
"#
                ),
                _ => unreachable!(),
            };
            let err = Config::parse_str(&config).expect_err("CR/LF should fail validation");
            assert!(err.to_string().contains("CR or LF"));
        }
    }

    #[test]
    fn rejects_direct_password_crlf_without_leaking_password() {
        let err = Config::parse_str(
            r#"
[[accounts]]
name = "local"
host = "localhost"
username = "me"
auth = "password"
password = "super-secret\nnext-line"

[[accounts.mailboxes]]
name = "INBOX"
on_notify = "echo sync"
"#,
        )
        .expect_err("direct password with CR/LF should fail");
        let text = err.to_string();
        assert!(text.contains("direct password"));
        assert!(!text.contains("super-secret"));
    }

    #[test]
    fn deprecated_log_command_output_still_parses() {
        let config = Config::parse_str(
            r#"
log_command_output = true

[[accounts]]
name = "gmail"
host = "imap.gmail.com"
username = "user@example.com"
auth = "xoauth2_cmd"
xoauth2_cmd = "gmail-oauth-token"

[[accounts.mailboxes]]
name = "INBOX"
on_notify = "echo sync"
"#,
        )
        .expect("deprecated field should remain compatible");
        assert!(config.capture_command_output());
    }

    #[test]
    fn rejects_both_command_output_field_names() {
        let err = Config::parse_str(
            r#"
capture_command_output = true
log_command_output = true

[[accounts]]
name = "gmail"
host = "imap.gmail.com"
username = "user@example.com"
auth = "xoauth2_cmd"
xoauth2_cmd = "gmail-oauth-token"

[[accounts.mailboxes]]
name = "INBOX"
on_notify = "echo sync"
"#,
        )
        .expect_err("ambiguous output capture fields should fail");
        assert!(err.to_string().contains("capture_command_output"));
    }

    #[test]
    fn parses_generic_fs_state_source_without_mail_semantics() {
        let config = Config::parse_str(
            r#"
default_debounce_seconds = 5
default_max_debounce_seconds = 60

[[commands]]
name = "local-push"
lane = "sync"
cmd = "echo push"
timeout_seconds = 30
min_interval_seconds = 0

[[sources]]
name = "local-state"
type = "fs_state"
watch_paths = ["/tmp/app-state"]
state_cmd = "cat /tmp/app-state/version"
on_change = "local-push"
"#,
        )
        .expect("fs_state-only config should parse");

        assert!(config.accounts.is_empty());
        assert_eq!(config.source_count(), 1);
        assert_eq!(config.command_count(), 1);
        assert_eq!(config.command_lane_count(), 1);
        let SourceConfig::FsState(source) = &config.sources[0] else {
            panic!("source should be fs_state");
        };
        assert!(!source.recursive());
        assert_eq!(source.debounce(&config).as_secs(), 5);
        assert_eq!(source.max_debounce(&config).as_secs(), 60);
    }

    #[test]
    fn recursive_defaults_to_false_and_must_be_explicit() {
        let config = Config::parse_str(
            r#"
[[commands]]
name = "changed"
cmd = "echo changed"

[[sources]]
name = "nonrecursive"
type = "fs_state"
watch_paths = ["/tmp/state"]
on_change = "changed"

[[sources]]
name = "recursive"
type = "fs_state"
watch_paths = ["/tmp/tree"]
recursive = true
on_change = "changed"
"#,
        )
        .expect("fs_state recursive config should parse");

        let SourceConfig::FsState(nonrecursive) = &config.sources[0] else {
            panic!("source should be fs_state");
        };
        let SourceConfig::FsState(recursive) = &config.sources[1] else {
            panic!("source should be fs_state");
        };
        assert!(!nonrecursive.recursive());
        assert!(recursive.recursive());
    }

    #[test]
    fn imap_and_fs_state_sources_can_share_a_command_lane() {
        let config = Config::parse_str(
            r#"
[[accounts]]
name = "gmail"
host = "imap.gmail.com"
username = "user@example.com"
auth = "xoauth2_cmd"
xoauth2_cmd = "gmail-oauth-token"

[[commands]]
name = "remote-sync"
lane = "gmail-sync"
cmd = "echo remote"

[[commands]]
name = "local-push"
lane = "gmail-sync"
cmd = "echo local"

[[sources]]
name = "remote-inbox"
type = "imap_idle"
account = "gmail"
mailbox = "INBOX"
on_event = "remote-sync"

[[sources]]
name = "local-state"
type = "fs_state"
watch_paths = ["/tmp/not-a-maildir"]
state_cmd = "cat /tmp/state-version"
on_change = "local-push"
"#,
        )
        .expect("shared lane config should parse");

        assert_eq!(config.source_count(), 2);
        assert_eq!(config.command_count(), 2);
        assert_eq!(config.command_lane_count(), 1);
    }

    #[test]
    fn sources_must_reference_configured_commands() {
        let err = Config::parse_str(
            r#"
[[sources]]
name = "local-state"
type = "fs_state"
watch_paths = ["/tmp/state"]
on_change = "missing"
"#,
        )
        .expect_err("missing command reference should fail");
        assert!(err.to_string().contains("unknown command"));
    }

    #[test]
    fn named_commands_must_not_collide_with_legacy_mailbox_commands() {
        let err = Config::parse_str(
            r#"
[[accounts]]
name = "gmail"
host = "imap.gmail.com"
username = "user@example.com"
auth = "xoauth2_cmd"
xoauth2_cmd = "gmail-oauth-token"

[[accounts.mailboxes]]
name = "INBOX"
on_notify = "echo legacy"

[[commands]]
name = "gmail/INBOX"
cmd = "echo named"
"#,
        )
        .expect_err("legacy synthetic command name collision should fail");
        assert!(err.to_string().contains("collides"));
    }

    #[test]
    fn no_notmuch_dependency_is_declared() {
        let manifest = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"),
        )
        .expect("read Cargo.toml");
        assert!(!manifest.to_ascii_lowercase().contains("notmuch"));
    }
}
