use crate::config::{AccountConfig, AuthMethod, Config, SecretString};
use std::env;
use std::fmt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use thiserror::Error;
use tokio::process::Command;
use tokio::time::timeout;

#[derive(Clone)]
pub enum Credentials {
    Xoauth2 { token: SecretString },
    Password { password: SecretString },
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Xoauth2 { .. } => f.write_str("Credentials::Xoauth2 { token: [REDACTED] }"),
            Self::Password { .. } => f.write_str("Credentials::Password { password: [REDACTED] }"),
        }
    }
}

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("auth helper command has unsupported syntax")]
    InvalidHelperSyntax,
    #[error("auth helper executable {0:?} was not found")]
    HelperNotFound(String),
    #[error("auth helper could not be started: {source}")]
    HelperStart {
        #[source]
        source: std::io::Error,
    },
    #[error("auth helper failed with {status}")]
    HelperFailed { status: String },
    #[error("auth helper timed out after {seconds} seconds")]
    HelperTimedOut { seconds: u64 },
    #[error("auth helper wrote non-UTF-8 output")]
    HelperOutputUtf8,
    #[error("auth helper returned an empty secret")]
    EmptySecret,
    #[error("account {account:?} is missing required auth field for {method}")]
    MissingAuthField { account: String, method: AuthMethod },
}

pub fn validate_auth_helpers(config: &Config) -> Result<(), AuthError> {
    for account in &config.accounts {
        match account.auth {
            AuthMethod::Xoauth2Cmd => {
                let cmd =
                    account
                        .xoauth2_cmd
                        .as_deref()
                        .ok_or_else(|| AuthError::MissingAuthField {
                            account: account.name.clone(),
                            method: account.auth,
                        })?;
                check_helper_exists_if_practical(cmd)?;
            }
            AuthMethod::PasswordCmd => {
                let cmd =
                    account
                        .password_cmd
                        .as_deref()
                        .ok_or_else(|| AuthError::MissingAuthField {
                            account: account.name.clone(),
                            method: account.auth,
                        })?;
                check_helper_exists_if_practical(cmd)?;
            }
            AuthMethod::Password => {}
        }
    }
    Ok(())
}

pub async fn credentials_for(
    account: &AccountConfig,
    helper_timeout: Duration,
) -> Result<Credentials, AuthError> {
    match account.auth {
        AuthMethod::Xoauth2Cmd => {
            let cmd =
                account
                    .xoauth2_cmd
                    .as_deref()
                    .ok_or_else(|| AuthError::MissingAuthField {
                        account: account.name.clone(),
                        method: account.auth,
                    })?;
            let token = run_secret_command(cmd, helper_timeout).await?;
            Ok(Credentials::Xoauth2 { token })
        }
        AuthMethod::PasswordCmd => {
            let cmd =
                account
                    .password_cmd
                    .as_deref()
                    .ok_or_else(|| AuthError::MissingAuthField {
                        account: account.name.clone(),
                        method: account.auth,
                    })?;
            let password = run_secret_command(cmd, helper_timeout).await?;
            Ok(Credentials::Password { password })
        }
        AuthMethod::Password => {
            let password = account
                .password
                .clone()
                .ok_or_else(|| AuthError::MissingAuthField {
                    account: account.name.clone(),
                    method: account.auth,
                })?;
            Ok(Credentials::Password { password })
        }
    }
}

pub async fn run_secret_command(
    command: &str,
    helper_timeout: Duration,
) -> Result<SecretString, AuthError> {
    let mut process = Command::new("sh");
    process
        .arg("-c")
        .arg(command)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);

    let child = process
        .spawn()
        .map_err(|source| AuthError::HelperStart { source })?;
    // If the timeout fires, dropping the wait future drops the child handle;
    // kill_on_drop above kills the helper without logging its stdout.
    let output = match timeout(helper_timeout, child.wait_with_output()).await {
        Ok(result) => result.map_err(|source| AuthError::HelperStart { source })?,
        Err(_) => {
            return Err(AuthError::HelperTimedOut {
                seconds: helper_timeout.as_secs(),
            });
        }
    };

    if !output.status.success() {
        return Err(AuthError::HelperFailed {
            status: describe_status(output.status),
        });
    }

    let stdout = String::from_utf8(output.stdout).map_err(|_| AuthError::HelperOutputUtf8)?;
    let secret = trim_trailing_crlf(&stdout).to_string();
    if secret.is_empty() {
        return Err(AuthError::EmptySecret);
    }
    Ok(SecretString::new(secret))
}

pub fn check_helper_exists_if_practical(command: &str) -> Result<(), AuthError> {
    let words = match shell_words::split(command) {
        Ok(words) => words,
        Err(_) => return Ok(()),
    };
    let Some(program) = first_program_word(&words) else {
        return Ok(());
    };
    if is_shell_builtin(program) {
        return Ok(());
    }
    if program.contains('/') {
        let path = Path::new(program);
        if is_executable(path) {
            return Ok(());
        }
        return Err(AuthError::HelperNotFound(program.to_string()));
    }
    if find_on_path(program).is_some() {
        return Ok(());
    }
    Err(AuthError::HelperNotFound(program.to_string()))
}

fn first_program_word(words: &[String]) -> Option<&str> {
    words
        .iter()
        .map(String::as_str)
        .find(|word| !is_env_assignment(word))
}

fn is_env_assignment(word: &str) -> bool {
    let Some((name, _value)) = word.split_once('=') else {
        return false;
    };
    let mut chars = name.chars();
    match chars.next() {
        Some(first) if first == '_' || first.is_ascii_alphabetic() => {}
        _ => return false,
    }
    chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn is_shell_builtin(program: &str) -> bool {
    matches!(
        program,
        ":" | "."
            | "["
            | "alias"
            | "bg"
            | "break"
            | "cd"
            | "command"
            | "continue"
            | "eval"
            | "exec"
            | "exit"
            | "export"
            | "false"
            | "fg"
            | "getopts"
            | "hash"
            | "jobs"
            | "pwd"
            | "read"
            | "readonly"
            | "return"
            | "set"
            | "shift"
            | "test"
            | "times"
            | "trap"
            | "true"
            | "type"
            | "ulimit"
            | "umask"
            | "unalias"
            | "unset"
    )
}

fn find_on_path(program: &str) -> Option<PathBuf> {
    let path = env::var_os("PATH")?;
    env::split_paths(&path)
        .map(|dir| dir.join(program))
        .find(|candidate| is_executable(candidate))
}

fn is_executable(path: &Path) -> bool {
    let Ok(metadata) = path.metadata() else {
        return false;
    };
    metadata.is_file() && metadata.permissions().mode() & 0o111 != 0
}

fn trim_trailing_crlf(value: &str) -> &str {
    value.trim_end_matches(['\r', '\n'])
}

fn describe_status(status: std::process::ExitStatus) -> String {
    if let Some(code) = status.code() {
        return format!("exit status {code}");
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            return format!("signal {signal}");
        }
    }
    "unknown status".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn password_cmd_output_trims_trailing_newlines() {
        let secret = run_secret_command("printf 'password\\n\\n'", Duration::from_secs(1))
            .await
            .expect("helper should succeed");
        assert_eq!(secret.expose_secret(), "password");
    }

    #[tokio::test]
    async fn xoauth2_cmd_output_trims_trailing_newlines() {
        let secret = run_secret_command("printf 'ya29.token\\r\\n'", Duration::from_secs(1))
            .await
            .expect("helper should succeed");
        assert_eq!(secret.expose_secret(), "ya29.token");
    }

    #[tokio::test]
    async fn auth_helper_errors_do_not_include_output() {
        let err = run_secret_command("printf 'super-secret'; exit 42", Duration::from_secs(1))
            .await
            .expect_err("helper should fail");
        let text = err.to_string();
        assert!(text.contains("exit status 42"));
        assert!(!text.contains("super-secret"));
    }

    #[tokio::test]
    async fn auth_helper_timeout_does_not_include_output() {
        let err = run_secret_command("printf 'super-secret'; sleep 5", Duration::from_millis(50))
            .await
            .expect_err("helper should time out");
        let text = err.to_string();
        assert!(text.contains("timed out"));
        assert!(!text.contains("super-secret"));
    }

    #[test]
    fn helper_path_validation_does_not_execute_helper() {
        let dir = tempfile::tempdir().expect("tempdir");
        let touched = dir.path().join("should-not-exist");
        let config = Config::parse_str(&format!(
            r#"
[[accounts]]
name = "gmail"
host = "imap.gmail.com"
username = "me@example.com"
auth = "xoauth2_cmd"
xoauth2_cmd = "sh -c 'touch {}'"

[[accounts.mailboxes]]
name = "INBOX"
on_notify = "echo sync"
"#,
            touched.display()
        ))
        .expect("config should parse");
        validate_auth_helpers(&config).expect("shell helper path should validate");
        assert!(!touched.exists());
    }

    #[test]
    fn helper_path_check_finds_missing_program() {
        let err = check_helper_exists_if_practical("definitely-not-a-mailwake-helper")
            .expect_err("missing helper should fail");
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn credential_debug_redacts_secret() {
        let creds = Credentials::Xoauth2 {
            token: SecretString::new("ya29.secret"),
        };
        let debug = format!("{creds:?}");
        assert!(!debug.contains("ya29.secret"));
        assert!(debug.contains("[REDACTED]"));
    }
}
