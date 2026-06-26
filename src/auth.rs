use crate::config::{AccountConfig, AuthMethod, Config, SecretString, SourceConfig};
use crate::process::{ShellProcessError, ShellRun, run_shell_process};
use std::env;
use std::fmt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;
use thiserror::Error;

pub const REAUTH_REQUIRED_EXIT_CODE: u8 = 78;

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
    #[error("auth helper executable {0:?} was not found")]
    HelperNotFound(String),
    #[error("auth helper could not be started: {source}")]
    HelperStart {
        #[source]
        source: std::io::Error,
    },
    #[error("auth helper failed with {status}")]
    HelperFailed { status: String },
    #[error("auth helper reported that reauthorization is required")]
    HelperReauthRequired,
    #[error("auth helper timed out after {seconds} seconds")]
    HelperTimedOut { seconds: u64 },
    #[error("auth helper wrote more than {limit} bytes to stdout")]
    HelperOutputTooLarge { limit: usize },
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
    for source in &config.sources {
        if let SourceConfig::GmailApiPoll(source) = source {
            check_helper_exists_if_practical(&source.gmail_token_cmd)?;
        }
    }
    Ok(())
}

pub async fn credentials_for(
    account: &AccountConfig,
    helper_timeout: Duration,
    helper_max_output_bytes: usize,
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
            let token = run_secret_command(cmd, helper_timeout, helper_max_output_bytes).await?;
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
            let password = run_secret_command(cmd, helper_timeout, helper_max_output_bytes).await?;
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
    helper_max_output_bytes: usize,
) -> Result<SecretString, AuthError> {
    let output = match run_shell_process(
        command,
        true,
        false,
        helper_timeout,
        Some(helper_max_output_bytes),
        None,
    )
    .await
    .map_err(AuthError::from)?
    {
        ShellRun::Completed(output) => output,
        ShellRun::TimedOut(_) => {
            return Err(AuthError::HelperTimedOut {
                seconds: helper_timeout.as_secs(),
            });
        }
        ShellRun::Cancelled(_) => {
            unreachable!("auth helpers are not run with shutdown cancellation")
        }
        ShellRun::OutputLimitExceeded(exceeded) => {
            return Err(AuthError::HelperOutputTooLarge {
                limit: exceeded.limit,
            });
        }
    };

    if !output.status.success() {
        if output.status.code() == Some(i32::from(REAUTH_REQUIRED_EXIT_CODE)) {
            return Err(AuthError::HelperReauthRequired);
        }
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

impl From<ShellProcessError> for AuthError {
    fn from(error: ShellProcessError) -> Self {
        match error {
            ShellProcessError::Start { source }
            | ShellProcessError::Wait { source }
            | ShellProcessError::Output { source } => Self::HelperStart { source },
        }
    }
}

pub fn check_helper_exists_if_practical(command: &str) -> Result<(), AuthError> {
    if contains_shell_syntax(command) {
        return Ok(());
    }
    let words = match shell_words::split(command) {
        Ok(words) => words,
        Err(_) => return Ok(()),
    };
    let Some(program) = first_program_word(&words) else {
        return Ok(());
    };
    if contains_shell_syntax(program) {
        return Ok(());
    }
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

fn contains_shell_syntax(command: &str) -> bool {
    command.chars().any(|ch| {
        matches!(
            ch,
            '\r' | '\n' | '~' | '|' | '&' | ';' | '<' | '>' | '(' | ')' | '$' | '`' | '*' | '?'
        )
    })
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
    use std::fs;
    use std::path::Path;
    use tokio::time::sleep;

    const MAX_SECRET_OUTPUT: usize = 65_536;

    #[tokio::test]
    async fn password_cmd_output_trims_trailing_newlines() {
        let secret = run_secret_command(
            "printf 'password\\n\\n'",
            Duration::from_secs(1),
            MAX_SECRET_OUTPUT,
        )
        .await
        .expect("helper should succeed");
        assert_eq!(secret.expose_secret(), "password");
    }

    #[tokio::test]
    async fn xoauth2_cmd_output_trims_trailing_newlines() {
        let secret = run_secret_command(
            "printf 'ya29.token\\r\\n'",
            Duration::from_secs(1),
            MAX_SECRET_OUTPUT,
        )
        .await
        .expect("helper should succeed");
        assert_eq!(secret.expose_secret(), "ya29.token");
    }

    #[tokio::test]
    async fn auth_helper_errors_do_not_include_output() {
        let err = run_secret_command(
            "printf 'super-secret'; exit 42",
            Duration::from_secs(1),
            MAX_SECRET_OUTPUT,
        )
        .await
        .expect_err("helper should fail");
        let text = err.to_string();
        assert!(text.contains("exit status 42"));
        assert!(!text.contains("super-secret"));
    }

    #[tokio::test]
    async fn auth_helper_reauth_exit_code_is_classified_without_output() {
        let err = run_secret_command(
            "printf 'super-secret'; exit 78",
            Duration::from_secs(1),
            MAX_SECRET_OUTPUT,
        )
        .await
        .expect_err("helper should report reauth required");
        assert!(matches!(err, AuthError::HelperReauthRequired));
        let text = err.to_string();
        assert!(text.contains("reauthorization is required"));
        assert!(!text.contains("super-secret"));
    }

    #[tokio::test]
    async fn auth_helper_timeout_does_not_include_output() {
        let err = run_secret_command(
            "printf 'super-secret'; sleep 5",
            Duration::from_millis(50),
            MAX_SECRET_OUTPUT,
        )
        .await
        .expect_err("helper should time out");
        let text = err.to_string();
        assert!(text.contains("timed out"));
        assert!(!text.contains("super-secret"));
    }

    #[tokio::test]
    async fn auth_helper_timeout_kills_child_process_tree() {
        let dir = tempfile::tempdir().expect("tempdir");
        let pid_file = dir.path().join("sleep.pid");
        let command = format!(
            "sleep 30 & printf '%s\\n' \"$!\" > {}; wait",
            shell_quote(&pid_file)
        );
        let err = run_secret_command(&command, Duration::from_millis(200), MAX_SECRET_OUTPUT)
            .await
            .expect_err("helper should time out");
        assert!(err.to_string().contains("timed out"));

        let pid = read_pid_file(&pid_file).await;
        assert_pid_exits(pid).await;
    }

    #[tokio::test]
    async fn auth_helper_output_cap_kills_child_process_tree_without_leaking_output() {
        let dir = tempfile::tempdir().expect("tempdir");
        let pid_file = dir.path().join("sleep.pid");
        let command = format!(
            "sleep 30 & printf '%s\\n' \"$!\" > {}; yes super-secret",
            shell_quote(&pid_file)
        );
        let err = run_secret_command(&command, Duration::from_secs(5), 1024)
            .await
            .expect_err("helper output should exceed cap");
        let text = err.to_string();
        assert!(text.contains("more than 1024 bytes"));
        assert!(!text.contains("super-secret"));

        let pid = read_pid_file(&pid_file).await;
        assert_pid_exits(pid).await;
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
username = "user@example.com"
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
    fn helper_path_validation_skips_complex_shell_commands() {
        for command in [
            "~/bin/gmail-oauth-token",
            "definitely-not-a-mailwake-helper | cat",
            "definitely-not-a-mailwake-helper && cat",
            "definitely-not-a-mailwake-helper > /tmp/token",
            "definitely-not-a-mailwake-helper; cat",
            "TOKEN=$(definitely-not-a-mailwake-helper) printf '%s' \"$TOKEN\"",
        ] {
            check_helper_exists_if_practical(command)
                .unwrap_or_else(|error| panic!("{command:?} should not hard-fail: {error}"));
        }
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

    fn shell_quote(path: &Path) -> String {
        let value = path.to_string_lossy();
        format!("'{}'", value.replace('\'', "'\\''"))
    }

    async fn read_pid_file(path: &Path) -> libc::pid_t {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Ok(contents) = fs::read_to_string(path)
                    && let Ok(pid) = contents.trim().parse::<libc::pid_t>()
                {
                    return pid;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("sleep pid file was not written")
    }

    async fn assert_pid_exits(pid: libc::pid_t) {
        tokio::time::timeout(Duration::from_secs(3), async move {
            while process_exists(pid) {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("timed-out auth helper left a child process running");
    }

    fn process_exists(pid: libc::pid_t) -> bool {
        let result = unsafe { libc::kill(pid, 0) };
        if result == 0 {
            return true;
        }
        std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
    }
}
