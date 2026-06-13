use std::future::Future;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::process::Command;
use tokio::time::timeout;
use tracing::debug;

pub type CommandRunFuture = Pin<Box<dyn Future<Output = CommandRunResult> + Send>>;
pub type CommandRunResult = Result<CommandOutcome, CommandError>;

pub trait CommandExecutor: Send + Sync {
    fn run(&self) -> CommandRunFuture;
}

#[derive(Clone)]
pub struct ShellCommandExecutor {
    command: Arc<str>,
    log_output: bool,
    timeout: Duration,
}

impl ShellCommandExecutor {
    pub fn new(command: impl Into<Arc<str>>, log_output: bool, timeout: Duration) -> Self {
        Self {
            command: command.into(),
            log_output,
            timeout,
        }
    }
}

impl CommandExecutor for ShellCommandExecutor {
    fn run(&self) -> CommandRunFuture {
        let command = Arc::clone(&self.command);
        let log_output = self.log_output;
        let timeout = self.timeout;
        Box::pin(async move { run_shell_command(&command, log_output, timeout).await })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandOutcome {
    pub success: bool,
    pub code: Option<i32>,
    pub signal: Option<i32>,
    pub timed_out: bool,
    pub timeout: Option<Duration>,
}

impl CommandOutcome {
    pub fn timed_out(timeout: Duration) -> Self {
        Self {
            success: false,
            code: None,
            signal: None,
            timed_out: true,
            timeout: Some(timeout),
        }
    }

    pub fn description(&self) -> String {
        if self.timed_out {
            let seconds = self.timeout.unwrap_or_default().as_secs();
            return format!("timed out after {seconds} seconds");
        }
        if let Some(code) = self.code {
            return format!("exit status {code}");
        }
        if let Some(signal) = self.signal {
            return format!("signal {signal}");
        }
        "unknown status".to_string()
    }
}

impl From<std::process::ExitStatus> for CommandOutcome {
    fn from(status: std::process::ExitStatus) -> Self {
        #[cfg(unix)]
        let signal = {
            use std::os::unix::process::ExitStatusExt;
            status.signal()
        };
        #[cfg(not(unix))]
        let signal = None;

        Self {
            success: status.success(),
            code: status.code(),
            signal,
            timed_out: false,
            timeout: None,
        }
    }
}

#[derive(Debug, Error)]
pub enum CommandError {
    #[error("command could not be started: {source}")]
    Start {
        #[source]
        source: std::io::Error,
    },
    #[error("command could not be waited on: {source}")]
    Wait {
        #[source]
        source: std::io::Error,
    },
}

pub async fn run_shell_command(
    command: &str,
    log_output: bool,
    command_timeout: Duration,
) -> CommandRunResult {
    let mut process = Command::new("sh");
    process
        .arg("-c")
        .arg(command)
        .stdin(Stdio::null())
        .kill_on_drop(true);

    if log_output {
        process.stdout(Stdio::piped()).stderr(Stdio::piped());
    } else {
        process.stdout(Stdio::null()).stderr(Stdio::null());
    }

    let child = process
        .spawn()
        .map_err(|source| CommandError::Start { source })?;
    // If the timeout fires, dropping the wait future drops the child handle;
    // kill_on_drop above kills the command child before we report timeout.
    let output = match timeout(command_timeout, child.wait_with_output()).await {
        Ok(result) => result.map_err(|source| CommandError::Wait { source })?,
        Err(_) => return Ok(CommandOutcome::timed_out(command_timeout)),
    };

    if log_output {
        debug!(
            stdout_bytes = output.stdout.len(),
            stderr_bytes = output.stderr.len(),
            "command output captured by explicit debug option"
        );
    }

    Ok(output.status.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn command_success_is_reported() {
        let outcome = run_shell_command("exit 0", false, Duration::from_secs(1))
            .await
            .expect("command should run");
        assert!(outcome.success);
        assert_eq!(outcome.code, Some(0));
        assert!(!outcome.timed_out);
    }

    #[tokio::test]
    async fn command_failure_is_reported_without_error() {
        let outcome = run_shell_command("printf 'secret'; exit 7", false, Duration::from_secs(1))
            .await
            .expect("nonzero exits are outcomes, not spawn errors");
        assert!(!outcome.success);
        assert_eq!(outcome.code, Some(7));
        assert!(!outcome.timed_out);
    }

    #[tokio::test]
    async fn command_timeout_is_reported_without_secret_output() {
        let outcome = run_shell_command(
            "printf 'super-secret'; sleep 5",
            true,
            Duration::from_millis(50),
        )
        .await
        .expect("timeout should be reported as an outcome");
        assert!(!outcome.success);
        assert!(outcome.timed_out);
        let text = outcome.description();
        assert!(text.contains("timed out"));
        assert!(!text.contains("super-secret"));
    }
}
