use std::future::Future;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;
use thiserror::Error;
use tokio::process::Command;
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
}

impl ShellCommandExecutor {
    pub fn new(command: impl Into<Arc<str>>, log_output: bool) -> Self {
        Self {
            command: command.into(),
            log_output,
        }
    }
}

impl CommandExecutor for ShellCommandExecutor {
    fn run(&self) -> CommandRunFuture {
        let command = Arc::clone(&self.command);
        let log_output = self.log_output;
        Box::pin(async move { run_shell_command(&command, log_output).await })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandOutcome {
    pub success: bool,
    pub code: Option<i32>,
    pub signal: Option<i32>,
}

impl CommandOutcome {
    pub fn description(&self) -> String {
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

pub async fn run_shell_command(command: &str, log_output: bool) -> CommandRunResult {
    let mut process = Command::new("sh");
    process.arg("-c").arg(command).stdin(Stdio::null());

    if log_output {
        process.stdout(Stdio::piped()).stderr(Stdio::piped());
        let output = process
            .output()
            .await
            .map_err(|source| CommandError::Start { source })?;
        debug!(
            stdout_bytes = output.stdout.len(),
            stderr_bytes = output.stderr.len(),
            "command output captured by explicit debug option"
        );
        Ok(output.status.into())
    } else {
        process.stdout(Stdio::null()).stderr(Stdio::null());
        let status = process
            .status()
            .await
            .map_err(|source| CommandError::Wait { source })?;
        Ok(status.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn command_success_is_reported() {
        let outcome = run_shell_command("exit 0", false)
            .await
            .expect("command should run");
        assert!(outcome.success);
        assert_eq!(outcome.code, Some(0));
    }

    #[tokio::test]
    async fn command_failure_is_reported_without_error() {
        let outcome = run_shell_command("printf 'secret'; exit 7", false)
            .await
            .expect("nonzero exits are outcomes, not spawn errors");
        assert!(!outcome.success);
        assert_eq!(outcome.code, Some(7));
    }
}
