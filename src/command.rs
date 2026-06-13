use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::watch;
use tracing::debug;

use crate::process::{ShellProcessError, ShellRun, run_shell_process};

pub type CommandRunFuture = Pin<Box<dyn Future<Output = CommandRunResult> + Send>>;
pub type CommandRunResult = Result<CommandOutcome, CommandError>;

pub trait CommandExecutor: Send + Sync {
    fn run(&self, shutdown: watch::Receiver<bool>) -> CommandRunFuture;
}

#[derive(Clone)]
pub struct ShellCommandExecutor {
    command: Arc<str>,
    capture_output: bool,
    timeout: Duration,
}

impl ShellCommandExecutor {
    pub fn new(command: impl Into<Arc<str>>, capture_output: bool, timeout: Duration) -> Self {
        Self {
            command: command.into(),
            capture_output,
            timeout,
        }
    }
}

impl CommandExecutor for ShellCommandExecutor {
    fn run(&self, shutdown: watch::Receiver<bool>) -> CommandRunFuture {
        let command = Arc::clone(&self.command);
        let capture_output = self.capture_output;
        let timeout = self.timeout;
        Box::pin(async move {
            run_shell_command_with_shutdown(&command, capture_output, timeout, shutdown).await
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandOutcome {
    pub success: bool,
    pub code: Option<i32>,
    pub signal: Option<i32>,
    pub timed_out: bool,
    pub cancelled: bool,
    pub timeout: Option<Duration>,
}

impl CommandOutcome {
    pub fn timed_out(timeout: Duration) -> Self {
        Self {
            success: false,
            code: None,
            signal: None,
            timed_out: true,
            cancelled: false,
            timeout: Some(timeout),
        }
    }

    pub fn cancelled() -> Self {
        Self {
            success: false,
            code: None,
            signal: None,
            timed_out: false,
            cancelled: true,
            timeout: None,
        }
    }

    pub fn description(&self) -> String {
        if self.timed_out {
            let seconds = self.timeout.unwrap_or_default().as_secs();
            return format!("timed out after {seconds} seconds");
        }
        if self.cancelled {
            return "cancelled by shutdown".to_string();
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
            cancelled: false,
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
    #[error("command output could not be collected: {source}")]
    Output {
        #[source]
        source: std::io::Error,
    },
}

pub async fn run_shell_command(
    command: &str,
    capture_output: bool,
    command_timeout: Duration,
) -> CommandRunResult {
    run_shell_command_inner(command, capture_output, command_timeout, None).await
}

pub async fn run_shell_command_with_shutdown(
    command: &str,
    capture_output: bool,
    command_timeout: Duration,
    shutdown: watch::Receiver<bool>,
) -> CommandRunResult {
    run_shell_command_inner(command, capture_output, command_timeout, Some(shutdown)).await
}

async fn run_shell_command_inner(
    command: &str,
    capture_output: bool,
    command_timeout: Duration,
    shutdown: Option<watch::Receiver<bool>>,
) -> CommandRunResult {
    match run_shell_process(
        command,
        capture_output,
        capture_output,
        command_timeout,
        None,
        shutdown,
    )
    .await
    .map_err(CommandError::from)?
    {
        ShellRun::Completed(output) => {
            if capture_output {
                debug!(
                    stdout_bytes = output.stdout.len(),
                    stderr_bytes = output.stderr.len(),
                    "command output captured by explicit debug option"
                );
            }
            Ok(output.status.into())
        }
        ShellRun::TimedOut => Ok(CommandOutcome::timed_out(command_timeout)),
        ShellRun::Cancelled => Ok(CommandOutcome::cancelled()),
        ShellRun::OutputLimitExceeded(_) => {
            unreachable!("notification commands have no output cap")
        }
    }
}

impl From<ShellProcessError> for CommandError {
    fn from(error: ShellProcessError) -> Self {
        match error {
            ShellProcessError::Start { source } => Self::Start { source },
            ShellProcessError::Wait { source } => Self::Wait { source },
            ShellProcessError::Output { source } => Self::Output { source },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;
    use tokio::time::sleep;

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

    #[tokio::test]
    async fn command_timeout_kills_child_process_tree() {
        let dir = tempfile::tempdir().expect("tempdir");
        let pid_file = dir.path().join("sleep.pid");
        let command = format!(
            "sleep 30 & printf '%s\\n' \"$!\" > {}; wait",
            shell_quote(&pid_file)
        );
        let outcome = run_shell_command(&command, false, Duration::from_millis(200))
            .await
            .expect("timeout should be reported as an outcome");
        assert!(outcome.timed_out);

        let pid = read_pid_file(&pid_file).await;
        assert_pid_exits(pid).await;
    }

    #[tokio::test]
    async fn shutdown_cancels_running_command_process_tree() {
        let dir = tempfile::tempdir().expect("tempdir");
        let pid_file = dir.path().join("sleep.pid");
        let command = format!(
            "sleep 30 & printf '%s\\n' \"$!\" > {}; wait",
            shell_quote(&pid_file)
        );
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            run_shell_command_with_shutdown(&command, false, Duration::from_secs(30), shutdown_rx)
                .await
        });

        let pid = read_pid_file(&pid_file).await;
        shutdown_tx.send(true).unwrap();
        let outcome = task
            .await
            .expect("command task should not panic")
            .expect("shutdown should be reported as an outcome");
        assert!(outcome.cancelled);
        assert_pid_exits(pid).await;
    }

    fn shell_quote(path: &Path) -> String {
        let value = path.to_string_lossy();
        format!("'{}'", value.replace('\'', "'\\''"))
    }

    async fn read_pid_file(path: &Path) -> libc::pid_t {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Ok(contents) = fs::read_to_string(path) {
                    if let Ok(pid) = contents.trim().parse::<libc::pid_t>() {
                        return pid;
                    }
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
        .expect("timed-out command left a child process running");
    }

    fn process_exists(pid: libc::pid_t) -> bool {
        let result = unsafe { libc::kill(pid, 0) };
        if result == 0 {
            return true;
        }
        std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
    }
}
