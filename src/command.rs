use serde::Deserialize;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use crate::process::{
    OutputLimitExceeded, ShellCapturedOutput, ShellProcessError, ShellRun, ShellStreamMode,
    run_shell_process, run_shell_process_with_streams,
};

pub const DEFAULT_COMMAND_OUTPUT_MAX_BYTES: usize = 1_048_576;
pub const DEFAULT_COMMAND_OUTPUT_TAIL_LINES: usize = 100;

pub type CommandRunFuture = Pin<Box<dyn Future<Output = CommandRunResult> + Send>>;
pub type CommandRunResult = Result<CommandOutcome, CommandError>;

pub trait CommandExecutor: Send + Sync {
    fn run(&self, shutdown: watch::Receiver<bool>) -> CommandRunFuture;
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CommandOutputMode {
    Silent,
    FailureTail,
    Tail,
    Debug,
    Journal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommandOutputPolicy {
    pub mode: CommandOutputMode,
    pub max_bytes: usize,
    pub tail_lines: usize,
}

impl Default for CommandOutputPolicy {
    fn default() -> Self {
        Self {
            mode: CommandOutputMode::FailureTail,
            max_bytes: DEFAULT_COMMAND_OUTPUT_MAX_BYTES,
            tail_lines: DEFAULT_COMMAND_OUTPUT_TAIL_LINES,
        }
    }
}

impl CommandOutputPolicy {
    pub fn silent() -> Self {
        Self {
            mode: CommandOutputMode::Silent,
            ..Self::default()
        }
    }

    fn captures_output(&self) -> bool {
        matches!(
            self.mode,
            CommandOutputMode::FailureTail | CommandOutputMode::Tail | CommandOutputMode::Debug
        )
    }

    fn streams_to_journal(&self) -> bool {
        self.mode == CommandOutputMode::Journal
    }
}

#[derive(Clone)]
pub struct ShellCommandExecutor {
    name: Arc<str>,
    command: Arc<str>,
    timeout: Duration,
    output_policy: CommandOutputPolicy,
}

impl ShellCommandExecutor {
    pub fn new(
        name: impl Into<Arc<str>>,
        command: impl Into<Arc<str>>,
        timeout: Duration,
        output_policy: CommandOutputPolicy,
    ) -> Self {
        Self {
            name: name.into(),
            command: command.into(),
            timeout,
            output_policy,
        }
    }
}

impl CommandExecutor for ShellCommandExecutor {
    fn run(&self, shutdown: watch::Receiver<bool>) -> CommandRunFuture {
        let name = Arc::clone(&self.name);
        let command = Arc::clone(&self.command);
        let timeout = self.timeout;
        let output_policy = self.output_policy;
        Box::pin(async move {
            run_named_shell_command_with_policy(
                &name,
                &command,
                timeout,
                output_policy,
                Some(shutdown),
            )
            .await
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
    pub output_limit_exceeded: bool,
    pub output_limit: Option<usize>,
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
            output_limit_exceeded: false,
            output_limit: None,
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
            output_limit_exceeded: false,
            output_limit: None,
        }
    }

    pub fn output_limit_exceeded(limit: usize) -> Self {
        Self {
            success: false,
            code: None,
            signal: None,
            timed_out: false,
            cancelled: false,
            timeout: None,
            output_limit_exceeded: true,
            output_limit: Some(limit),
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
        if self.output_limit_exceeded {
            let limit = self.output_limit.unwrap_or_default();
            return format!("output exceeded {limit} byte limit");
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
            output_limit_exceeded: false,
            output_limit: None,
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
    run_shell_command_with_output_limit(
        command,
        capture_output,
        command_timeout,
        DEFAULT_COMMAND_OUTPUT_MAX_BYTES,
    )
    .await
}

pub async fn run_shell_command_with_output_limit(
    command: &str,
    capture_output: bool,
    command_timeout: Duration,
    output_max_bytes: usize,
) -> CommandRunResult {
    run_shell_command_inner(
        command,
        capture_output,
        command_timeout,
        output_max_bytes,
        None,
    )
    .await
}

pub async fn run_shell_command_with_shutdown(
    command: &str,
    capture_output: bool,
    command_timeout: Duration,
    shutdown: watch::Receiver<bool>,
) -> CommandRunResult {
    run_shell_command_with_shutdown_and_output_limit(
        command,
        capture_output,
        command_timeout,
        DEFAULT_COMMAND_OUTPUT_MAX_BYTES,
        shutdown,
    )
    .await
}

pub async fn run_shell_command_with_shutdown_and_output_limit(
    command: &str,
    capture_output: bool,
    command_timeout: Duration,
    output_max_bytes: usize,
    shutdown: watch::Receiver<bool>,
) -> CommandRunResult {
    run_shell_command_inner(
        command,
        capture_output,
        command_timeout,
        output_max_bytes,
        Some(shutdown),
    )
    .await
}

pub async fn run_named_shell_command_with_policy(
    command_name: &str,
    command: &str,
    command_timeout: Duration,
    output_policy: CommandOutputPolicy,
    shutdown: Option<watch::Receiver<bool>>,
) -> CommandRunResult {
    let started = Instant::now();
    let result =
        run_shell_command_with_policy(command, command_timeout, output_policy, shutdown).await;
    let duration = started.elapsed();
    match &result {
        Ok(outcome) => log_command_completion(command_name, outcome, duration, output_policy),
        Err(error) => log_command_error(command_name, error, duration, output_policy),
    }
    result.map(|report| report.outcome)
}

async fn run_shell_command_with_policy(
    command: &str,
    command_timeout: Duration,
    output_policy: CommandOutputPolicy,
    shutdown: Option<watch::Receiver<bool>>,
) -> Result<CommandExecutionReport, CommandError> {
    let stdout_mode = if output_policy.streams_to_journal() {
        ShellStreamMode::Inherit
    } else if output_policy.captures_output() {
        ShellStreamMode::Capture
    } else {
        ShellStreamMode::Null
    };
    let stderr_mode = stdout_mode;
    let output_limit = output_policy
        .captures_output()
        .then_some(output_policy.max_bytes);

    let run = run_shell_process_with_streams(
        command,
        stdout_mode,
        stderr_mode,
        command_timeout,
        output_limit,
        shutdown,
    )
    .await
    .map_err(CommandError::from)?;

    Ok(match run {
        ShellRun::Completed(output) => {
            let outcome = output.status.into();
            CommandExecutionReport {
                outcome,
                output: CommandOutputReport::Captured(output.captured_output()),
            }
        }
        ShellRun::TimedOut(output) => CommandExecutionReport {
            outcome: CommandOutcome::timed_out(command_timeout),
            output: CommandOutputReport::Captured(output),
        },
        ShellRun::Cancelled(output) => CommandExecutionReport {
            outcome: CommandOutcome::cancelled(),
            output: CommandOutputReport::Captured(output),
        },
        ShellRun::OutputLimitExceeded(exceeded) => CommandExecutionReport {
            outcome: CommandOutcome::output_limit_exceeded(exceeded.limit),
            output: CommandOutputReport::OutputLimitExceeded(exceeded),
        },
    })
}

async fn run_shell_command_inner(
    command: &str,
    capture_output: bool,
    command_timeout: Duration,
    output_max_bytes: usize,
    shutdown: Option<watch::Receiver<bool>>,
) -> CommandRunResult {
    let output_limit = capture_output.then_some(output_max_bytes);
    match run_shell_process(
        command,
        capture_output,
        capture_output,
        command_timeout,
        output_limit,
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
                    stdout_truncated = output.stdout_truncated,
                    stderr_truncated = output.stderr_truncated,
                    "command output captured by explicit debug option"
                );
            }
            Ok(output.status.into())
        }
        ShellRun::TimedOut(_) => Ok(CommandOutcome::timed_out(command_timeout)),
        ShellRun::Cancelled(_) => Ok(CommandOutcome::cancelled()),
        ShellRun::OutputLimitExceeded(exceeded) => {
            Ok(CommandOutcome::output_limit_exceeded(exceeded.limit))
        }
    }
}

#[derive(Debug)]
struct CommandExecutionReport {
    outcome: CommandOutcome,
    output: CommandOutputReport,
}

#[derive(Debug)]
enum CommandOutputReport {
    Captured(ShellCapturedOutput),
    OutputLimitExceeded(OutputLimitExceeded),
}

impl CommandOutputReport {
    fn summary(&self, output_policy: CommandOutputPolicy) -> OutputSummary {
        match self {
            Self::Captured(output) => OutputSummary {
                stdout_bytes: output.stdout_len(),
                stderr_bytes: output.stderr_len(),
                stdout_truncated: output.stdout_truncated,
                stderr_truncated: output.stderr_truncated,
                captured: output_policy.captures_output(),
                suppressed: output_policy.mode == CommandOutputMode::Silent,
                journal: output_policy.streams_to_journal(),
            },
            Self::OutputLimitExceeded(exceeded) => OutputSummary {
                stdout_bytes: exceeded.stdout.len(),
                stderr_bytes: exceeded.stderr.len(),
                stdout_truncated: exceeded.stdout_truncated,
                stderr_truncated: exceeded.stderr_truncated,
                captured: true,
                suppressed: false,
                journal: false,
            },
        }
    }

    fn tails(&self, output_policy: CommandOutputPolicy) -> OutputTails {
        match self {
            Self::Captured(output) => OutputTails::from_captured(output, output_policy.tail_lines),
            Self::OutputLimitExceeded(exceeded) => OutputTails::from_bytes(
                &exceeded.stdout,
                &exceeded.stderr,
                output_policy.tail_lines,
            ),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
struct OutputSummary {
    stdout_bytes: usize,
    stderr_bytes: usize,
    stdout_truncated: bool,
    stderr_truncated: bool,
    captured: bool,
    suppressed: bool,
    journal: bool,
}

#[derive(Debug, Eq, PartialEq)]
struct OutputTails {
    stdout: Option<String>,
    stderr: Option<String>,
}

impl OutputTails {
    fn from_captured(output: &ShellCapturedOutput, max_lines: usize) -> Self {
        Self::from_bytes(&output.stdout, &output.stderr, max_lines)
    }

    fn from_bytes(stdout: &[u8], stderr: &[u8], max_lines: usize) -> Self {
        Self {
            stdout: tail_lines(stdout, max_lines),
            stderr: tail_lines(stderr, max_lines),
        }
    }

    fn is_empty(&self) -> bool {
        self.stdout.is_none() && self.stderr.is_none()
    }
}

fn log_command_completion(
    command_name: &str,
    outcome: &CommandExecutionReport,
    duration: Duration,
    output_policy: CommandOutputPolicy,
) {
    let summary = outcome.output.summary(output_policy);
    let status = outcome.outcome.description();
    if outcome.outcome.success {
        info!(
            command = %command_name,
            status = %status,
            duration_ms = duration.as_millis(),
            output_mode = ?output_policy.mode,
            stdout_bytes = summary.stdout_bytes,
            stderr_bytes = summary.stderr_bytes,
            stdout_truncated = summary.stdout_truncated,
            stderr_truncated = summary.stderr_truncated,
            output_captured = summary.captured,
            output_suppressed = summary.suppressed,
            output_journal = summary.journal,
            "notification command completed"
        );
    } else {
        warn!(
            command = %command_name,
            status = %status,
            duration_ms = duration.as_millis(),
            output_mode = ?output_policy.mode,
            stdout_bytes = summary.stdout_bytes,
            stderr_bytes = summary.stderr_bytes,
            stdout_truncated = summary.stdout_truncated,
            stderr_truncated = summary.stderr_truncated,
            output_captured = summary.captured,
            output_suppressed = summary.suppressed,
            output_journal = summary.journal,
            "notification command completed"
        );
    }
    log_output_tail_if_needed(
        command_name,
        &outcome.outcome,
        &outcome.output,
        output_policy,
    );
}

fn log_command_error(
    command_name: &str,
    error: &CommandError,
    duration: Duration,
    output_policy: CommandOutputPolicy,
) {
    error!(
        command = %command_name,
        %error,
        duration_ms = duration.as_millis(),
        output_mode = ?output_policy.mode,
        output_captured = false,
        output_suppressed = output_policy.mode == CommandOutputMode::Silent,
        output_journal = output_policy.mode == CommandOutputMode::Journal,
        "notification command could not run"
    );
}

fn log_output_tail_if_needed(
    command_name: &str,
    outcome: &CommandOutcome,
    output: &CommandOutputReport,
    output_policy: CommandOutputPolicy,
) {
    let plan = output_log_plan(output_policy, outcome, output);
    if plan.tails.is_empty() {
        return;
    }
    match plan.level {
        OutputLogLevel::Info => info!(
            command = %command_name,
            output_tail_lines = output_policy.tail_lines,
            stdout_tail = plan.tails.stdout.as_deref().unwrap_or(""),
            stderr_tail = plan.tails.stderr.as_deref().unwrap_or(""),
            "notification command output tail"
        ),
        OutputLogLevel::Warn => warn!(
            command = %command_name,
            output_tail_lines = output_policy.tail_lines,
            stdout_tail = plan.tails.stdout.as_deref().unwrap_or(""),
            stderr_tail = plan.tails.stderr.as_deref().unwrap_or(""),
            "notification command output tail"
        ),
        OutputLogLevel::Debug => debug!(
            command = %command_name,
            output_tail_lines = output_policy.tail_lines,
            stdout_tail = plan.tails.stdout.as_deref().unwrap_or(""),
            stderr_tail = plan.tails.stderr.as_deref().unwrap_or(""),
            "notification command output tail"
        ),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OutputLogLevel {
    Info,
    Warn,
    Debug,
}

#[derive(Debug, Eq, PartialEq)]
struct OutputLogPlan {
    level: OutputLogLevel,
    tails: OutputTails,
}

fn output_log_plan(
    output_policy: CommandOutputPolicy,
    outcome: &CommandOutcome,
    output: &CommandOutputReport,
) -> OutputLogPlan {
    let should_log_tail = match output_policy.mode {
        CommandOutputMode::Silent | CommandOutputMode::Journal => false,
        CommandOutputMode::FailureTail => !outcome.success,
        CommandOutputMode::Tail | CommandOutputMode::Debug => true,
    };
    let level = match output_policy.mode {
        CommandOutputMode::Debug => OutputLogLevel::Debug,
        CommandOutputMode::Tail if outcome.success => OutputLogLevel::Info,
        CommandOutputMode::Tail | CommandOutputMode::FailureTail => OutputLogLevel::Warn,
        CommandOutputMode::Silent | CommandOutputMode::Journal => OutputLogLevel::Debug,
    };
    OutputLogPlan {
        level,
        tails: if should_log_tail {
            output.tails(output_policy)
        } else {
            OutputTails {
                stdout: None,
                stderr: None,
            }
        },
    }
}

fn tail_lines(output: &[u8], max_lines: usize) -> Option<String> {
    if output.is_empty() || max_lines == 0 {
        return None;
    }
    let text = String::from_utf8_lossy(output);
    let mut lines = text.lines().rev().take(max_lines).collect::<Vec<_>>();
    lines.reverse();
    if lines.is_empty() {
        None
    } else {
        Some(lines.join("\n"))
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
    async fn captured_command_output_limit_is_reported_without_output() {
        let outcome = run_shell_command_with_output_limit(
            "printf 'super-secret-output'",
            true,
            Duration::from_secs(1),
            8,
        )
        .await
        .expect("output cap should be reported as an outcome");
        assert!(!outcome.success);
        assert!(outcome.output_limit_exceeded);
        let text = outcome.description();
        assert!(text.contains("output exceeded 8 byte limit"));
        assert!(!text.contains("super-secret-output"));
    }

    #[test]
    fn failure_tail_mode_logs_capped_tail_only_on_failure() {
        let policy = CommandOutputPolicy {
            mode: CommandOutputMode::FailureTail,
            max_bytes: 1024,
            tail_lines: 2,
        };
        let output = CommandOutputReport::Captured(ShellCapturedOutput {
            stdout: b"one\ntwo\nthree\n".to_vec(),
            stderr: b"err1\nerr2\nerr3\n".to_vec(),
            stdout_truncated: false,
            stderr_truncated: false,
        });
        let success = CommandOutcome::from(
            std::process::Command::new("sh")
                .arg("-c")
                .arg("exit 0")
                .status()
                .unwrap(),
        );
        assert!(output_log_plan(policy, &success, &output).tails.is_empty());

        let failure = CommandOutcome::from(
            std::process::Command::new("sh")
                .arg("-c")
                .arg("exit 1")
                .status()
                .unwrap(),
        );
        let plan = output_log_plan(policy, &failure, &output);
        assert_eq!(plan.level, OutputLogLevel::Warn);
        assert_eq!(plan.tails.stdout.as_deref(), Some("two\nthree"));
        assert_eq!(plan.tails.stderr.as_deref(), Some("err2\nerr3"));
    }

    #[test]
    fn tail_mode_logs_capped_tail_on_success() {
        let policy = CommandOutputPolicy {
            mode: CommandOutputMode::Tail,
            max_bytes: 1024,
            tail_lines: 1,
        };
        let output = CommandOutputReport::Captured(ShellCapturedOutput {
            stdout: b"first\nlast\n".to_vec(),
            stderr: Vec::new(),
            stdout_truncated: false,
            stderr_truncated: false,
        });
        let success = CommandOutcome::from(
            std::process::Command::new("sh")
                .arg("-c")
                .arg("exit 0")
                .status()
                .unwrap(),
        );
        let plan = output_log_plan(policy, &success, &output);
        assert_eq!(plan.level, OutputLogLevel::Info);
        assert_eq!(plan.tails.stdout.as_deref(), Some("last"));
        assert!(plan.tails.stderr.is_none());
    }

    #[test]
    fn silent_mode_does_not_log_output() {
        let policy = CommandOutputPolicy {
            mode: CommandOutputMode::Silent,
            max_bytes: 1024,
            tail_lines: 100,
        };
        let output = CommandOutputReport::Captured(ShellCapturedOutput {
            stdout: b"visible".to_vec(),
            stderr: b"error".to_vec(),
            stdout_truncated: false,
            stderr_truncated: false,
        });
        let failure = CommandOutcome::from(
            std::process::Command::new("sh")
                .arg("-c")
                .arg("exit 1")
                .status()
                .unwrap(),
        );
        assert!(output_log_plan(policy, &failure, &output).tails.is_empty());
    }

    #[tokio::test]
    async fn output_max_bytes_kills_command_if_exceeded() {
        let policy = CommandOutputPolicy {
            mode: CommandOutputMode::FailureTail,
            max_bytes: 8,
            tail_lines: 10,
        };
        let outcome = run_named_shell_command_with_policy(
            "test-command",
            "yes output | head -c 65536; sleep 30",
            Duration::from_secs(30),
            policy,
            None,
        )
        .await
        .expect("output cap should be reported as an outcome");
        assert!(!outcome.success);
        assert!(outcome.output_limit_exceeded);
        assert_eq!(outcome.output_limit, Some(8));
    }

    #[tokio::test]
    async fn command_timeout_kills_child_process_tree() {
        let dir = tempfile::tempdir().expect("tempdir");
        let pid_file = dir.path().join("sleep.pid");
        let command = format!(
            "sleep 30 & printf '%s\n' \"$!\" > {}; wait",
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
            "sleep 30 & printf '%s\n' \"$!\" > {}; wait",
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
