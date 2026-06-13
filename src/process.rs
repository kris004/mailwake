use std::io;
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use thiserror::Error;
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout};

const TERMINATE_GRACE: Duration = Duration::from_secs(2);
const WAIT_POLL_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShellStreamMode {
    Null,
    Capture,
    Inherit,
}

#[derive(Debug, Default)]
pub struct ShellCapturedOutput {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
}

impl ShellCapturedOutput {
    pub fn stdout_len(&self) -> usize {
        self.stdout.len()
    }

    pub fn stderr_len(&self) -> usize {
        self.stderr.len()
    }

    pub fn captured_any(&self) -> bool {
        !(self.stdout.is_empty() && self.stderr.is_empty())
    }
}

#[derive(Debug)]
pub struct ShellOutput {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
}

impl ShellOutput {
    fn new(status: ExitStatus, captured: ShellCapturedOutput) -> Self {
        Self {
            status,
            stdout: captured.stdout,
            stderr: captured.stderr,
            stdout_truncated: captured.stdout_truncated,
            stderr_truncated: captured.stderr_truncated,
        }
    }

    pub fn captured_output(&self) -> ShellCapturedOutput {
        ShellCapturedOutput {
            stdout: self.stdout.clone(),
            stderr: self.stderr.clone(),
            stdout_truncated: self.stdout_truncated,
            stderr_truncated: self.stderr_truncated,
        }
    }
}

#[derive(Debug)]
pub struct OutputLimitExceeded {
    pub stream: &'static str,
    pub limit: usize,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
}

impl OutputLimitExceeded {
    fn new(first: PipeLimitExceeded, captured: ShellCapturedOutput) -> Self {
        Self {
            stream: first.stream,
            limit: first.limit,
            stdout: captured.stdout,
            stderr: captured.stderr,
            stdout_truncated: captured.stdout_truncated,
            stderr_truncated: captured.stderr_truncated,
        }
    }

    pub fn captured_output(&self) -> ShellCapturedOutput {
        ShellCapturedOutput {
            stdout: self.stdout.clone(),
            stderr: self.stderr.clone(),
            stdout_truncated: self.stdout_truncated,
            stderr_truncated: self.stderr_truncated,
        }
    }
}

#[derive(Debug)]
pub enum ShellRun {
    Completed(ShellOutput),
    TimedOut(ShellCapturedOutput),
    Cancelled(ShellCapturedOutput),
    OutputLimitExceeded(OutputLimitExceeded),
}

enum End {
    Completed(ExitStatus),
    TimedOut,
    Cancelled,
    OutputLimitExceeded(PipeLimitExceeded),
}

enum PipeOutput {
    Data(Vec<u8>),
    TooLarge(PipeLimitExceeded),
}

enum PipeReadError {
    Io(io::Error),
    TooLarge(PipeLimitExceeded),
}

#[derive(Clone, Debug)]
struct PipeLimitExceeded {
    stream: &'static str,
    limit: usize,
    output: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum ShellProcessError {
    #[error("shell command could not be started: {source}")]
    Start {
        #[source]
        source: io::Error,
    },
    #[error("shell command could not be waited on: {source}")]
    Wait {
        #[source]
        source: io::Error,
    },
    #[error("shell command output could not be collected: {source}")]
    Output {
        #[source]
        source: io::Error,
    },
}

pub async fn run_shell_process(
    command: &str,
    capture_stdout: bool,
    capture_stderr: bool,
    command_timeout: Duration,
    output_limit: Option<usize>,
    shutdown: Option<watch::Receiver<bool>>,
) -> Result<ShellRun, ShellProcessError> {
    run_shell_process_with_streams(
        command,
        if capture_stdout {
            ShellStreamMode::Capture
        } else {
            ShellStreamMode::Null
        },
        if capture_stderr {
            ShellStreamMode::Capture
        } else {
            ShellStreamMode::Null
        },
        command_timeout,
        output_limit,
        shutdown,
    )
    .await
}

pub async fn run_shell_process_with_streams(
    command: &str,
    stdout_mode: ShellStreamMode,
    stderr_mode: ShellStreamMode,
    command_timeout: Duration,
    output_limit: Option<usize>,
    shutdown: Option<watch::Receiver<bool>>,
) -> Result<ShellRun, ShellProcessError> {
    let mut process = Command::new("sh");
    process
        .arg("-c")
        .arg(command)
        .stdin(Stdio::null())
        .kill_on_drop(true);
    configure_process_group(&mut process);

    configure_stdio(&mut process, stdout_mode, stderr_mode);

    let mut child = process
        .spawn()
        .map_err(|source| ShellProcessError::Start { source })?;
    let pgid = child.id().map(|id| id as libc::pid_t);

    let (limit_tx, limit_rx) = mpsc::unbounded_channel();
    let shared_limit = output_limit.map(SharedOutputLimit::new);
    let stdout_task = child
        .stdout
        .take()
        .map(|pipe| read_pipe(pipe, "stdout", shared_limit.clone(), limit_tx.clone()));
    let stderr_task = child
        .stderr
        .take()
        .map(|pipe| read_pipe(pipe, "stderr", shared_limit.clone(), limit_tx.clone()));
    let mut limit_rx = shared_limit.map(|_| limit_rx);

    let mut shutdown = shutdown;
    let end = wait_for_child(
        &mut child,
        pgid,
        command_timeout,
        &mut shutdown,
        &mut limit_rx,
    )
    .await?;

    let run = match end {
        End::Completed(status) => match collect_captured_output(stdout_task, stderr_task).await? {
            CollectedOutput::Captured(captured) => {
                ShellRun::Completed(ShellOutput::new(status, captured))
            }
            CollectedOutput::TooLarge(first, captured) => {
                ShellRun::OutputLimitExceeded(OutputLimitExceeded::new(first, captured))
            }
        },
        End::TimedOut => match collect_captured_output(stdout_task, stderr_task).await? {
            CollectedOutput::Captured(captured) => ShellRun::TimedOut(captured),
            CollectedOutput::TooLarge(first, captured) => {
                ShellRun::OutputLimitExceeded(OutputLimitExceeded::new(first, captured))
            }
        },
        End::Cancelled => match collect_captured_output(stdout_task, stderr_task).await? {
            CollectedOutput::Captured(captured) => ShellRun::Cancelled(captured),
            CollectedOutput::TooLarge(first, captured) => {
                ShellRun::OutputLimitExceeded(OutputLimitExceeded::new(first, captured))
            }
        },
        End::OutputLimitExceeded(first) => {
            match collect_captured_output(stdout_task, stderr_task).await? {
                CollectedOutput::Captured(captured) | CollectedOutput::TooLarge(_, captured) => {
                    ShellRun::OutputLimitExceeded(OutputLimitExceeded::new(first, captured))
                }
            }
        }
    };

    Ok(run)
}

fn configure_stdio(
    process: &mut Command,
    stdout_mode: ShellStreamMode,
    stderr_mode: ShellStreamMode,
) {
    match stdout_mode {
        ShellStreamMode::Null => {
            process.stdout(Stdio::null());
        }
        ShellStreamMode::Capture => {
            process.stdout(Stdio::piped());
        }
        ShellStreamMode::Inherit => {
            process.stdout(Stdio::inherit());
        }
    }

    match stderr_mode {
        ShellStreamMode::Null => {
            process.stderr(Stdio::null());
        }
        ShellStreamMode::Capture => {
            process.stderr(Stdio::piped());
        }
        ShellStreamMode::Inherit => {
            process.stderr(Stdio::inherit());
        }
    }
}

#[derive(Clone)]
struct SharedOutputLimit {
    limit: usize,
    used: Arc<Mutex<usize>>,
}

impl SharedOutputLimit {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            used: Arc::new(Mutex::new(0)),
        }
    }

    fn reserve(&self, requested: usize) -> usize {
        let mut used = self.used.lock().expect("output limit mutex poisoned");
        let remaining = self.limit.saturating_sub(*used);
        let allowed = std::cmp::min(remaining, requested);
        *used = used.saturating_add(allowed);
        allowed
    }
}

async fn wait_for_child(
    child: &mut Child,
    pgid: Option<libc::pid_t>,
    command_timeout: Duration,
    shutdown: &mut Option<watch::Receiver<bool>>,
    limit_rx: &mut Option<mpsc::UnboundedReceiver<PipeLimitExceeded>>,
) -> Result<End, ShellProcessError> {
    let deadline = Instant::now() + command_timeout;
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|source| ShellProcessError::Wait { source })?
        {
            return Ok(End::Completed(status));
        }

        if shutdown.as_ref().is_some_and(|receiver| *receiver.borrow()) {
            terminate_process_group(child, pgid)
                .await
                .map_err(|source| ShellProcessError::Wait { source })?;
            return Ok(End::Cancelled);
        }

        let now = Instant::now();
        if now >= deadline {
            terminate_process_group(child, pgid)
                .await
                .map_err(|source| ShellProcessError::Wait { source })?;
            return Ok(End::TimedOut);
        }
        let sleep_for = std::cmp::min(WAIT_POLL_INTERVAL, deadline - now);
        let tick = sleep(sleep_for);
        tokio::pin!(tick);

        match (shutdown.as_mut(), limit_rx.as_mut()) {
            (Some(receiver), Some(receiver_limit)) => {
                tokio::select! {
                    () = &mut tick => {}
                    changed = receiver.changed() => {
                        if changed.is_ok() && *receiver.borrow() {
                            terminate_process_group(child, pgid)
                                .await
                                .map_err(|source| ShellProcessError::Wait { source })?;
                            return Ok(End::Cancelled);
                        }
                    }
                    exceeded = receiver_limit.recv() => {
                        if let Some(exceeded) = exceeded {
                            terminate_process_group(child, pgid)
                                .await
                                .map_err(|source| ShellProcessError::Wait { source })?;
                            return Ok(End::OutputLimitExceeded(exceeded));
                        }
                        *limit_rx = None;
                    }
                }
            }
            (Some(receiver), None) => {
                tokio::select! {
                    () = &mut tick => {}
                    changed = receiver.changed() => {
                        if changed.is_ok() && *receiver.borrow() {
                            terminate_process_group(child, pgid)
                                .await
                                .map_err(|source| ShellProcessError::Wait { source })?;
                            return Ok(End::Cancelled);
                        }
                    }
                }
            }
            (None, Some(receiver_limit)) => {
                tokio::select! {
                    () = &mut tick => {}
                    exceeded = receiver_limit.recv() => {
                        if let Some(exceeded) = exceeded {
                            terminate_process_group(child, pgid)
                                .await
                                .map_err(|source| ShellProcessError::Wait { source })?;
                            return Ok(End::OutputLimitExceeded(exceeded));
                        }
                        *limit_rx = None;
                    }
                }
            }
            (None, None) => {
                tick.await;
            }
        }
    }
}

fn read_pipe<R>(
    mut pipe: R,
    stream: &'static str,
    limit: Option<SharedOutputLimit>,
    limit_tx: mpsc::UnboundedSender<PipeLimitExceeded>,
) -> JoinHandle<Result<Vec<u8>, PipeReadError>>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut output = Vec::new();
        let mut buffer = [0; 8192];
        loop {
            let bytes = tokio::io::AsyncReadExt::read(&mut pipe, &mut buffer)
                .await
                .map_err(PipeReadError::Io)?;
            if bytes == 0 {
                return Ok(output);
            }
            if let Some(limit) = &limit {
                let allowed = limit.reserve(bytes);
                output.extend_from_slice(&buffer[..allowed]);
                if allowed < bytes {
                    let exceeded = PipeLimitExceeded {
                        stream,
                        limit: limit.limit,
                        output,
                    };
                    let _ = limit_tx.send(exceeded.clone());
                    return Err(PipeReadError::TooLarge(exceeded));
                }
            } else {
                output.extend_from_slice(&buffer[..bytes]);
            }
        }
    })
}

enum CollectedOutput {
    Captured(ShellCapturedOutput),
    TooLarge(PipeLimitExceeded, ShellCapturedOutput),
}

async fn collect_captured_output(
    stdout_task: Option<JoinHandle<Result<Vec<u8>, PipeReadError>>>,
    stderr_task: Option<JoinHandle<Result<Vec<u8>, PipeReadError>>>,
) -> Result<CollectedOutput, ShellProcessError> {
    let stdout = collect_pipe(stdout_task).await?;
    let stderr = collect_pipe(stderr_task).await?;
    let mut first_exceeded = None;
    let (stdout, stdout_truncated) = match stdout {
        PipeOutput::Data(output) => (output, false),
        PipeOutput::TooLarge(exceeded) => {
            first_exceeded = Some(exceeded.clone());
            (exceeded.output, true)
        }
    };
    let (stderr, stderr_truncated) = match stderr {
        PipeOutput::Data(output) => (output, false),
        PipeOutput::TooLarge(exceeded) => {
            if first_exceeded.is_none() {
                first_exceeded = Some(exceeded.clone());
            }
            (exceeded.output, true)
        }
    };
    let captured = ShellCapturedOutput {
        stdout,
        stderr,
        stdout_truncated,
        stderr_truncated,
    };
    Ok(match first_exceeded {
        Some(exceeded) => CollectedOutput::TooLarge(exceeded, captured),
        None => CollectedOutput::Captured(captured),
    })
}

async fn collect_pipe(
    task: Option<JoinHandle<Result<Vec<u8>, PipeReadError>>>,
) -> Result<PipeOutput, ShellProcessError> {
    let Some(task) = task else {
        return Ok(PipeOutput::Data(Vec::new()));
    };
    match task.await.map_err(|source| ShellProcessError::Output {
        source: io::Error::other(format!("pipe reader task failed: {source}")),
    })? {
        Ok(output) => Ok(PipeOutput::Data(output)),
        Err(PipeReadError::TooLarge(exceeded)) => Ok(PipeOutput::TooLarge(exceeded)),
        Err(PipeReadError::Io(source)) => Err(ShellProcessError::Output { source }),
    }
}

async fn terminate_process_group(
    child: &mut Child,
    pgid: Option<libc::pid_t>,
) -> io::Result<ExitStatus> {
    if let Some(pgid) = pgid {
        signal_process_group(pgid, libc::SIGTERM)?;
    } else {
        child.start_kill()?;
    }

    let status = match timeout(TERMINATE_GRACE, child.wait()).await {
        Ok(result) => result?,
        Err(_) => {
            if let Some(pgid) = pgid {
                signal_process_group(pgid, libc::SIGKILL)?;
            } else {
                child.start_kill()?;
            }
            child.wait().await?
        }
    };

    // The shell can die from SIGTERM before all descendants in its process
    // group do. Send SIGKILL once more to clean up any remaining children before
    // collecting pipes; ESRCH is treated as success by signal_process_group.
    if let Some(pgid) = pgid {
        signal_process_group(pgid, libc::SIGKILL)?;
    }

    Ok(status)
}

fn signal_process_group(pgid: libc::pid_t, signal: libc::c_int) -> io::Result<()> {
    if pgid <= 0 {
        return Ok(());
    }
    let result = unsafe { libc::kill(-pgid, signal) };
    if result == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        return Ok(());
    }
    Err(error)
}

#[cfg(unix)]
fn configure_process_group(process: &mut Command) {
    unsafe {
        process.pre_exec(|| {
            if libc::setsid() < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(not(unix))]
fn configure_process_group(_process: &mut Command) {}
