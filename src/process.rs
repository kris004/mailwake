use std::io;
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use thiserror::Error;
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout};

const TERMINATE_GRACE: Duration = Duration::from_secs(2);
const WAIT_POLL_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Debug)]
pub struct ShellOutput {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OutputLimitExceeded {
    pub stream: &'static str,
    pub limit: usize,
}

#[derive(Debug)]
pub enum ShellRun {
    Completed(ShellOutput),
    TimedOut,
    Cancelled,
    OutputLimitExceeded(OutputLimitExceeded),
}

enum End {
    Completed(ExitStatus),
    TimedOut,
    Cancelled,
    OutputLimitExceeded(OutputLimitExceeded),
}

enum PipeOutput {
    Data(Vec<u8>),
    TooLarge(OutputLimitExceeded),
}

enum PipeReadError {
    Io(io::Error),
    TooLarge(OutputLimitExceeded),
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
    let mut process = Command::new("sh");
    process
        .arg("-c")
        .arg(command)
        .stdin(Stdio::null())
        .kill_on_drop(true);
    configure_process_group(&mut process);

    if capture_stdout {
        process.stdout(Stdio::piped());
    } else {
        process.stdout(Stdio::null());
    }
    if capture_stderr {
        process.stderr(Stdio::piped());
    } else {
        process.stderr(Stdio::null());
    }

    let mut child = process
        .spawn()
        .map_err(|source| ShellProcessError::Start { source })?;
    let pgid = child.id().map(|id| id as libc::pid_t);

    let (limit_tx, limit_rx) = mpsc::unbounded_channel();
    let stdout_task = child
        .stdout
        .take()
        .map(|pipe| read_pipe(pipe, "stdout", output_limit, limit_tx.clone()));
    let stderr_task = child
        .stderr
        .take()
        .map(|pipe| read_pipe(pipe, "stderr", output_limit, limit_tx));
    let mut limit_rx = output_limit.map(|_| limit_rx);

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
        End::Completed(status) => {
            let stdout = collect_pipe(stdout_task).await?;
            let stderr = collect_pipe(stderr_task).await?;
            match (stdout, stderr) {
                (PipeOutput::TooLarge(exceeded), _) | (_, PipeOutput::TooLarge(exceeded)) => {
                    ShellRun::OutputLimitExceeded(exceeded)
                }
                (PipeOutput::Data(stdout), PipeOutput::Data(stderr)) => {
                    ShellRun::Completed(ShellOutput {
                        status,
                        stdout,
                        stderr,
                    })
                }
            }
        }
        End::TimedOut => {
            drain_pipe(stdout_task).await;
            drain_pipe(stderr_task).await;
            ShellRun::TimedOut
        }
        End::Cancelled => {
            drain_pipe(stdout_task).await;
            drain_pipe(stderr_task).await;
            ShellRun::Cancelled
        }
        End::OutputLimitExceeded(exceeded) => {
            drain_pipe(stdout_task).await;
            drain_pipe(stderr_task).await;
            ShellRun::OutputLimitExceeded(exceeded)
        }
    };

    Ok(run)
}

async fn wait_for_child(
    child: &mut Child,
    pgid: Option<libc::pid_t>,
    command_timeout: Duration,
    shutdown: &mut Option<watch::Receiver<bool>>,
    limit_rx: &mut Option<mpsc::UnboundedReceiver<OutputLimitExceeded>>,
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
    limit: Option<usize>,
    limit_tx: mpsc::UnboundedSender<OutputLimitExceeded>,
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
            if let Some(limit) = limit
                && output.len().saturating_add(bytes) > limit
            {
                let exceeded = OutputLimitExceeded { stream, limit };
                let _ = limit_tx.send(exceeded);
                return Err(PipeReadError::TooLarge(exceeded));
            }
            output.extend_from_slice(&buffer[..bytes]);
        }
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

async fn drain_pipe(task: Option<JoinHandle<Result<Vec<u8>, PipeReadError>>>) {
    if let Some(task) = task {
        let _ = task.await;
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
