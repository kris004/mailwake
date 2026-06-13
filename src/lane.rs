use crate::command::{CommandExecutor, CommandOutcome};
use crate::state::{CommandRunnerPhase, RuntimeState};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, sleep_until};
use tracing::{debug, error, info, warn};

pub type CommandTriggerStartFuture =
    Pin<Box<dyn Future<Output = Result<CommandLifecycleReceiver, CommandTriggerError>> + Send>>;

pub type CommandLifecycleReceiver = mpsc::UnboundedReceiver<CommandLifecycle>;

pub trait CommandTriggerTarget: Send + Sync {
    fn start(&self) -> CommandTriggerStartFuture;
}

#[derive(Clone, Debug)]
pub enum CommandLifecycle {
    Started,
    Finished(CommandRunReport),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommandRunReport {
    Outcome(CommandOutcome),
    Error(Arc<str>),
    Cancelled,
}

impl CommandRunReport {
    pub fn success(&self) -> bool {
        matches!(self, Self::Outcome(outcome) if outcome.success)
    }

    pub fn description(&self) -> String {
        match self {
            Self::Outcome(outcome) => outcome.description(),
            Self::Error(error) => format!("could not run: {error}"),
            Self::Cancelled => "cancelled by shutdown".to_string(),
        }
    }
}

#[derive(Debug, Error)]
pub enum CommandTriggerError {
    #[error("command lane is stopped")]
    LaneStopped,
    #[error("command lane stopped before reporting command completion")]
    CompletionLost,
}

pub async fn await_trigger_finished(
    mut lifecycle: CommandLifecycleReceiver,
) -> Result<CommandRunReport, CommandTriggerError> {
    while let Some(event) = lifecycle.recv().await {
        if let CommandLifecycle::Finished(report) = event {
            return Ok(report);
        }
    }
    Err(CommandTriggerError::CompletionLost)
}

#[derive(Clone)]
pub struct CommandTrigger {
    command: Arc<str>,
    source: Arc<str>,
    requests: mpsc::Sender<CommandRequest>,
}

impl CommandTrigger {
    pub fn new(
        command: impl Into<Arc<str>>,
        source: impl Into<Arc<str>>,
        requests: mpsc::Sender<CommandRequest>,
    ) -> Self {
        Self {
            command: command.into(),
            source: source.into(),
            requests,
        }
    }

    pub async fn trigger(&self) -> Result<CommandRunReport, CommandTriggerError> {
        let lifecycle = self.start().await?;
        await_trigger_finished(lifecycle).await
    }
}

impl CommandTriggerTarget for CommandTrigger {
    fn start(&self) -> CommandTriggerStartFuture {
        let command = Arc::clone(&self.command);
        let source = Arc::clone(&self.source);
        let requests = self.requests.clone();
        Box::pin(async move {
            let (lifecycle_tx, lifecycle_rx) = mpsc::unbounded_channel();
            let request = CommandRequest {
                command,
                source,
                lifecycle: lifecycle_tx,
            };
            requests
                .send(request)
                .await
                .map_err(|_| CommandTriggerError::LaneStopped)?;
            Ok(lifecycle_rx)
        })
    }
}

pub struct CommandRequest {
    command: Arc<str>,
    source: Arc<str>,
    lifecycle: mpsc::UnboundedSender<CommandLifecycle>,
}

#[derive(Clone)]
pub struct LaneCommand {
    pub name: Arc<str>,
    pub executor: Arc<dyn CommandExecutor>,
    pub min_interval: Duration,
}

pub struct CommandLaneRunner {
    lane: String,
    commands: HashMap<Arc<str>, LaneCommand>,
    requests: mpsc::Receiver<CommandRequest>,
    state: Option<Arc<RuntimeState>>,
}

impl CommandLaneRunner {
    pub fn new(
        lane: impl Into<String>,
        commands: Vec<LaneCommand>,
        requests: mpsc::Receiver<CommandRequest>,
        state: Option<Arc<RuntimeState>>,
    ) -> Self {
        let commands = commands
            .into_iter()
            .map(|command| (Arc::clone(&command.name), command))
            .collect();
        Self {
            lane: lane.into(),
            commands,
            requests,
            state,
        }
    }

    pub async fn run(mut self, mut shutdown: watch::Receiver<bool>) {
        self.mark_runner(CommandRunnerPhase::Idle);
        let mut pending = PendingRequests::default();
        let mut last_finished: HashMap<Arc<str>, Instant> = HashMap::new();

        loop {
            if pending.is_empty() {
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_ok() && *shutdown.borrow() {
                            break;
                        }
                    }
                    request = self.requests.recv() => {
                        let Some(request) = request else {
                            break;
                        };
                        pending.push(request);
                    }
                }
            }

            while let Some(command_name) = pending.pop_next_command() {
                let Some(command) = self.commands.get(&command_name).cloned() else {
                    let waiters = pending.take_waiters(&command_name);
                    finish_waiters(
                        waiters,
                        CommandRunReport::Error(Arc::from("unknown command")),
                    );
                    continue;
                };
                let mut waiters = pending.take_waiters(&command_name);
                if !self
                    .wait_for_min_interval(
                        &command,
                        &mut waiters,
                        &mut pending,
                        &last_finished,
                        &mut shutdown,
                    )
                    .await
                {
                    finish_waiters(waiters, CommandRunReport::Cancelled);
                    cancel_pending(pending);
                    return;
                }

                start_waiters(&waiters);
                let report = self.run_command(&command, shutdown.clone()).await;
                last_finished.insert(Arc::clone(&command.name), Instant::now());
                finish_waiters(waiters, report);
                self.drain_requests(&mut pending);

                if *shutdown.borrow() {
                    cancel_pending(pending);
                    return;
                }
            }
        }

        cancel_pending(pending);
    }

    async fn wait_for_min_interval(
        &mut self,
        command: &LaneCommand,
        waiters: &mut Vec<mpsc::UnboundedSender<CommandLifecycle>>,
        pending: &mut PendingRequests,
        last_finished: &HashMap<Arc<str>, Instant>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> bool {
        let Some(last_finished) = last_finished.get(&command.name) else {
            return true;
        };
        if command.min_interval.is_zero() {
            return true;
        }
        let run_at = *last_finished + command.min_interval;
        if Instant::now() >= run_at {
            return true;
        }

        self.mark_runner(CommandRunnerPhase::CoolingDown);
        let sleep = sleep_until(run_at);
        tokio::pin!(sleep);

        loop {
            tokio::select! {
                () = &mut sleep => {
                    return true;
                }
                changed = shutdown.changed() => {
                    return !(changed.is_ok() && *shutdown.borrow());
                }
                request = self.requests.recv() => {
                    let Some(request) = request else {
                        return false;
                    };
                    if request.command == command.name {
                        debug!(
                            lane = %self.lane,
                            command = %command.name,
                            source = %request.source,
                            "coalescing command request into pending lane cooldown run"
                        );
                        waiters.push(request.lifecycle);
                    } else {
                        pending.push(request);
                    }
                }
            }
        }
    }

    async fn run_command(
        &self,
        command: &LaneCommand,
        shutdown: watch::Receiver<bool>,
    ) -> CommandRunReport {
        info!(
            lane = %self.lane,
            command = %command.name,
            "starting notification command"
        );
        if let Some(state) = &self.state {
            state.mark_command_started(&self.lane);
        }

        let report = match command.executor.run(shutdown).await {
            Ok(outcome) => CommandRunReport::Outcome(outcome),
            Err(error) => CommandRunReport::Error(Arc::from(error.to_string())),
        };

        match &report {
            CommandRunReport::Outcome(outcome) => {
                if let Some(state) = &self.state {
                    state.mark_command_finished(&self.lane, outcome);
                }
                if outcome.success {
                    info!(
                        lane = %self.lane,
                        command = %command.name,
                        status = %outcome.description(),
                        "notification command succeeded"
                    );
                } else {
                    warn!(
                        lane = %self.lane,
                        command = %command.name,
                        status = %outcome.description(),
                        "notification command failed"
                    );
                }
            }
            CommandRunReport::Error(error) => {
                if let Some(state) = &self.state {
                    state.mark_command_error();
                    state.mark_command_runner(&self.lane, CommandRunnerPhase::Idle);
                }
                error!(
                    lane = %self.lane,
                    command = %command.name,
                    %error,
                    "notification command could not run"
                );
            }
            CommandRunReport::Cancelled => {
                if let Some(state) = &self.state {
                    state.mark_command_runner(&self.lane, CommandRunnerPhase::Idle);
                }
            }
        }

        report
    }

    fn drain_requests(&mut self, pending: &mut PendingRequests) {
        while let Ok(request) = self.requests.try_recv() {
            pending.push(request);
        }
    }

    fn mark_runner(&self, phase: CommandRunnerPhase) {
        if let Some(state) = &self.state {
            state.mark_command_runner(&self.lane, phase);
        }
    }
}

#[derive(Default)]
struct PendingRequests {
    order: VecDeque<Arc<str>>,
    waiters: HashMap<Arc<str>, Vec<mpsc::UnboundedSender<CommandLifecycle>>>,
}

impl PendingRequests {
    fn is_empty(&self) -> bool {
        self.waiters.is_empty()
    }

    fn push(&mut self, request: CommandRequest) {
        let command = Arc::clone(&request.command);
        match self.waiters.get_mut(&command) {
            Some(waiters) => waiters.push(request.lifecycle),
            None => {
                self.order.push_back(Arc::clone(&command));
                self.waiters.insert(command, vec![request.lifecycle]);
            }
        }
    }

    fn pop_next_command(&mut self) -> Option<Arc<str>> {
        while let Some(command) = self.order.pop_front() {
            if self.waiters.contains_key(&command) {
                return Some(command);
            }
        }
        None
    }

    fn take_waiters(&mut self, command: &Arc<str>) -> Vec<mpsc::UnboundedSender<CommandLifecycle>> {
        self.waiters.remove(command).unwrap_or_default()
    }
}

fn start_waiters(waiters: &[mpsc::UnboundedSender<CommandLifecycle>]) {
    for waiter in waiters {
        let _ = waiter.send(CommandLifecycle::Started);
    }
}

fn finish_waiters(waiters: Vec<mpsc::UnboundedSender<CommandLifecycle>>, report: CommandRunReport) {
    for waiter in waiters {
        let _ = waiter.send(CommandLifecycle::Finished(report.clone()));
    }
}

fn cancel_pending(pending: PendingRequests) {
    for waiters in pending.waiters.into_values() {
        finish_waiters(waiters, CommandRunReport::Cancelled);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::CommandRunFuture;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{sleep, timeout};

    #[derive(Clone)]
    struct TestExecutor {
        calls: Arc<AtomicUsize>,
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
        sleep: Duration,
    }

    impl TestExecutor {
        fn new(sleep: Duration) -> Self {
            Self {
                calls: Arc::new(AtomicUsize::new(0)),
                active: Arc::new(AtomicUsize::new(0)),
                max_active: Arc::new(AtomicUsize::new(0)),
                sleep,
            }
        }
    }

    impl CommandExecutor for TestExecutor {
        fn run(&self, _shutdown: watch::Receiver<bool>) -> CommandRunFuture {
            let this = self.clone();
            Box::pin(async move {
                this.calls.fetch_add(1, Ordering::SeqCst);
                let active = this.active.fetch_add(1, Ordering::SeqCst) + 1;
                this.max_active.fetch_max(active, Ordering::SeqCst);
                sleep(this.sleep).await;
                this.active.fetch_sub(1, Ordering::SeqCst);
                Ok(ok_outcome())
            })
        }
    }

    fn ok_outcome() -> CommandOutcome {
        CommandOutcome {
            success: true,
            code: Some(0),
            signal: None,
            timed_out: false,
            cancelled: false,
            timeout: None,
            output_limit_exceeded: false,
            output_limit: None,
        }
    }

    async fn spawn_lane(
        lane: &str,
        commands: Vec<LaneCommand>,
    ) -> (
        mpsc::Sender<CommandRequest>,
        watch::Sender<bool>,
        tokio::task::JoinHandle<()>,
    ) {
        let (tx, rx) = mpsc::channel(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let runner = CommandLaneRunner::new(lane, commands, rx, None);
        let handle = tokio::spawn(runner.run(shutdown_rx));
        (tx, shutdown_tx, handle)
    }

    #[tokio::test]
    async fn commands_in_same_lane_do_not_overlap() {
        let exec_a = TestExecutor::new(Duration::from_millis(60));
        let exec_b = TestExecutor {
            calls: Arc::new(AtomicUsize::new(0)),
            active: Arc::clone(&exec_a.active),
            max_active: Arc::clone(&exec_a.max_active),
            sleep: Duration::from_millis(60),
        };
        let commands = vec![
            LaneCommand {
                name: Arc::from("a"),
                executor: Arc::new(exec_a.clone()),
                min_interval: Duration::ZERO,
            },
            LaneCommand {
                name: Arc::from("b"),
                executor: Arc::new(exec_b.clone()),
                min_interval: Duration::ZERO,
            },
        ];
        let (tx, shutdown_tx, handle) = spawn_lane("shared", commands).await;
        let a = CommandTrigger::new("a", "imap-source", tx.clone());
        let b = CommandTrigger::new("b", "fs-source", tx);

        let first = tokio::spawn(async move { a.trigger().await.unwrap() });
        let second = tokio::spawn(async move { b.trigger().await.unwrap() });
        let (first, second) = tokio::join!(first, second);
        assert!(first.unwrap().success());
        assert!(second.unwrap().success());
        assert_eq!(exec_a.max_active.load(Ordering::SeqCst), 1);

        let _ = shutdown_tx.send(true);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn repeated_requests_for_busy_command_are_coalesced() {
        let exec = TestExecutor::new(Duration::from_millis(70));
        let commands = vec![LaneCommand {
            name: Arc::from("sync"),
            executor: Arc::new(exec.clone()),
            min_interval: Duration::ZERO,
        }];
        let (tx, shutdown_tx, handle) = spawn_lane("shared", commands).await;
        let first = CommandTrigger::new("sync", "one", tx.clone());
        let second = CommandTrigger::new("sync", "two", tx.clone());
        let third = CommandTrigger::new("sync", "three", tx);

        let first_task = tokio::spawn(async move { first.trigger().await.unwrap() });
        timeout(Duration::from_secs(1), async {
            while exec.calls.load(Ordering::SeqCst) == 0 {
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        let second_task = tokio::spawn(async move { second.trigger().await.unwrap() });
        let third_task = tokio::spawn(async move { third.trigger().await.unwrap() });

        let _ = tokio::join!(first_task, second_task, third_task);
        assert_eq!(exec.calls.load(Ordering::SeqCst), 2);

        let _ = shutdown_tx.send(true);
        handle.await.unwrap();
    }
}
