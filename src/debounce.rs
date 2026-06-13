use crate::command::{CommandExecutor, CommandOutcome};
use crate::state::{CommandRunnerPhase, RuntimeState};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};
use tokio::time::sleep;
use tracing::{error, info, warn};

pub struct DebounceRunner {
    account: String,
    mailbox: String,
    debounce: Duration,
    cooldown: Duration,
    executor: Arc<dyn CommandExecutor>,
    state: Option<Arc<RuntimeState>>,
    runner_id: String,
}

impl DebounceRunner {
    pub fn new(
        account: impl Into<String>,
        mailbox: impl Into<String>,
        debounce: Duration,
        cooldown: Duration,
        executor: Arc<dyn CommandExecutor>,
        state: Option<Arc<RuntimeState>>,
    ) -> Self {
        let account = account.into();
        let mailbox = mailbox.into();
        let runner_id = format!("{account}/{mailbox}");
        Self {
            account,
            mailbox,
            debounce,
            cooldown,
            executor,
            state,
            runner_id,
        }
    }

    pub async fn run(self, mut events: mpsc::Receiver<()>, mut shutdown: watch::Receiver<bool>) {
        self.mark_runner(CommandRunnerPhase::Idle);
        let mut last_command_finished = None;
        loop {
            let got_event = tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_ok() && *shutdown.borrow() {
                        break;
                    }
                    continue;
                }
                event = events.recv() => event.is_some(),
            };

            if !got_event {
                break;
            }
            self.mark_event();

            loop {
                self.mark_runner(CommandRunnerPhase::Debouncing);
                if !sleep_or_shutdown(self.debounce, &mut shutdown).await {
                    return;
                }
                self.drain_events(&mut events);
                if !self
                    .wait_for_cooldown(last_command_finished, &mut events, &mut shutdown)
                    .await
                {
                    return;
                }
                self.run_once(shutdown.clone()).await;
                last_command_finished = Some(Instant::now());

                if self.drain_events(&mut events) {
                    continue;
                }
                break;
            }
        }
    }

    fn mark_event(&self) {
        if let Some(state) = &self.state {
            state.mark_event();
        }
    }

    fn mark_runner(&self, phase: CommandRunnerPhase) {
        if let Some(state) = &self.state {
            state.mark_command_runner(&self.runner_id, phase);
        }
    }

    fn drain_events(&self, events: &mut mpsc::Receiver<()>) -> bool {
        let mut saw_event = false;
        while let Ok(()) = events.try_recv() {
            saw_event = true;
            self.mark_event();
        }
        saw_event
    }

    async fn wait_for_cooldown(
        &self,
        last_command_finished: Option<Instant>,
        events: &mut mpsc::Receiver<()>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> bool {
        let Some(last_command_finished) = last_command_finished else {
            return true;
        };
        if self.cooldown.is_zero() {
            return true;
        }
        let elapsed = last_command_finished.elapsed();
        if elapsed >= self.cooldown {
            return true;
        }
        self.mark_runner(CommandRunnerPhase::CoolingDown);
        if !sleep_or_shutdown(self.cooldown - elapsed, shutdown).await {
            return false;
        }
        self.drain_events(events);
        true
    }

    async fn run_once(&self, shutdown: watch::Receiver<bool>) {
        info!(
            account = %self.account,
            mailbox = %self.mailbox,
            "starting notification command"
        );
        if let Some(state) = &self.state {
            state.mark_command_started(&self.runner_id);
        }
        match self.executor.run(shutdown).await {
            Ok(outcome) => self.record_outcome(outcome),
            Err(error) => {
                if let Some(state) = &self.state {
                    state.mark_command_error();
                    state.mark_command_runner(&self.runner_id, CommandRunnerPhase::Idle);
                }
                error!(
                    account = %self.account,
                    mailbox = %self.mailbox,
                    %error,
                    "notification command could not run"
                );
            }
        }
    }

    fn record_outcome(&self, outcome: CommandOutcome) {
        if let Some(state) = &self.state {
            state.mark_command_finished(&self.runner_id, &outcome);
        }
        if outcome.success {
            info!(
                account = %self.account,
                mailbox = %self.mailbox,
                status = %outcome.description(),
                "notification command succeeded"
            );
        } else {
            warn!(
                account = %self.account,
                mailbox = %self.mailbox,
                status = %outcome.description(),
                "notification command failed"
            );
        }
    }
}

async fn sleep_or_shutdown(duration: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    if duration.is_zero() {
        return true;
    }
    tokio::select! {
        () = sleep(duration) => true,
        changed = shutdown.changed() => !(changed.is_ok() && *shutdown.borrow()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::CommandRunFuture;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{Instant, timeout};

    #[derive(Clone)]
    struct TestExecutor {
        calls: Arc<AtomicUsize>,
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
        sleep: Duration,
        success: bool,
    }

    impl TestExecutor {
        fn new(sleep: Duration) -> Self {
            Self {
                calls: Arc::new(AtomicUsize::new(0)),
                active: Arc::new(AtomicUsize::new(0)),
                max_active: Arc::new(AtomicUsize::new(0)),
                sleep,
                success: true,
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
                Ok(CommandOutcome {
                    success: this.success,
                    code: Some(if this.success { 0 } else { 1 }),
                    signal: None,
                    timed_out: false,
                    cancelled: false,
                    timeout: None,
                })
            })
        }
    }

    async fn wait_for_calls(calls: &AtomicUsize, expected: usize) {
        timeout(Duration::from_secs(1), async {
            while calls.load(Ordering::SeqCst) < expected {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("timed out waiting for command calls");
    }

    #[tokio::test]
    async fn coalesces_many_quick_events_into_one_run() {
        let exec = TestExecutor::new(Duration::ZERO);
        let (tx, rx) = mpsc::channel(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let runner = DebounceRunner::new(
            "gmail",
            "INBOX",
            Duration::from_millis(30),
            Duration::ZERO,
            Arc::new(exec.clone()),
            None,
        );
        let task = tokio::spawn(runner.run(rx, shutdown_rx));

        tx.send(()).await.unwrap();
        tx.send(()).await.unwrap();
        tx.send(()).await.unwrap();
        wait_for_calls(&exec.calls, 1).await;
        sleep(Duration::from_millis(80)).await;
        assert_eq!(exec.calls.load(Ordering::SeqCst), 1);

        shutdown_tx.send(true).unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn never_overlaps_commands_for_one_mailbox() {
        let exec = TestExecutor::new(Duration::from_millis(80));
        let (tx, rx) = mpsc::channel(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let runner = DebounceRunner::new(
            "gmail",
            "INBOX",
            Duration::from_millis(5),
            Duration::ZERO,
            Arc::new(exec.clone()),
            None,
        );
        let task = tokio::spawn(runner.run(rx, shutdown_rx));

        tx.send(()).await.unwrap();
        wait_for_calls(&exec.calls, 1).await;
        tx.send(()).await.unwrap();
        tx.send(()).await.unwrap();
        wait_for_calls(&exec.calls, 2).await;

        assert_eq!(exec.max_active.load(Ordering::SeqCst), 1);
        shutdown_tx.send(true).unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn dirty_events_during_command_trigger_one_more_run() {
        let exec = TestExecutor::new(Duration::from_millis(80));
        let (tx, rx) = mpsc::channel(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let runner = DebounceRunner::new(
            "gmail",
            "INBOX",
            Duration::from_millis(10),
            Duration::ZERO,
            Arc::new(exec.clone()),
            None,
        );
        let task = tokio::spawn(runner.run(rx, shutdown_rx));

        tx.send(()).await.unwrap();
        wait_for_calls(&exec.calls, 1).await;
        let start = Instant::now();
        while exec.active.load(Ordering::SeqCst) == 0 && start.elapsed() < Duration::from_secs(1) {
            sleep(Duration::from_millis(1)).await;
        }
        tx.send(()).await.unwrap();
        tx.send(()).await.unwrap();
        wait_for_calls(&exec.calls, 2).await;
        sleep(Duration::from_millis(40)).await;
        assert_eq!(exec.calls.load(Ordering::SeqCst), 2);

        shutdown_tx.send(true).unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn cooldown_coalesces_events_after_command() {
        let exec = TestExecutor::new(Duration::ZERO);
        let (tx, rx) = mpsc::channel(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let runner = DebounceRunner::new(
            "gmail",
            "INBOX",
            Duration::from_millis(5),
            Duration::from_millis(80),
            Arc::new(exec.clone()),
            None,
        );
        let task = tokio::spawn(runner.run(rx, shutdown_rx));

        tx.send(()).await.unwrap();
        wait_for_calls(&exec.calls, 1).await;
        tx.send(()).await.unwrap();
        tx.send(()).await.unwrap();
        sleep(Duration::from_millis(30)).await;
        assert_eq!(exec.calls.load(Ordering::SeqCst), 1);
        wait_for_calls(&exec.calls, 2).await;
        sleep(Duration::from_millis(30)).await;
        assert_eq!(exec.calls.load(Ordering::SeqCst), 2);

        shutdown_tx.send(true).unwrap();
        task.await.unwrap();
    }
}
