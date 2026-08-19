use crate::command::CommandOutcome;
use std::collections::HashMap;
use std::fmt;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::sync::watch;

#[derive(Debug)]
pub struct RuntimeState {
    total_accounts: usize,
    total_sources: usize,
    total_command_lanes: usize,
    watcher_stale: Duration,
    command_timeout: Duration,
    status_revision: watch::Sender<u64>,
    inner: Mutex<RuntimeInner>,
}

#[derive(Debug)]
struct RuntimeInner {
    watchers: HashMap<String, WatcherStatus>,
    command_runners: HashMap<String, CommandRunnerStatus>,
    last_event_seen: bool,
    last_command_success: Option<bool>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WatcherPhase {
    Starting,
    Connecting,
    Idling,
    Reconnecting,
    Stopped,
    Crashed,
}

impl fmt::Display for WatcherPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Starting => f.write_str("starting"),
            Self::Connecting => f.write_str("connecting"),
            Self::Idling => f.write_str("idling"),
            Self::Reconnecting => f.write_str("reconnecting"),
            Self::Stopped => f.write_str("stopped"),
            Self::Crashed => f.write_str("crashed"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandRunnerPhase {
    Starting,
    Idle,
    Debouncing,
    CoolingDown,
    Running,
    Stopped,
    Crashed,
}

impl fmt::Display for CommandRunnerPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Starting => f.write_str("starting"),
            Self::Idle => f.write_str("idle"),
            Self::Debouncing => f.write_str("debouncing"),
            Self::CoolingDown => f.write_str("cooling_down"),
            Self::Running => f.write_str("running"),
            Self::Stopped => f.write_str("stopped"),
            Self::Crashed => f.write_str("crashed"),
        }
    }
}

#[derive(Debug)]
struct WatcherStatus {
    phase: WatcherPhase,
    last_progress: Instant,
}

#[derive(Debug)]
struct CommandRunnerStatus {
    phase: CommandRunnerPhase,
    last_progress: Instant,
    command_started: Option<Instant>,
}

impl RuntimeState {
    pub fn new(
        total_accounts: usize,
        total_sources: usize,
        total_command_lanes: usize,
        watcher_stale: Duration,
        command_timeout: Duration,
    ) -> Self {
        let (status_revision, _) = watch::channel(0);
        Self {
            total_accounts,
            total_sources,
            total_command_lanes,
            watcher_stale,
            command_timeout,
            status_revision,
            inner: Mutex::new(RuntimeInner {
                watchers: HashMap::new(),
                command_runners: HashMap::new(),
                last_event_seen: false,
                last_command_success: None,
            }),
        }
    }

    pub fn total_sources(&self) -> usize {
        self.total_sources
    }

    pub fn subscribe_status_changes(&self) -> watch::Receiver<u64> {
        self.status_revision.subscribe()
    }

    pub fn register_watcher(&self, id: impl Into<String>) {
        let mut inner = self.inner.lock().expect("runtime state mutex poisoned");
        inner.watchers.insert(
            id.into(),
            WatcherStatus {
                phase: WatcherPhase::Starting,
                last_progress: Instant::now(),
            },
        );
    }

    pub fn register_command_runner(&self, id: impl Into<String>) {
        let mut inner = self.inner.lock().expect("runtime state mutex poisoned");
        inner.command_runners.insert(
            id.into(),
            CommandRunnerStatus {
                phase: CommandRunnerPhase::Starting,
                last_progress: Instant::now(),
                command_started: None,
            },
        );
    }

    pub fn mark_watcher(&self, id: &str, phase: WatcherPhase) {
        let mut inner = self.inner.lock().expect("runtime state mutex poisoned");
        let entry = inner
            .watchers
            .entry(id.to_string())
            .or_insert_with(|| WatcherStatus {
                phase: WatcherPhase::Starting,
                last_progress: Instant::now(),
            });
        entry.phase = phase;
        entry.last_progress = Instant::now();
    }

    pub fn mark_watcher_progress(&self, id: &str) {
        let mut inner = self.inner.lock().expect("runtime state mutex poisoned");
        let Some(entry) = inner.watchers.get_mut(id) else {
            return;
        };
        if matches!(entry.phase, WatcherPhase::Crashed | WatcherPhase::Stopped) {
            return;
        }
        entry.last_progress = Instant::now();
    }

    pub fn watcher_heartbeat_interval(&self) -> Duration {
        let half_stale = self.watcher_stale / 2;
        let interval = if half_stale.is_zero() {
            Duration::from_millis(1)
        } else {
            half_stale
        };
        std::cmp::min(interval, Duration::from_secs(60))
    }

    pub fn mark_command_runner(&self, id: &str, phase: CommandRunnerPhase) {
        let mut inner = self.inner.lock().expect("runtime state mutex poisoned");
        let entry = inner
            .command_runners
            .entry(id.to_string())
            .or_insert_with(|| CommandRunnerStatus {
                phase: CommandRunnerPhase::Starting,
                last_progress: Instant::now(),
                command_started: None,
            });
        let running_changed =
            (entry.phase == CommandRunnerPhase::Running) != (phase == CommandRunnerPhase::Running);
        entry.phase = phase;
        entry.last_progress = Instant::now();
        if phase != CommandRunnerPhase::Running {
            entry.command_started = None;
        }
        drop(inner);
        if running_changed {
            self.notify_status_changed();
        }
    }

    pub fn mark_command_started(&self, id: &str) {
        let mut inner = self.inner.lock().expect("runtime state mutex poisoned");
        let entry = inner
            .command_runners
            .entry(id.to_string())
            .or_insert_with(|| CommandRunnerStatus {
                phase: CommandRunnerPhase::Starting,
                last_progress: Instant::now(),
                command_started: None,
            });
        entry.phase = CommandRunnerPhase::Running;
        entry.last_progress = Instant::now();
        entry.command_started = Some(Instant::now());
        drop(inner);
        self.notify_status_changed();
    }

    pub fn mark_command_finished(&self, id: &str, outcome: &CommandOutcome) {
        let mut inner = self.inner.lock().expect("runtime state mutex poisoned");
        let entry = inner
            .command_runners
            .entry(id.to_string())
            .or_insert_with(|| CommandRunnerStatus {
                phase: CommandRunnerPhase::Starting,
                last_progress: Instant::now(),
                command_started: None,
            });
        entry.phase = CommandRunnerPhase::Idle;
        entry.last_progress = Instant::now();
        entry.command_started = None;
        inner.last_command_success = Some(outcome.success);
        drop(inner);
        self.notify_status_changed();
    }

    pub fn mark_event(&self) {
        let mut inner = self.inner.lock().expect("runtime state mutex poisoned");
        let changed = !inner.last_event_seen;
        inner.last_event_seen = true;
        drop(inner);
        if changed {
            self.notify_status_changed();
        }
    }

    pub fn mark_command_outcome(&self, outcome: &CommandOutcome) {
        let mut inner = self.inner.lock().expect("runtime state mutex poisoned");
        let changed = inner.last_command_success != Some(outcome.success);
        inner.last_command_success = Some(outcome.success);
        drop(inner);
        if changed {
            self.notify_status_changed();
        }
    }

    pub fn mark_command_error(&self) {
        let mut inner = self.inner.lock().expect("runtime state mutex poisoned");
        let changed = inner.last_command_success != Some(false);
        inner.last_command_success = Some(false);
        drop(inner);
        if changed {
            self.notify_status_changed();
        }
    }

    pub fn status_message(&self) -> String {
        let inner = self.inner.lock().expect("runtime state mutex poisoned");
        let event_text = if inner.last_event_seen {
            "last event received"
        } else {
            "no events yet"
        };
        let command_text = match inner.last_command_success {
            Some(true) => "last command ok",
            Some(false) => "last command failed",
            None => "no command runs yet",
        };
        let running_command_count = inner
            .command_runners
            .values()
            .filter(|runner| runner.phase == CommandRunnerPhase::Running)
            .count();
        format!(
            "watching {} account(s), {} source(s); running commands: {running_command_count}; {event_text}; {command_text}",
            self.total_accounts, self.total_sources,
        )
    }

    fn notify_status_changed(&self) {
        self.status_revision
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }

    pub fn is_healthy(&self) -> bool {
        self.health_problem().is_none()
    }

    pub fn health_problem(&self) -> Option<String> {
        let inner = self.inner.lock().expect("runtime state mutex poisoned");
        self.watchers_health_problem(&inner)
            .or_else(|| self.command_runners_health_problem(&inner))
    }

    fn watchers_health_problem(&self, inner: &RuntimeInner) -> Option<String> {
        if inner.watchers.len() != self.total_sources {
            return Some(format!(
                "watcher count is {}, expected {}",
                inner.watchers.len(),
                self.total_sources
            ));
        }
        inner.watchers.iter().find_map(|(id, watcher)| {
            if matches!(watcher.phase, WatcherPhase::Crashed | WatcherPhase::Stopped) {
                return Some(format!("watcher {id:?} is {}", watcher.phase));
            }
            let elapsed = watcher.last_progress.elapsed();
            if elapsed > self.watcher_stale {
                return Some(format!(
                    "watcher {id:?} stale for {}s, limit {}s",
                    elapsed.as_secs(),
                    self.watcher_stale.as_secs()
                ));
            }
            None
        })
    }

    fn command_runners_health_problem(&self, inner: &RuntimeInner) -> Option<String> {
        if inner.command_runners.len() != self.total_command_lanes {
            return Some(format!(
                "command runner count is {}, expected {}",
                inner.command_runners.len(),
                self.total_command_lanes
            ));
        }
        inner.command_runners.iter().find_map(|(id, runner)| {
            if matches!(
                runner.phase,
                CommandRunnerPhase::Crashed | CommandRunnerPhase::Stopped
            ) {
                return Some(format!("command runner {id:?} is {}", runner.phase));
            }
            if let Some(started) = runner.command_started
                && started.elapsed() > self.command_timeout
            {
                return Some(format!(
                    "command runner {id:?} running for {}s, limit {}s",
                    started.elapsed().as_secs(),
                    self.command_timeout.as_secs()
                ));
            }
            None
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn healthy_state() -> RuntimeState {
        let state = RuntimeState::new(1, 1, 1, Duration::from_secs(60), Duration::from_millis(50));
        state.register_watcher("gmail/INBOX");
        state.register_command_runner("gmail/INBOX");
        state
    }

    #[test]
    fn status_message_has_only_counts_and_generic_state() {
        let state = healthy_state();
        let status = state.status_message();
        assert!(status.contains("watching 1 account(s), 1 source(s)"));
        assert!(status.contains("running commands: 0"));
        assert!(!status.contains("gmail/INBOX"));
        assert!(!status.contains("private-address"));
        assert!(!status.contains("gmail-oauth-token"));
        assert!(!status.contains("gmi sync"));
    }

    #[test]
    fn status_reports_and_signals_running_command_count() {
        let state = healthy_state();
        let mut status_changes = state.subscribe_status_changes();

        state.mark_command_started("gmail/INBOX");
        assert!(status_changes.has_changed().expect("status channel open"));
        assert!(state.status_message().contains("running commands: 1"));
        status_changes.borrow_and_update();

        state.mark_command_finished(
            "gmail/INBOX",
            &CommandOutcome {
                success: true,
                code: Some(0),
                signal: None,
                timed_out: false,
                cancelled: false,
                timeout: None,
                output_limit_exceeded: false,
                output_limit: None,
            },
        );
        assert!(status_changes.has_changed().expect("status channel open"));
        assert!(state.status_message().contains("running commands: 0"));
    }

    #[test]
    fn crashed_watcher_is_unhealthy() {
        let state = healthy_state();
        assert!(state.is_healthy());
        state.mark_watcher("gmail/INBOX", WatcherPhase::Crashed);
        assert!(!state.is_healthy());
    }

    #[test]
    fn missing_command_runner_is_unhealthy() {
        let state = RuntimeState::new(1, 1, 1, Duration::from_secs(60), Duration::from_millis(50));
        state.register_watcher("gmail/INBOX");
        assert!(!state.is_healthy());
    }

    #[test]
    fn crashed_command_runner_is_unhealthy() {
        let state = healthy_state();
        assert!(state.is_healthy());
        state.mark_command_runner("gmail/INBOX", CommandRunnerPhase::Crashed);
        assert!(!state.is_healthy());
    }

    #[tokio::test]
    async fn running_command_past_timeout_is_unhealthy() {
        let state = healthy_state();
        state.mark_command_started("gmail/INBOX");
        assert!(state.is_healthy());
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(!state.is_healthy());
    }

    #[tokio::test]
    async fn watchdog_health_requires_command_runners_not_wedged() {
        let state = healthy_state();
        state.mark_watcher("gmail/INBOX", WatcherPhase::Idling);
        state.mark_command_started("gmail/INBOX");
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(!state.is_healthy());
    }

    #[tokio::test]
    async fn watcher_progress_refresh_keeps_idle_watcher_healthy() {
        let state = RuntimeState::new(1, 1, 1, Duration::from_millis(40), Duration::from_secs(1));
        state.register_watcher("local-state");
        state.register_command_runner("sync");
        state.mark_watcher("local-state", WatcherPhase::Idling);
        state.mark_command_runner("sync", CommandRunnerPhase::Idle);
        assert!(state.is_healthy());

        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(!state.is_healthy());

        state.mark_watcher_progress("local-state");
        assert!(state.is_healthy());
    }

    #[test]
    fn watcher_heartbeat_interval_is_bounded_by_stale_threshold() {
        let short = RuntimeState::new(0, 0, 0, Duration::from_millis(40), Duration::from_secs(1));
        assert_eq!(
            short.watcher_heartbeat_interval(),
            Duration::from_millis(20)
        );

        let long = RuntimeState::new(0, 0, 0, Duration::from_secs(1000), Duration::from_secs(1));
        assert_eq!(long.watcher_heartbeat_interval(), Duration::from_secs(60));
    }
}
