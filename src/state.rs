use crate::command::CommandOutcome;
use std::collections::HashMap;
use std::fmt;
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct RuntimeState {
    total_accounts: usize,
    total_mailboxes: usize,
    watcher_stale: Duration,
    command_timeout: Duration,
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
        total_mailboxes: usize,
        watcher_stale: Duration,
        command_timeout: Duration,
    ) -> Self {
        Self {
            total_accounts,
            total_mailboxes,
            watcher_stale,
            command_timeout,
            inner: Mutex::new(RuntimeInner {
                watchers: HashMap::new(),
                command_runners: HashMap::new(),
                last_event_seen: false,
                last_command_success: None,
            }),
        }
    }

    pub fn total_mailboxes(&self) -> usize {
        self.total_mailboxes
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
        entry.phase = phase;
        entry.last_progress = Instant::now();
        if phase != CommandRunnerPhase::Running {
            entry.command_started = None;
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
    }

    pub fn mark_event(&self) {
        let mut inner = self.inner.lock().expect("runtime state mutex poisoned");
        inner.last_event_seen = true;
    }

    pub fn mark_command_outcome(&self, outcome: &CommandOutcome) {
        let mut inner = self.inner.lock().expect("runtime state mutex poisoned");
        inner.last_command_success = Some(outcome.success);
    }

    pub fn mark_command_error(&self) {
        let mut inner = self.inner.lock().expect("runtime state mutex poisoned");
        inner.last_command_success = Some(false);
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
        format!(
            "watching {} account(s), {} mailbox(es); {event_text}; {command_text}",
            self.total_accounts, self.total_mailboxes
        )
    }

    pub fn is_healthy(&self) -> bool {
        let inner = self.inner.lock().expect("runtime state mutex poisoned");
        self.watchers_healthy(&inner) && self.command_runners_healthy(&inner)
    }

    fn watchers_healthy(&self, inner: &RuntimeInner) -> bool {
        if inner.watchers.len() != self.total_mailboxes {
            return false;
        }
        inner.watchers.values().all(|watcher| {
            if matches!(watcher.phase, WatcherPhase::Crashed | WatcherPhase::Stopped) {
                return false;
            }
            watcher.last_progress.elapsed() <= self.watcher_stale
        })
    }

    fn command_runners_healthy(&self, inner: &RuntimeInner) -> bool {
        if inner.command_runners.len() != self.total_mailboxes {
            return false;
        }
        inner.command_runners.values().all(|runner| {
            if matches!(
                runner.phase,
                CommandRunnerPhase::Crashed | CommandRunnerPhase::Stopped
            ) {
                return false;
            }
            if let Some(started) = runner.command_started
                && started.elapsed() > self.command_timeout
            {
                return false;
            }
            true
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn healthy_state() -> RuntimeState {
        let state = RuntimeState::new(1, 1, Duration::from_secs(60), Duration::from_millis(50));
        state.register_watcher("gmail/INBOX");
        state.register_command_runner("gmail/INBOX");
        state
    }

    #[test]
    fn status_message_has_only_counts_and_generic_state() {
        let state = healthy_state();
        let status = state.status_message();
        assert!(status.contains("watching 1 account(s), 1 mailbox(es)"));
        assert!(!status.contains("user"));
        assert!(!status.contains("gmail-oauth-token"));
        assert!(!status.contains("gmi sync"));
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
        let state = RuntimeState::new(1, 1, Duration::from_secs(60), Duration::from_millis(50));
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
}
