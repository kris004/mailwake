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
    inner: Mutex<RuntimeInner>,
}

#[derive(Debug)]
struct RuntimeInner {
    watchers: HashMap<String, WatcherStatus>,
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

#[derive(Debug)]
struct WatcherStatus {
    phase: WatcherPhase,
    last_progress: Instant,
}

impl RuntimeState {
    pub fn new(total_accounts: usize, total_mailboxes: usize, watcher_stale: Duration) -> Self {
        Self {
            total_accounts,
            total_mailboxes,
            watcher_stale,
            inner: Mutex::new(RuntimeInner {
                watchers: HashMap::new(),
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_message_has_only_counts_and_generic_state() {
        let state = RuntimeState::new(1, 1, Duration::from_secs(60));
        state.register_watcher("gmail/INBOX");
        let status = state.status_message();
        assert!(status.contains("watching 1 account(s), 1 mailbox(es)"));
        assert!(!status.contains("user"));
        assert!(!status.contains("gmail-oauth-token"));
        assert!(!status.contains("gmi sync"));
    }

    #[test]
    fn crashed_watcher_is_unhealthy() {
        let state = RuntimeState::new(1, 1, Duration::from_secs(60));
        state.register_watcher("gmail/INBOX");
        assert!(state.is_healthy());
        state.mark_watcher("gmail/INBOX", WatcherPhase::Crashed);
        assert!(!state.is_healthy());
    }
}
