use crate::config::FsStateSourceConfig;
use crate::lane::{CommandLifecycle, CommandRunReport, CommandTriggerError, CommandTriggerTarget};
use crate::process::{ShellProcessError, ShellRun, run_shell_process};
use crate::state::{RuntimeState, WatcherPhase};
use notify::{Config as NotifyConfig, RecommendedWatcher, RecursiveMode, Watcher};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Instant, sleep, sleep_until};
use tracing::{debug, error, info, warn};

const STATE_CMD_MAX_OUTPUT_BYTES: usize = 1024 * 1024;
const SELF_SETTLE_AFTER_COMMAND: Duration = Duration::from_secs(1);

pub type StateReadFuture = Pin<Box<dyn Future<Output = Result<String, StateReadError>> + Send>>;

pub trait StateReader: Send + Sync {
    fn read(&self, shutdown: watch::Receiver<bool>) -> StateReadFuture;
}

#[derive(Debug, Error)]
pub enum StateReadError {
    #[error("state_cmd could not be started: {source}")]
    Start {
        #[source]
        source: std::io::Error,
    },
    #[error("state_cmd could not be waited on: {source}")]
    Wait {
        #[source]
        source: std::io::Error,
    },
    #[error("state_cmd output could not be collected: {source}")]
    Output {
        #[source]
        source: std::io::Error,
    },
    #[error("state_cmd failed with {status}")]
    Failed { status: String },
    #[error("state_cmd timed out after {seconds} seconds")]
    TimedOut { seconds: u64 },
    #[error("state_cmd was cancelled by shutdown")]
    Cancelled,
    #[error("state_cmd wrote more than {limit} bytes to stdout")]
    OutputTooLarge { limit: usize },
    #[error("state_cmd wrote non-UTF-8 stdout")]
    Utf8,
}

#[derive(Clone)]
pub struct ShellStateReader {
    command: Arc<str>,
    timeout: Duration,
}

impl ShellStateReader {
    pub fn new(command: impl Into<Arc<str>>, timeout: Duration) -> Self {
        Self {
            command: command.into(),
            timeout,
        }
    }
}

impl StateReader for ShellStateReader {
    fn read(&self, shutdown: watch::Receiver<bool>) -> StateReadFuture {
        let command = Arc::clone(&self.command);
        let timeout = self.timeout;
        Box::pin(async move {
            let run = run_shell_process(
                &command,
                true,
                false,
                timeout,
                Some(STATE_CMD_MAX_OUTPUT_BYTES),
                Some(shutdown),
            )
            .await
            .map_err(StateReadError::from)?;

            match run {
                ShellRun::Completed(output) => {
                    if !output.status.success() {
                        return Err(StateReadError::Failed {
                            status: describe_status(output.status),
                        });
                    }
                    let stdout =
                        String::from_utf8(output.stdout).map_err(|_| StateReadError::Utf8)?;
                    Ok(trim_trailing_crlf(&stdout).to_string())
                }
                ShellRun::TimedOut => Err(StateReadError::TimedOut {
                    seconds: timeout.as_secs(),
                }),
                ShellRun::Cancelled => Err(StateReadError::Cancelled),
                ShellRun::OutputLimitExceeded(exceeded) => Err(StateReadError::OutputTooLarge {
                    limit: exceeded.limit,
                }),
            }
        })
    }
}

impl From<ShellProcessError> for StateReadError {
    fn from(error: ShellProcessError) -> Self {
        match error {
            ShellProcessError::Start { source } => Self::Start { source },
            ShellProcessError::Wait { source } => Self::Wait { source },
            ShellProcessError::Output { source } => Self::Output { source },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FsStateEvent {
    Changed,
    Overflow,
}

pub struct FsStateRunner {
    source: String,
    debounce: Duration,
    max_debounce: Duration,
    self_settle: Duration,
    state_reader: Option<Arc<dyn StateReader>>,
    trigger: Arc<dyn CommandTriggerTarget>,
    state: Option<Arc<RuntimeState>>,
}

impl FsStateRunner {
    pub fn new(
        source: impl Into<String>,
        debounce: Duration,
        max_debounce: Duration,
        state_reader: Option<Arc<dyn StateReader>>,
        trigger: Arc<dyn CommandTriggerTarget>,
        state: Option<Arc<RuntimeState>>,
    ) -> Self {
        Self::new_with_settle(
            source,
            debounce,
            max_debounce,
            SELF_SETTLE_AFTER_COMMAND,
            state_reader,
            trigger,
            state,
        )
    }

    pub fn new_with_settle(
        source: impl Into<String>,
        debounce: Duration,
        max_debounce: Duration,
        self_settle: Duration,
        state_reader: Option<Arc<dyn StateReader>>,
        trigger: Arc<dyn CommandTriggerTarget>,
        state: Option<Arc<RuntimeState>>,
    ) -> Self {
        Self {
            source: source.into(),
            debounce,
            max_debounce,
            self_settle,
            state_reader,
            trigger,
            state,
        }
    }

    pub async fn run(
        self,
        mut events: mpsc::Receiver<FsStateEvent>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let mut baseline = match &self.state_reader {
            Some(reader) => match reader.read(shutdown.clone()).await {
                Ok(state) => {
                    debug!(
                        source = %self.source,
                        state_bytes = state.len(),
                        "captured fs_state startup baseline"
                    );
                    Some(state)
                }
                Err(StateReadError::Cancelled) => return,
                Err(error) => {
                    warn!(
                        source = %self.source,
                        %error,
                        "failed to capture fs_state startup baseline; will retry on change"
                    );
                    None
                }
            },
            None => None,
        };

        loop {
            let Some(first_event_at) = self.recv_event(&mut events, &mut shutdown).await else {
                break;
            };
            let mut batch = EventBatch::new(first_event_at);

            loop {
                match self
                    .wait_for_settled_batch(&mut events, &mut shutdown, &mut batch)
                    .await
                {
                    BatchWait::CheckNow => {}
                    BatchWait::Shutdown | BatchWait::Closed => return,
                }

                match self
                    .process_dirty_batch(&mut baseline, &mut events, &mut shutdown)
                    .await
                {
                    OperationResult::Shutdown => return,
                    OperationResult::Idle => break,
                    OperationResult::DirtyAgain(dirty) => {
                        batch = dirty.into_batch();
                    }
                }
            }
        }
    }

    async fn recv_event(
        &self,
        events: &mut mpsc::Receiver<FsStateEvent>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Option<Instant> {
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_ok() && *shutdown.borrow() {
                        return None;
                    }
                }
                event = events.recv() => {
                    let event = event?;
                    self.record_event(event);
                    return Some(Instant::now());
                }
            }
        }
    }

    async fn wait_for_settled_batch(
        &self,
        events: &mut mpsc::Receiver<FsStateEvent>,
        shutdown: &mut watch::Receiver<bool>,
        batch: &mut EventBatch,
    ) -> BatchWait {
        loop {
            let due = std::cmp::min(batch.last + self.debounce, batch.first + self.max_debounce);
            let now = Instant::now();
            if now >= due {
                return BatchWait::CheckNow;
            }

            let sleep = sleep_until(due);
            tokio::pin!(sleep);
            tokio::select! {
                () = &mut sleep => return BatchWait::CheckNow,
                changed = shutdown.changed() => {
                    if changed.is_ok() && *shutdown.borrow() {
                        return BatchWait::Shutdown;
                    }
                }
                event = events.recv() => {
                    let Some(event) = event else {
                        return BatchWait::Closed;
                    };
                    self.record_event(event);
                    batch.last = Instant::now();
                }
            }
        }
    }

    async fn process_dirty_batch(
        &self,
        baseline: &mut Option<String>,
        events: &mut mpsc::Receiver<FsStateEvent>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> OperationResult {
        let should_trigger = match &self.state_reader {
            Some(reader) => {
                let (state, dirty) = match self
                    .read_state_collecting_events(reader, events, shutdown, false)
                    .await
                {
                    ReadOperation::Read { state, dirty } => (state, dirty),
                    ReadOperation::Shutdown => return OperationResult::Shutdown,
                    ReadOperation::Failed { dirty } => return dirty.into_operation_result(),
                };

                if let Some(previous) = baseline.as_ref() {
                    if previous == &state {
                        return dirty.into_operation_result();
                    }
                    true
                } else {
                    debug!(
                        source = %self.source,
                        state_bytes = state.len(),
                        "captured fs_state baseline after prior state_cmd failure"
                    );
                    *baseline = Some(state);
                    return dirty.into_operation_result();
                }
            }
            None => true,
        };

        if !should_trigger {
            return OperationResult::Idle;
        }

        let dirty = match self.trigger_collecting_events(events, shutdown).await {
            TriggerOperation::Finished { dirty } => dirty,
            TriggerOperation::Shutdown => return OperationResult::Shutdown,
            TriggerOperation::Failed { dirty } => return dirty.into_operation_result(),
        };

        if !self
            .settle_after_own_command(events, shutdown, self.self_settle)
            .await
        {
            return OperationResult::Shutdown;
        }

        if let Some(reader) = &self.state_reader {
            match self
                .read_state_collecting_events(reader, events, shutdown, true)
                .await
            {
                ReadOperation::Read { state, .. } => {
                    debug!(
                        source = %self.source,
                        state_bytes = state.len(),
                        "rebaselined fs_state after command"
                    );
                    *baseline = Some(state);
                }
                ReadOperation::Failed { .. } => {
                    warn!(
                        source = %self.source,
                        "failed to rebaseline fs_state after command; keeping previous baseline"
                    );
                }
                ReadOperation::Shutdown => return OperationResult::Shutdown,
            }
        }

        dirty.into_operation_result()
    }

    async fn read_state_collecting_events(
        &self,
        reader: &Arc<dyn StateReader>,
        events: &mut mpsc::Receiver<FsStateEvent>,
        shutdown: &mut watch::Receiver<bool>,
        suppress_events: bool,
    ) -> ReadOperation {
        let read = reader.read(shutdown.clone());
        tokio::pin!(read);
        let mut dirty = DirtyTracker::default();
        let mut events_open = true;

        loop {
            tokio::select! {
                result = &mut read => {
                    return match result {
                        Ok(state) => ReadOperation::Read { state, dirty },
                        Err(StateReadError::Cancelled) => ReadOperation::Shutdown,
                        Err(error) => {
                            warn!(
                                source = %self.source,
                                %error,
                                "fs_state state_cmd failed"
                            );
                            ReadOperation::Failed { dirty }
                        }
                    };
                }
                changed = shutdown.changed() => {
                    if changed.is_ok() && *shutdown.borrow() {
                        return ReadOperation::Shutdown;
                    }
                }
                event = events.recv(), if events_open => {
                    match event {
                        Some(event) => {
                            self.record_event(event);
                            if suppress_events {
                                debug!(
                                    source = %self.source,
                                    "suppressing fs_state event during own-command rebaseline"
                                );
                            } else {
                                dirty.note_now();
                            }
                        }
                        None => events_open = false,
                    }
                }
            }
        }
    }

    async fn trigger_collecting_events(
        &self,
        events: &mut mpsc::Receiver<FsStateEvent>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> TriggerOperation {
        let start = self.trigger.start();
        tokio::pin!(start);
        let mut dirty = DirtyTracker::default();
        let mut events_open = true;

        let mut lifecycle = loop {
            tokio::select! {
                result = &mut start => {
                    match result {
                        Ok(lifecycle) => break lifecycle,
                        Err(error) => {
                            warn!(
                                source = %self.source,
                                %error,
                                "fs_state command could not be submitted"
                            );
                            return TriggerOperation::Failed { dirty };
                        }
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_ok() && *shutdown.borrow() {
                        return TriggerOperation::Shutdown;
                    }
                }
                event = events.recv(), if events_open => {
                    match event {
                        Some(event) => {
                            self.record_event(event);
                            dirty.note_now();
                        }
                        None => events_open = false,
                    }
                }
            }
        };

        let mut own_command_running = false;
        loop {
            tokio::select! {
                lifecycle_event = lifecycle.recv() => {
                    match lifecycle_event {
                        Some(CommandLifecycle::Started) => {
                            own_command_running = true;
                        }
                        Some(CommandLifecycle::Finished(report)) => {
                            if matches!(report, CommandRunReport::Error(_) | CommandRunReport::Cancelled) {
                                warn!(
                                    source = %self.source,
                                    status = %report.description(),
                                    "fs_state command did not complete successfully"
                                );
                            }
                            return TriggerOperation::Finished { dirty };
                        }
                        None => {
                            warn!(
                                source = %self.source,
                                error = %CommandTriggerError::CompletionLost,
                                "fs_state command lifecycle ended before completion"
                            );
                            return TriggerOperation::Failed { dirty };
                        }
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_ok() && *shutdown.borrow() {
                        return TriggerOperation::Shutdown;
                    }
                }
                event = events.recv(), if events_open => {
                    match event {
                        Some(event) => {
                            self.record_event(event);
                            if own_command_running {
                                debug!(
                                    source = %self.source,
                                    "suppressing fs_state event while own command is running"
                                );
                            } else {
                                dirty.note_now();
                            }
                        }
                        None => events_open = false,
                    }
                }
            }
        }
    }

    async fn settle_after_own_command(
        &self,
        events: &mut mpsc::Receiver<FsStateEvent>,
        shutdown: &mut watch::Receiver<bool>,
        settle: Duration,
    ) -> bool {
        if settle.is_zero() {
            return true;
        }
        let sleep = sleep(settle);
        tokio::pin!(sleep);
        let mut events_open = true;

        loop {
            tokio::select! {
                () = &mut sleep => return true,
                changed = shutdown.changed() => {
                    if changed.is_ok() && *shutdown.borrow() {
                        return false;
                    }
                }
                event = events.recv(), if events_open => {
                    match event {
                        Some(event) => {
                            self.record_event(event);
                            debug!(
                                source = %self.source,
                                "suppressing fs_state event during own-command settle period"
                            );
                        }
                        None => events_open = false,
                    }
                }
            }
        }
    }

    fn record_event(&self, event: FsStateEvent) {
        if let Some(state) = &self.state {
            state.mark_event();
        }
        match event {
            FsStateEvent::Changed => {}
            FsStateEvent::Overflow => {
                warn!(
                    source = %self.source,
                    "filesystem watcher reported an overflow/error; treating source as dirty"
                );
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn watch_fs_state_forever(
    source: FsStateSourceConfig,
    runner: FsStateRunner,
    events_tx: mpsc::Sender<FsStateEvent>,
    events_rx: mpsc::Receiver<FsStateEvent>,
    state: Arc<RuntimeState>,
    watcher_id: String,
    initial_ready: Option<oneshot::Sender<()>>,
    shutdown: watch::Receiver<bool>,
) {
    state.mark_watcher(&watcher_id, WatcherPhase::Connecting);
    match build_watcher(&source, events_tx) {
        Ok(_watcher) => {
            state.mark_watcher(&watcher_id, WatcherPhase::Idling);
            if let Some(sender) = initial_ready {
                let _ = sender.send(());
            }
            info!(
                source = %source.name,
                paths = source.watch_paths.len(),
                recursive = source.recursive(),
                "fs_state watcher started"
            );
            runner.run(events_rx, shutdown).await;
            state.mark_watcher(&watcher_id, WatcherPhase::Stopped);
            info!(source = %source.name, "fs_state watcher stopped");
        }
        Err(error) => {
            state.mark_watcher(&watcher_id, WatcherPhase::Crashed);
            error!(
                source = %source.name,
                %error,
                "failed to start fs_state watcher"
            );
        }
    }
}

fn build_watcher(
    source: &FsStateSourceConfig,
    events: mpsc::Sender<FsStateEvent>,
) -> notify::Result<RecommendedWatcher> {
    let source_name = source.name.clone();
    let callback_events = events.clone();
    let mut watcher = RecommendedWatcher::new(
        move |result| {
            let event = match result {
                Ok(_) => FsStateEvent::Changed,
                Err(error) => {
                    warn!(
                        source = %source_name,
                        %error,
                        "filesystem watcher error; treating source as dirty"
                    );
                    FsStateEvent::Overflow
                }
            };
            match callback_events.try_send(event) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    debug!(
                        source = %source_name,
                        "fs_state event queue is full; coalescing filesystem event"
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {}
            }
        },
        NotifyConfig::default(),
    )?;
    let recursive_mode = if source.recursive() {
        RecursiveMode::Recursive
    } else {
        RecursiveMode::NonRecursive
    };
    for path in &source.watch_paths {
        watcher.watch(path, recursive_mode)?;
    }
    Ok(watcher)
}

#[derive(Clone, Copy, Debug)]
struct EventBatch {
    first: Instant,
    last: Instant,
}

impl EventBatch {
    fn new(first: Instant) -> Self {
        Self { first, last: first }
    }
}

#[derive(Default)]
struct DirtyTracker {
    first: Option<Instant>,
    last: Option<Instant>,
}

impl DirtyTracker {
    fn note_now(&mut self) {
        let now = Instant::now();
        if self.first.is_none() {
            self.first = Some(now);
        }
        self.last = Some(now);
    }

    fn into_operation_result(self) -> OperationResult {
        match self.first {
            Some(_) => OperationResult::DirtyAgain(self),
            None => OperationResult::Idle,
        }
    }

    fn into_batch(self) -> EventBatch {
        let first = self
            .first
            .expect("dirty tracker converted to batch without a first event");
        EventBatch {
            first,
            last: self.last.unwrap_or(first),
        }
    }
}

enum BatchWait {
    CheckNow,
    Shutdown,
    Closed,
}

enum ReadOperation {
    Read { state: String, dirty: DirtyTracker },
    Failed { dirty: DirtyTracker },
    Shutdown,
}

enum TriggerOperation {
    Finished { dirty: DirtyTracker },
    Failed { dirty: DirtyTracker },
    Shutdown,
}

enum OperationResult {
    Idle,
    DirtyAgain(DirtyTracker),
    Shutdown,
}

fn trim_trailing_crlf(value: &str) -> &str {
    value.trim_end_matches(['\r', '\n'])
}

fn describe_status(status: std::process::ExitStatus) -> String {
    if let Some(code) = status.code() {
        return format!("exit status {code}");
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            return format!("signal {signal}");
        }
    }
    "unknown status".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::CommandOutcome;
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::timeout;

    #[derive(Clone)]
    struct TestStateReader {
        calls: Arc<AtomicUsize>,
        states: Arc<Mutex<VecDeque<String>>>,
        last: Arc<Mutex<String>>,
        delay: Duration,
    }

    impl TestStateReader {
        fn new(states: &[&str]) -> Self {
            Self::new_with_delay(states, Duration::ZERO)
        }

        fn new_with_delay(states: &[&str], delay: Duration) -> Self {
            let last = states.last().copied().unwrap_or_default().to_string();
            Self {
                calls: Arc::new(AtomicUsize::new(0)),
                states: Arc::new(Mutex::new(
                    states.iter().map(|state| (*state).to_string()).collect(),
                )),
                last: Arc::new(Mutex::new(last)),
                delay,
            }
        }
    }

    impl StateReader for TestStateReader {
        fn read(&self, _shutdown: watch::Receiver<bool>) -> StateReadFuture {
            let this = self.clone();
            Box::pin(async move {
                this.calls.fetch_add(1, Ordering::SeqCst);
                if !this.delay.is_zero() {
                    sleep(this.delay).await;
                }
                let mut states = this.states.lock().unwrap();
                let state = states.pop_front().unwrap_or_else(|| {
                    this.last.lock().expect("last state mutex poisoned").clone()
                });
                *this.last.lock().expect("last state mutex poisoned") = state.clone();
                Ok(state)
            })
        }
    }

    #[derive(Clone)]
    struct TestTrigger {
        calls: Arc<AtomicUsize>,
        delay: Duration,
        event_during_run: Option<mpsc::Sender<FsStateEvent>>,
    }

    impl TestTrigger {
        fn new(delay: Duration) -> Self {
            Self {
                calls: Arc::new(AtomicUsize::new(0)),
                delay,
                event_during_run: None,
            }
        }
    }

    impl CommandTriggerTarget for TestTrigger {
        fn start(&self) -> crate::lane::CommandTriggerStartFuture {
            let this = self.clone();
            Box::pin(async move {
                let (lifecycle_tx, lifecycle_rx) = mpsc::unbounded_channel();
                tokio::spawn(async move {
                    this.calls.fetch_add(1, Ordering::SeqCst);
                    let _ = lifecycle_tx.send(CommandLifecycle::Started);
                    if let Some(events) = this.event_during_run {
                        let _ = events.send(FsStateEvent::Changed).await;
                    }
                    if !this.delay.is_zero() {
                        sleep(this.delay).await;
                    }
                    let _ = lifecycle_tx.send(CommandLifecycle::Finished(
                        CommandRunReport::Outcome(CommandOutcome::success()),
                    ));
                });
                Ok(lifecycle_rx)
            })
        }
    }

    impl CommandOutcome {
        fn success() -> Self {
            Self {
                success: true,
                code: Some(0),
                signal: None,
                timed_out: false,
                cancelled: false,
                timeout: None,
            }
        }
    }

    fn spawn_test_runner(
        reader: Option<Arc<TestStateReader>>,
        trigger: Arc<TestTrigger>,
        debounce: Duration,
        max_debounce: Duration,
    ) -> (
        mpsc::Sender<FsStateEvent>,
        watch::Sender<bool>,
        tokio::task::JoinHandle<()>,
    ) {
        let (events_tx, events_rx) = mpsc::channel(64);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let runner = FsStateRunner::new_with_settle(
            "local",
            debounce,
            max_debounce,
            Duration::ZERO,
            reader.map(|reader| reader as Arc<dyn StateReader>),
            trigger,
            None,
        );
        let handle = tokio::spawn(runner.run(events_rx, shutdown_rx));
        (events_tx, shutdown_tx, handle)
    }

    async fn wait_for_calls(calls: &AtomicUsize, expected: usize) {
        timeout(Duration::from_secs(2), async {
            while calls.load(Ordering::SeqCst) < expected {
                sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("timed out waiting for calls");
    }

    #[tokio::test]
    async fn startup_baseline_captures_state_cmd_output() {
        let reader = Arc::new(TestStateReader::new(&["baseline\n"]));
        let trigger = Arc::new(TestTrigger::new(Duration::ZERO));
        let (_events, shutdown, handle) = spawn_test_runner(
            Some(Arc::clone(&reader)),
            Arc::clone(&trigger),
            Duration::from_millis(5),
            Duration::from_millis(50),
        );

        wait_for_calls(&reader.calls, 1).await;
        assert_eq!(trigger.calls.load(Ordering::SeqCst), 0);
        let _ = shutdown.send(true);
        handle.await.unwrap();
    }

    #[test]
    fn state_cmd_output_trims_trailing_newlines() {
        assert_eq!(trim_trailing_crlf("version-1\r\n\n"), "version-1");
        assert_eq!(trim_trailing_crlf("version-1\nmiddle"), "version-1\nmiddle");
    }

    #[tokio::test]
    async fn many_filesystem_events_cause_one_state_cmd_after_debounce() {
        let reader = Arc::new(TestStateReader::new(&["base", "base"]));
        let trigger = Arc::new(TestTrigger::new(Duration::ZERO));
        let (events, shutdown, handle) = spawn_test_runner(
            Some(Arc::clone(&reader)),
            Arc::clone(&trigger),
            Duration::from_millis(30),
            Duration::from_millis(200),
        );
        wait_for_calls(&reader.calls, 1).await;
        for _ in 0..20 {
            events.send(FsStateEvent::Changed).await.unwrap();
        }

        wait_for_calls(&reader.calls, 2).await;
        sleep(Duration::from_millis(80)).await;
        assert_eq!(reader.calls.load(Ordering::SeqCst), 2);
        assert_eq!(trigger.calls.load(Ordering::SeqCst), 0);
        let _ = shutdown.send(true);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn max_debounce_forces_state_check_during_continuous_events() {
        let reader = Arc::new(TestStateReader::new(&["base", "changed", "changed"]));
        let trigger = Arc::new(TestTrigger::new(Duration::ZERO));
        let (events, shutdown, handle) = spawn_test_runner(
            Some(Arc::clone(&reader)),
            Arc::clone(&trigger),
            Duration::from_millis(80),
            Duration::from_millis(60),
        );
        wait_for_calls(&reader.calls, 1).await;

        let sender = tokio::spawn(async move {
            for _ in 0..10 {
                events.send(FsStateEvent::Changed).await.unwrap();
                sleep(Duration::from_millis(15)).await;
            }
        });
        wait_for_calls(&trigger.calls, 1).await;
        sender.await.unwrap();

        let _ = shutdown.send(true);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn unchanged_state_does_not_trigger_command() {
        let reader = Arc::new(TestStateReader::new(&["base", "base"]));
        let trigger = Arc::new(TestTrigger::new(Duration::ZERO));
        let (events, shutdown, handle) = spawn_test_runner(
            Some(Arc::clone(&reader)),
            Arc::clone(&trigger),
            Duration::from_millis(5),
            Duration::from_millis(50),
        );
        wait_for_calls(&reader.calls, 1).await;
        events.send(FsStateEvent::Changed).await.unwrap();
        wait_for_calls(&reader.calls, 2).await;
        sleep(Duration::from_millis(30)).await;
        assert_eq!(trigger.calls.load(Ordering::SeqCst), 0);
        let _ = shutdown.send(true);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn changed_state_triggers_command_and_rebaseline() {
        let reader = Arc::new(TestStateReader::new(&["base", "changed", "after"]));
        let trigger = Arc::new(TestTrigger::new(Duration::ZERO));
        let (events, shutdown, handle) = spawn_test_runner(
            Some(Arc::clone(&reader)),
            Arc::clone(&trigger),
            Duration::from_millis(5),
            Duration::from_millis(50),
        );
        wait_for_calls(&reader.calls, 1).await;
        events.send(FsStateEvent::Changed).await.unwrap();
        wait_for_calls(&trigger.calls, 1).await;
        wait_for_calls(&reader.calls, 3).await;
        assert_eq!(trigger.calls.load(Ordering::SeqCst), 1);
        let _ = shutdown.send(true);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn after_command_rebaseline_prevents_repeat_for_same_state() {
        let reader = Arc::new(TestStateReader::new(&["base", "changed", "after", "after"]));
        let trigger = Arc::new(TestTrigger::new(Duration::ZERO));
        let (events, shutdown, handle) = spawn_test_runner(
            Some(Arc::clone(&reader)),
            Arc::clone(&trigger),
            Duration::from_millis(5),
            Duration::from_millis(50),
        );
        wait_for_calls(&reader.calls, 1).await;
        events.send(FsStateEvent::Changed).await.unwrap();
        wait_for_calls(&reader.calls, 3).await;
        events.send(FsStateEvent::Changed).await.unwrap();
        wait_for_calls(&reader.calls, 4).await;
        sleep(Duration::from_millis(30)).await;
        assert_eq!(trigger.calls.load(Ordering::SeqCst), 1);
        let _ = shutdown.send(true);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn own_command_events_do_not_self_trigger() {
        let reader = Arc::new(TestStateReader::new(&["base", "changed", "after"]));
        let (events_tx, events_rx) = mpsc::channel(64);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let trigger = Arc::new(TestTrigger {
            calls: Arc::new(AtomicUsize::new(0)),
            delay: Duration::from_millis(30),
            event_during_run: Some(events_tx.clone()),
        });
        let runner = FsStateRunner::new_with_settle(
            "local",
            Duration::from_millis(5),
            Duration::from_millis(50),
            Duration::ZERO,
            Some(reader.clone() as Arc<dyn StateReader>),
            trigger.clone(),
            None,
        );
        let handle = tokio::spawn(runner.run(events_rx, shutdown_rx));
        wait_for_calls(&reader.calls, 1).await;
        events_tx.send(FsStateEvent::Changed).await.unwrap();
        wait_for_calls(&reader.calls, 3).await;
        sleep(Duration::from_millis(80)).await;
        assert_eq!(trigger.calls.load(Ordering::SeqCst), 1);
        let _ = shutdown_tx.send(true);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn events_during_state_cmd_are_processed_once_afterward() {
        let reader = Arc::new(TestStateReader::new_with_delay(
            &["base", "base", "changed", "after"],
            Duration::from_millis(40),
        ));
        let trigger = Arc::new(TestTrigger::new(Duration::ZERO));
        let (events, shutdown, handle) = spawn_test_runner(
            Some(Arc::clone(&reader)),
            Arc::clone(&trigger),
            Duration::from_millis(5),
            Duration::from_millis(100),
        );
        wait_for_calls(&reader.calls, 1).await;
        events.send(FsStateEvent::Changed).await.unwrap();
        wait_for_calls(&reader.calls, 2).await;
        events.send(FsStateEvent::Changed).await.unwrap();
        wait_for_calls(&trigger.calls, 1).await;
        wait_for_calls(&reader.calls, 4).await;
        assert_eq!(trigger.calls.load(Ordering::SeqCst), 1);
        let _ = shutdown.send(true);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn watcher_overflow_is_treated_as_dirty() {
        let reader = Arc::new(TestStateReader::new(&["base", "changed", "after"]));
        let trigger = Arc::new(TestTrigger::new(Duration::ZERO));
        let (events, shutdown, handle) = spawn_test_runner(
            Some(Arc::clone(&reader)),
            Arc::clone(&trigger),
            Duration::from_millis(5),
            Duration::from_millis(50),
        );
        wait_for_calls(&reader.calls, 1).await;
        events.send(FsStateEvent::Overflow).await.unwrap();
        wait_for_calls(&trigger.calls, 1).await;
        let _ = shutdown.send(true);
        handle.await.unwrap();
    }
}
