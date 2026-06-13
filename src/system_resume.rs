use crate::config::SystemResumeSourceConfig;
use crate::lane::{CommandRunReport, CommandTriggerTarget, await_trigger_finished};
use crate::state::{RuntimeState, WatcherPhase};
use futures_util::{Stream, StreamExt};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, sleep_until, timeout};
use tracing::{debug, error, info, warn};
use zbus::{Connection, Proxy};

const DBUS_SETUP_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResumeSignal {
    Suspending,
    Resumed,
}

#[derive(Debug, Error)]
pub enum SystemResumeError {
    #[error("could not connect to system D-Bus/logind: {0}")]
    Connect(#[source] zbus::Error),
    #[error("could not subscribe to logind PrepareForSleep: {0}")]
    Subscribe(#[source] zbus::Error),
    #[error("could not read logind PrepareForSleep signal body: {0}")]
    SignalBody(#[source] zbus::Error),
    #[error("timed out after {0:?} while connecting to system D-Bus/logind")]
    SetupTimeout(Duration),
}

pub type ResumeEventFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<ResumeSignal>, SystemResumeError>> + Send + 'a>>;

pub trait ResumeEventSource: Send {
    fn next_event(&mut self) -> ResumeEventFuture<'_>;
}

struct DbusResumeEventSource {
    stream: Pin<Box<dyn Stream<Item = zbus::Message> + Send>>,
}

impl DbusResumeEventSource {
    async fn connect() -> Result<Self, SystemResumeError> {
        let connection = Connection::system()
            .await
            .map_err(SystemResumeError::Connect)?;
        let proxy = Proxy::new(
            &connection,
            "org.freedesktop.login1",
            "/org/freedesktop/login1",
            "org.freedesktop.login1.Manager",
        )
        .await
        .map_err(SystemResumeError::Connect)?;
        let stream = proxy
            .receive_signal("PrepareForSleep")
            .await
            .map_err(SystemResumeError::Subscribe)?;
        Ok(Self {
            stream: Box::pin(stream),
        })
    }
}

impl ResumeEventSource for DbusResumeEventSource {
    fn next_event(&mut self) -> ResumeEventFuture<'_> {
        Box::pin(async move {
            let Some(message) = self.stream.next().await else {
                return Ok(None);
            };
            let sleeping = message
                .body()
                .deserialize::<bool>()
                .map_err(SystemResumeError::SignalBody)?;
            Ok(Some(if sleeping {
                ResumeSignal::Suspending
            } else {
                ResumeSignal::Resumed
            }))
        })
    }
}

pub struct SystemResumeRunner {
    source: String,
    settle: Duration,
    trigger: Arc<dyn CommandTriggerTarget>,
    state: Option<Arc<RuntimeState>>,
}

impl SystemResumeRunner {
    pub fn new(
        source: impl Into<String>,
        settle: Duration,
        trigger: Arc<dyn CommandTriggerTarget>,
        state: Option<Arc<RuntimeState>>,
    ) -> Self {
        Self {
            source: source.into(),
            settle,
            trigger,
            state,
        }
    }

    pub async fn run(self, mut events: mpsc::Receiver<()>, mut shutdown: watch::Receiver<bool>) {
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
                if !self.wait_for_quiet(&mut events, &mut shutdown).await {
                    return;
                }
                if !self.run_once(&mut shutdown).await {
                    return;
                }
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

    fn drain_events(&self, events: &mut mpsc::Receiver<()>) -> bool {
        let mut saw_event = false;
        while let Ok(()) = events.try_recv() {
            saw_event = true;
            self.mark_event();
        }
        saw_event
    }

    async fn wait_for_quiet(
        &self,
        events: &mut mpsc::Receiver<()>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> bool {
        if self.settle.is_zero() {
            self.drain_events(events);
            return true;
        }

        let mut settle_until = Instant::now() + self.settle;
        loop {
            let sleep = sleep_until(settle_until);
            tokio::pin!(sleep);
            tokio::select! {
                () = &mut sleep => return true,
                event = events.recv() => {
                    if event.is_some() {
                        self.mark_event();
                        settle_until = Instant::now() + self.settle;
                    } else {
                        return true;
                    }
                }
                changed = shutdown.changed() => {
                    return !(changed.is_ok() && *shutdown.borrow());
                }
            }
        }
    }

    async fn run_once(&self, shutdown: &mut watch::Receiver<bool>) -> bool {
        info!(
            source = %self.source,
            "submitting system_resume command"
        );
        let start = self.trigger.start();
        tokio::pin!(start);
        let lifecycle = tokio::select! {
            result = &mut start => result,
            changed = shutdown.changed() => {
                return !(changed.is_ok() && *shutdown.borrow());
            }
        };
        match lifecycle {
            Ok(lifecycle) => {
                let finished = await_trigger_finished(lifecycle);
                tokio::pin!(finished);
                tokio::select! {
                    result = &mut finished => {
                        match result {
                            Ok(report) => self.record_outcome(report),
                            Err(error) => warn!(
                                source = %self.source,
                                %error,
                                "system_resume command did not report completion"
                            ),
                        }
                        true
                    }
                    changed = shutdown.changed() => {
                        !(changed.is_ok() && *shutdown.borrow())
                    }
                }
            }
            Err(error) => {
                error!(
                    source = %self.source,
                    %error,
                    "system_resume command could not be submitted"
                );
                true
            }
        }
    }

    fn record_outcome(&self, report: CommandRunReport) {
        if report.success() {
            info!(
                source = %self.source,
                status = %report.description(),
                "system_resume command succeeded"
            );
        } else {
            warn!(
                source = %self.source,
                status = %report.description(),
                "system_resume command failed"
            );
        }
    }
}

pub struct SystemResumeWatcherTask {
    pub source: SystemResumeSourceConfig,
    pub events_tx: mpsc::Sender<()>,
    pub state: Arc<RuntimeState>,
    pub watcher_id: String,
    pub initial_ready: Option<oneshot::Sender<Result<(), String>>>,
    pub shutdown: watch::Receiver<bool>,
}

pub async fn watch_system_resume_forever(task: SystemResumeWatcherTask) {
    task.state
        .mark_watcher(&task.watcher_id, WatcherPhase::Connecting);
    match timeout(DBUS_SETUP_TIMEOUT, DbusResumeEventSource::connect()).await {
        Ok(Ok(listener)) => watch_system_resume_with_listener(task, listener).await,
        Ok(Err(error)) => report_system_resume_setup_error(task, error),
        Err(_) => report_system_resume_setup_error(
            task,
            SystemResumeError::SetupTimeout(DBUS_SETUP_TIMEOUT),
        ),
    }
}

fn report_system_resume_setup_error(task: SystemResumeWatcherTask, error: SystemResumeError) {
    let error_text = error.to_string();
    if let Some(sender) = task.initial_ready {
        let _ = sender.send(Err(format!(
            "system_resume source {:?} setup failed: {error_text}",
            task.source.name
        )));
    }
    task.state
        .mark_watcher(&task.watcher_id, WatcherPhase::Crashed);
    error!(
        source = %task.source.name,
        %error,
        "failed to start system_resume source"
    );
}

async fn watch_system_resume_with_listener<L>(mut task: SystemResumeWatcherTask, mut listener: L)
where
    L: ResumeEventSource,
{
    task.state
        .mark_watcher(&task.watcher_id, WatcherPhase::Idling);
    if let Some(sender) = task.initial_ready.take() {
        let _ = sender.send(Ok(()));
    }
    let heartbeat = spawn_watcher_progress_heartbeat(
        Arc::clone(&task.state),
        task.watcher_id.clone(),
        task.shutdown.clone(),
    );
    info!(source = %task.source.name, "system_resume source started");

    loop {
        tokio::select! {
            event = listener.next_event() => {
                task.state.mark_watcher_progress(&task.watcher_id);
                match event {
                    Ok(Some(ResumeSignal::Suspending)) => {
                        debug!(source = %task.source.name, "system is preparing for sleep");
                    }
                    Ok(Some(ResumeSignal::Resumed)) => {
                        task.state.mark_event();
                        match task.events_tx.try_send(()) {
                            Ok(()) => {
                                info!(source = %task.source.name, "queued system_resume event");
                            }
                            Err(mpsc::error::TrySendError::Full(())) => {
                                debug!(source = %task.source.name, "system_resume event queue is full; coalescing event");
                            }
                            Err(mpsc::error::TrySendError::Closed(())) => {
                                warn!(source = %task.source.name, "system_resume event queue is closed");
                                break;
                            }
                        }
                    }
                    Ok(None) => {
                        warn!(source = %task.source.name, "system_resume D-Bus signal stream ended");
                        task.state.mark_watcher(&task.watcher_id, WatcherPhase::Crashed);
                        break;
                    }
                    Err(error) => {
                        warn!(source = %task.source.name, %error, "system_resume source failed");
                        task.state.mark_watcher(&task.watcher_id, WatcherPhase::Crashed);
                        break;
                    }
                }
            }
            changed = task.shutdown.changed() => {
                if changed.is_ok() && *task.shutdown.borrow() {
                    task.state.mark_watcher(&task.watcher_id, WatcherPhase::Stopped);
                    break;
                }
            }
        }
    }

    heartbeat.abort();
    let _ = heartbeat.await;
}

fn spawn_watcher_progress_heartbeat(
    state: Arc<RuntimeState>,
    watcher_id: String,
    mut shutdown: watch::Receiver<bool>,
) -> JoinHandle<()> {
    let interval = state.watcher_heartbeat_interval();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = sleep(interval) => {
                    state.mark_watcher_progress(&watcher_id);
                }
                changed = shutdown.changed() => {
                    if changed.is_ok() && *shutdown.borrow() {
                        break;
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::{CommandExecutor, CommandOutcome, CommandRunFuture};
    use crate::lane::{
        CommandLaneRunner, CommandLifecycle, CommandRequest, CommandTrigger, LaneCommand,
    };
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::timeout;

    #[derive(Clone)]
    struct TestTrigger {
        calls: Arc<AtomicUsize>,
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
        delay: Duration,
    }

    impl TestTrigger {
        fn new(delay: Duration) -> Self {
            Self {
                calls: Arc::new(AtomicUsize::new(0)),
                active: Arc::new(AtomicUsize::new(0)),
                max_active: Arc::new(AtomicUsize::new(0)),
                delay,
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
                    let active = this.active.fetch_add(1, Ordering::SeqCst) + 1;
                    this.max_active.fetch_max(active, Ordering::SeqCst);
                    let _ = lifecycle_tx.send(CommandLifecycle::Started);
                    if !this.delay.is_zero() {
                        sleep(this.delay).await;
                    }
                    this.active.fetch_sub(1, Ordering::SeqCst);
                    let _ = lifecycle_tx.send(CommandLifecycle::Finished(
                        CommandRunReport::Outcome(success_outcome()),
                    ));
                });
                Ok(lifecycle_rx)
            })
        }
    }

    fn success_outcome() -> CommandOutcome {
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

    #[derive(Clone)]
    struct TestExecutor {
        calls: Arc<AtomicUsize>,
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
        delay: Duration,
    }

    impl CommandExecutor for TestExecutor {
        fn run(&self, _shutdown: watch::Receiver<bool>) -> CommandRunFuture {
            let this = self.clone();
            Box::pin(async move {
                this.calls.fetch_add(1, Ordering::SeqCst);
                let active = this.active.fetch_add(1, Ordering::SeqCst) + 1;
                this.max_active.fetch_max(active, Ordering::SeqCst);
                if !this.delay.is_zero() {
                    sleep(this.delay).await;
                }
                this.active.fetch_sub(1, Ordering::SeqCst);
                Ok(success_outcome())
            })
        }
    }

    fn spawn_runner(
        settle: Duration,
        trigger: Arc<dyn CommandTriggerTarget>,
    ) -> (
        mpsc::Sender<()>,
        watch::Sender<bool>,
        tokio::task::JoinHandle<()>,
    ) {
        let (tx, rx) = mpsc::channel(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let runner = SystemResumeRunner::new("system-resume", settle, trigger, None);
        let handle = tokio::spawn(runner.run(rx, shutdown_rx));
        (tx, shutdown_tx, handle)
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
    async fn resume_event_triggers_command_once() {
        let trigger = Arc::new(TestTrigger::new(Duration::ZERO));
        let (events, shutdown, handle) = spawn_runner(Duration::from_millis(5), trigger.clone());
        events.send(()).await.unwrap();
        wait_for_calls(&trigger.calls, 1).await;
        sleep(Duration::from_millis(30)).await;
        assert_eq!(trigger.calls.load(Ordering::SeqCst), 1);
        let _ = shutdown.send(true);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn multiple_resume_events_are_coalesced() {
        let trigger = Arc::new(TestTrigger::new(Duration::ZERO));
        let (events, shutdown, handle) = spawn_runner(Duration::from_millis(30), trigger.clone());
        for _ in 0..5 {
            events.send(()).await.unwrap();
        }
        wait_for_calls(&trigger.calls, 1).await;
        sleep(Duration::from_millis(60)).await;
        assert_eq!(trigger.calls.load(Ordering::SeqCst), 1);
        let _ = shutdown.send(true);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn system_resume_uses_command_lanes() {
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let exec_a = TestExecutor {
            calls: Arc::new(AtomicUsize::new(0)),
            active: Arc::clone(&active),
            max_active: Arc::clone(&max_active),
            delay: Duration::from_millis(40),
        };
        let exec_b = TestExecutor {
            calls: Arc::new(AtomicUsize::new(0)),
            active,
            max_active: Arc::clone(&max_active),
            delay: Duration::from_millis(40),
        };
        let (request_tx, request_rx) = mpsc::channel::<CommandRequest>(16);
        let (lane_shutdown_tx, lane_shutdown_rx) = watch::channel(false);
        let lane = CommandLaneRunner::new(
            "shared",
            vec![
                LaneCommand {
                    name: Arc::from("a"),
                    executor: Arc::new(exec_a),
                    min_interval: Duration::ZERO,
                },
                LaneCommand {
                    name: Arc::from("b"),
                    executor: Arc::new(exec_b),
                    min_interval: Duration::ZERO,
                },
            ],
            request_rx,
            None,
        );
        let lane_handle = tokio::spawn(lane.run(lane_shutdown_rx));

        let trigger_a = Arc::new(CommandTrigger::new("a", "resume-a", request_tx.clone()));
        let trigger_b = Arc::new(CommandTrigger::new("b", "resume-b", request_tx));
        let (events_a, shutdown_a, handle_a) = spawn_runner(Duration::ZERO, trigger_a);
        let (events_b, shutdown_b, handle_b) = spawn_runner(Duration::ZERO, trigger_b);
        events_a.send(()).await.unwrap();
        events_b.send(()).await.unwrap();

        timeout(Duration::from_secs(2), async {
            while max_active.load(Ordering::SeqCst) == 0 {
                sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("timed out waiting for lane command");
        sleep(Duration::from_millis(120)).await;
        assert_eq!(max_active.load(Ordering::SeqCst), 1);

        let _ = shutdown_a.send(true);
        let _ = shutdown_b.send(true);
        let _ = lane_shutdown_tx.send(true);
        handle_a.await.unwrap();
        handle_b.await.unwrap();
        lane_handle.await.unwrap();
    }

    type FakeResumeEvents = VecDeque<Result<Option<ResumeSignal>, SystemResumeError>>;

    struct FakeResumeSource {
        events: Arc<Mutex<FakeResumeEvents>>,
    }

    impl FakeResumeSource {
        fn pending() -> Self {
            Self {
                events: Arc::new(Mutex::new(VecDeque::new())),
            }
        }

        fn with_events(events: impl IntoIterator<Item = ResumeSignal>) -> Self {
            Self {
                events: Arc::new(Mutex::new(
                    events.into_iter().map(|event| Ok(Some(event))).collect(),
                )),
            }
        }
    }

    impl ResumeEventSource for FakeResumeSource {
        fn next_event(&mut self) -> ResumeEventFuture<'_> {
            let events = Arc::clone(&self.events);
            Box::pin(async move {
                let event = events.lock().unwrap().pop_front();
                match event {
                    Some(event) => event,
                    None => std::future::pending().await,
                }
            })
        }
    }

    #[tokio::test]
    async fn system_resume_watcher_queues_resume_signal() {
        let config = SystemResumeSourceConfig {
            name: "system-resume".to_string(),
            on_resume: "sync".to_string(),
            settle_seconds: None,
        };
        let state = Arc::new(RuntimeState::new(
            0,
            1,
            0,
            Duration::from_secs(1),
            Duration::from_secs(1),
        ));
        state.register_watcher("system-resume");
        let (events_tx, mut events_rx) = mpsc::channel(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = SystemResumeWatcherTask {
            source: config,
            events_tx,
            state,
            watcher_id: "system-resume".to_string(),
            initial_ready: None,
            shutdown: shutdown_rx,
        };
        let handle = tokio::spawn(watch_system_resume_with_listener(
            task,
            FakeResumeSource::with_events([ResumeSignal::Suspending, ResumeSignal::Resumed]),
        ));

        assert_eq!(
            timeout(Duration::from_secs(1), events_rx.recv())
                .await
                .expect("timed out waiting for resume event"),
            Some(())
        );

        let _ = shutdown_tx.send(true);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn system_resume_watcher_health_does_not_go_stale_while_idle() {
        let config = SystemResumeSourceConfig {
            name: "system-resume".to_string(),
            on_resume: "sync".to_string(),
            settle_seconds: None,
        };
        let state = Arc::new(RuntimeState::new(
            0,
            1,
            0,
            Duration::from_millis(40),
            Duration::from_secs(1),
        ));
        state.register_watcher("system-resume");
        let (events_tx, _events_rx) = mpsc::channel(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (ready_tx, ready_rx) = oneshot::channel();
        let task = SystemResumeWatcherTask {
            source: config,
            events_tx,
            state: Arc::clone(&state),
            watcher_id: "system-resume".to_string(),
            initial_ready: Some(ready_tx),
            shutdown: shutdown_rx,
        };
        let handle = tokio::spawn(watch_system_resume_with_listener(
            task,
            FakeResumeSource::pending(),
        ));
        let ready = timeout(Duration::from_secs(1), ready_rx)
            .await
            .expect("timed out waiting for ready")
            .expect("ready sender dropped");
        assert!(ready.is_ok());

        sleep(Duration::from_millis(90)).await;
        assert!(state.is_healthy());

        let _ = shutdown_tx.send(true);
        handle.await.unwrap();
    }
}
