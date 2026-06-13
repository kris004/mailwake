use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use mailwake::auth;
use mailwake::command::{ShellCommandExecutor, run_shell_command};
use mailwake::config::{Config, MailboxConfig, SourceConfig, legacy_source_name};
use mailwake::debounce::DebounceRunner;
use mailwake::fs_state::{FsStateEvent, FsStateRunner, ShellStateReader, StateReader};
use mailwake::imap;
use mailwake::lane::{
    CommandLaneRunner, CommandRequest, CommandTrigger, CommandTriggerTarget, LaneCommand,
};
use mailwake::state::{CommandRunnerPhase, RuntimeState, WatcherPhase};
use mailwake::systemd::{self, Notifier};
use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Duration, timeout};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "mailwake", version, about = "Event-driven command trigger")]
struct Cli {
    #[arg(long, global = true, value_name = "PATH")]
    config: Option<PathBuf>,

    #[arg(
        long,
        global = true,
        help = "Disable opportunistic systemd notify/watchdog integration"
    )]
    no_systemd: bool,

    #[arg(
        long,
        global = true,
        help = "Wait for all watchers to complete initial setup before READY=1"
    )]
    initial_connect_required: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    #[command(
        name = "check-config",
        about = "Parse and validate config without connecting"
    )]
    CheckConfig,
    #[command(name = "test-command", about = "Run one configured mailbox command")]
    TestCommand {
        #[arg(long)]
        account: String,
        #[arg(long)]
        mailbox: String,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    init_tracing();
    let cli = Cli::parse();
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            error!(%error, "mailwake failed");
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<()> {
    let config_path = resolve_config_path(cli.config.as_deref());
    match cli.command {
        Some(Commands::CheckConfig) => check_config(&config_path),
        Some(Commands::TestCommand { account, mailbox }) => {
            test_command(&config_path, &account, &mailbox).await
        }
        None => run_daemon(&config_path, !cli.no_systemd, cli.initial_connect_required).await,
    }
}

fn check_config(path: &Path) -> Result<()> {
    let config =
        Config::load(path).with_context(|| format!("failed to load {}", path.display()))?;
    config.warn_for_insecure_options();
    auth::validate_auth_helpers(&config)?;
    println!(
        "config ok: {} account(s), {} source(s), {} command(s), {} lane(s)",
        config.accounts.len(),
        config.source_count(),
        config.command_count(),
        config.command_lane_count()
    );
    Ok(())
}

async fn test_command(path: &Path, account_name: &str, mailbox_name: &str) -> Result<()> {
    let config =
        Config::load(path).with_context(|| format!("failed to load {}", path.display()))?;
    config.warn_for_insecure_options();
    let (command, timeout) = mailbox_command(&config, account_name, mailbox_name)?;

    println!("running configured command for account={account_name:?} mailbox={mailbox_name:?}");
    let outcome = run_shell_command(command, config.capture_command_output(), timeout).await?;
    println!("command finished with {}", outcome.description());
    if !outcome.success {
        bail!("configured command failed with {}", outcome.description());
    }
    Ok(())
}

fn mailbox_command<'a>(
    config: &'a Config,
    account_name: &str,
    mailbox_name: &str,
) -> Result<(&'a str, Duration)> {
    if let Some(account) = config
        .accounts
        .iter()
        .find(|account| account.name == account_name)
        && let Some(mailbox) = account
            .mailboxes
            .iter()
            .find(|mailbox| mailbox.name == mailbox_name)
    {
        return Ok((&mailbox.on_notify, config.command_timeout()));
    }

    for source in &config.sources {
        let SourceConfig::ImapIdle(source) = source else {
            continue;
        };
        if source.account == account_name && source.mailbox == mailbox_name {
            let command = config
                .commands
                .iter()
                .find(|command| command.name == source.on_event)
                .with_context(|| {
                    format!(
                        "source {:?} references missing command {:?}",
                        source.name, source.on_event
                    )
                })?;
            return Ok((&command.cmd, command.timeout(config)));
        }
    }

    let account_exists = config
        .accounts
        .iter()
        .any(|account| account.name == account_name);
    if account_exists {
        bail!("mailbox {mailbox_name:?} not found in account {account_name:?}");
    }
    bail!("account {account_name:?} not found");
}

#[derive(Clone)]
struct RuntimeCommandSpec {
    name: String,
    lane: String,
    cmd: String,
    timeout: Duration,
    min_interval: Duration,
}

fn runtime_command_specs(config: &Config) -> Vec<RuntimeCommandSpec> {
    let mut commands = Vec::new();
    for account in &config.accounts {
        for mailbox in &account.mailboxes {
            let name = legacy_source_name(&account.name, &mailbox.name);
            commands.push(RuntimeCommandSpec {
                name: name.clone(),
                lane: name,
                cmd: mailbox.on_notify.clone(),
                timeout: config.command_timeout(),
                min_interval: config.min_command_interval(),
            });
        }
    }
    for command in &config.commands {
        commands.push(RuntimeCommandSpec {
            name: command.name.clone(),
            lane: command.lane_name().to_string(),
            cmd: command.cmd.clone(),
            timeout: command.timeout(config),
            min_interval: command.min_interval(config),
        });
    }
    commands
}

fn commands_by_lane(commands: Vec<RuntimeCommandSpec>) -> HashMap<String, Vec<RuntimeCommandSpec>> {
    let mut by_lane: HashMap<String, Vec<RuntimeCommandSpec>> = HashMap::new();
    for command in commands {
        by_lane
            .entry(command.lane.clone())
            .or_default()
            .push(command);
    }
    by_lane
}

fn command_trigger(
    command_senders: &HashMap<String, mpsc::Sender<CommandRequest>>,
    command_name: &str,
    source_name: &str,
) -> Result<Arc<dyn CommandTriggerTarget>> {
    let sender = command_senders
        .get(command_name)
        .with_context(|| format!("command {command_name:?} has no command lane"))?;
    Ok(Arc::new(CommandTrigger::new(
        Arc::<str>::from(command_name.to_string()),
        Arc::<str>::from(source_name.to_string()),
        sender.clone(),
    )))
}

async fn run_daemon(
    path: &Path,
    systemd_enabled: bool,
    initial_connect_required: bool,
) -> Result<()> {
    let config =
        Config::load(path).with_context(|| format!("failed to load {}", path.display()))?;
    auth::validate_auth_helpers(&config)?;
    config.warn_for_insecure_options();

    let command_specs = runtime_command_specs(&config);
    let max_command_timeout = command_specs
        .iter()
        .map(|command| command.timeout)
        .max()
        .unwrap_or_else(|| config.command_timeout());
    let state = Arc::new(RuntimeState::new(
        config.accounts.len(),
        config.source_count(),
        config.command_lane_count(),
        config.watcher_stale(),
        max_command_timeout,
    ));
    let notifier = Notifier::from_env(systemd_enabled);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut initial_receivers = Vec::new();
    let mut task_handles = Vec::new();
    let mut command_senders = HashMap::new();

    for (lane, commands) in commands_by_lane(command_specs) {
        let (request_tx, request_rx) = mpsc::channel(64);
        for command in &commands {
            command_senders.insert(command.name.clone(), request_tx.clone());
        }
        state.register_command_runner(lane.clone());
        let lane_commands = commands
            .into_iter()
            .map(|command| LaneCommand {
                name: Arc::<str>::from(command.name),
                executor: Arc::new(ShellCommandExecutor::new(
                    Arc::<str>::from(command.cmd),
                    config.capture_command_output(),
                    command.timeout,
                )),
                min_interval: command.min_interval,
            })
            .collect();
        let lane_runner = CommandLaneRunner::new(
            lane.clone(),
            lane_commands,
            request_rx,
            Some(Arc::clone(&state)),
        );
        task_handles.push(spawn_lane_runner(
            lane_runner,
            Arc::clone(&state),
            lane,
            shutdown_rx.clone(),
        ));
    }

    for account in config.accounts.clone() {
        for mailbox in account.mailboxes.clone() {
            let watcher_id = legacy_source_name(&account.name, &mailbox.name);
            state.register_watcher(watcher_id.clone());

            let (event_tx, event_rx) = mpsc::channel(64);
            let trigger = command_trigger(&command_senders, &watcher_id, &watcher_id)?;
            let debounce = mailbox.debounce(&config);
            let debounce_runner = DebounceRunner::new(
                account.name.clone(),
                mailbox.name.clone(),
                debounce,
                Duration::ZERO,
                trigger,
                Some(Arc::clone(&state)),
            );
            task_handles.push(spawn_debounce_runner(
                debounce_runner,
                event_rx,
                Arc::clone(&state),
                watcher_id.clone(),
                format!("{}/{}", account.name, mailbox.name),
                shutdown_rx.clone(),
            ));

            let (ready_tx, ready_rx) = oneshot::channel();
            initial_receivers.push(ready_rx);
            task_handles.push(spawn_watcher(
                account.clone(),
                mailbox,
                event_tx,
                Arc::clone(&state),
                watcher_id,
                Some(ready_tx),
                shutdown_rx.clone(),
                mailwake::imap::WatcherSettings {
                    idle_refresh: config.idle_refresh(),
                    auth_helper_timeout: config.auth_helper_timeout(),
                    auth_helper_max_output_bytes: config.auth_helper_max_output_bytes(),
                    connect_timeout: config.connect_timeout(),
                    operation_timeout: config.imap_operation_timeout(),
                },
            ));
        }
    }

    for source in config.sources.clone() {
        match source {
            SourceConfig::ImapIdle(source) => {
                let account = config
                    .accounts
                    .iter()
                    .find(|account| account.name == source.account)
                    .with_context(|| {
                        format!(
                            "source {:?} references unknown account {:?}",
                            source.name, source.account
                        )
                    })?
                    .clone();
                let watcher_id = source.name.clone();
                state.register_watcher(watcher_id.clone());

                let (event_tx, event_rx) = mpsc::channel(64);
                let trigger = command_trigger(&command_senders, &source.on_event, &watcher_id)?;
                let debounce_runner = DebounceRunner::new(
                    source.account.clone(),
                    source.mailbox.clone(),
                    source.debounce(&config),
                    Duration::ZERO,
                    trigger,
                    Some(Arc::clone(&state)),
                );
                task_handles.push(spawn_debounce_runner(
                    debounce_runner,
                    event_rx,
                    Arc::clone(&state),
                    watcher_id.clone(),
                    source.name.clone(),
                    shutdown_rx.clone(),
                ));

                let (ready_tx, ready_rx) = oneshot::channel();
                initial_receivers.push(ready_rx);
                let mailbox = MailboxConfig {
                    name: source.mailbox.clone(),
                    on_notify: String::new(),
                    debounce_seconds: source.debounce_seconds,
                };
                task_handles.push(spawn_watcher(
                    account,
                    mailbox,
                    event_tx,
                    Arc::clone(&state),
                    watcher_id,
                    Some(ready_tx),
                    shutdown_rx.clone(),
                    mailwake::imap::WatcherSettings {
                        idle_refresh: config.idle_refresh(),
                        auth_helper_timeout: config.auth_helper_timeout(),
                        auth_helper_max_output_bytes: config.auth_helper_max_output_bytes(),
                        connect_timeout: config.connect_timeout(),
                        operation_timeout: config.imap_operation_timeout(),
                    },
                ));
            }
            SourceConfig::FsState(source) => {
                let watcher_id = source.name.clone();
                state.register_watcher(watcher_id.clone());
                let trigger = command_trigger(&command_senders, &source.on_change, &watcher_id)?;
                let state_reader: Option<Arc<dyn StateReader>> =
                    source.state_cmd.as_ref().map(|command| {
                        Arc::new(ShellStateReader::new(
                            Arc::<str>::from(command.clone()),
                            config.command_timeout(),
                        )) as Arc<dyn StateReader>
                    });
                let runner = FsStateRunner::new(
                    source.name.clone(),
                    source.debounce(&config),
                    source.max_debounce(&config),
                    state_reader,
                    trigger,
                    Some(Arc::clone(&state)),
                );
                let (events_tx, events_rx) = mpsc::channel::<FsStateEvent>(64);
                let (ready_tx, ready_rx) = oneshot::channel();
                initial_receivers.push(ready_rx);
                task_handles.push(spawn_fs_state_watcher(
                    source,
                    runner,
                    events_tx,
                    events_rx,
                    Arc::clone(&state),
                    watcher_id,
                    Some(ready_tx),
                    shutdown_rx.clone(),
                ));
            }
        }
    }

    if initial_connect_required {
        info!("waiting for initial watcher setup");
        for ready in initial_receivers {
            tokio::select! {
                result = ready => result.context("watcher stopped before initial setup completed")?,
                _ = wait_for_shutdown_signal() => {
                    let _ = shutdown_tx.send(true);
                    let _ = notifier.stopping();
                    await_task_shutdown(
                        task_handles,
                        std::cmp::max(config.imap_operation_timeout(), max_command_timeout)
                            + Duration::from_secs(5),
                    )
                    .await;
                    return Ok(());
                }
            }
        }
    }

    let ready_status = format!(
        "mailwake started; watching {} source(s)",
        state.total_sources()
    );
    if let Err(error) = notifier.ready(&ready_status) {
        warn!(%error, "failed to send systemd READY notification");
    }
    info!(%ready_status);

    tokio::spawn(systemd::run_status_task(
        notifier.clone(),
        Arc::clone(&state),
        shutdown_rx.clone(),
    ));

    wait_for_shutdown_signal().await;
    info!("shutdown requested");
    let _ = shutdown_tx.send(true);
    if let Err(error) = notifier.stopping() {
        warn!(%error, "failed to send systemd STOPPING notification");
    }
    await_task_shutdown(
        task_handles,
        std::cmp::max(config.imap_operation_timeout(), max_command_timeout)
            + Duration::from_secs(5),
    )
    .await;
    Ok(())
}

fn spawn_debounce_runner(
    runner: DebounceRunner,
    event_rx: mpsc::Receiver<()>,
    state: Arc<RuntimeState>,
    watcher_id: String,
    source_name: String,
    shutdown: watch::Receiver<bool>,
) -> JoinHandle<()> {
    let handle = tokio::spawn(runner.run(event_rx, shutdown));
    tokio::spawn(async move {
        if let Err(error) = handle.await {
            state.mark_watcher(&watcher_id, WatcherPhase::Crashed);
            error!(
                source = %source_name,
                %error,
                "source debounce task crashed"
            );
        }
    })
}

fn spawn_lane_runner(
    runner: CommandLaneRunner,
    state: Arc<RuntimeState>,
    lane: String,
    shutdown: watch::Receiver<bool>,
) -> JoinHandle<()> {
    let handle = tokio::spawn(runner.run(shutdown));
    tokio::spawn(async move {
        match handle.await {
            Ok(()) => state.mark_command_runner(&lane, CommandRunnerPhase::Stopped),
            Err(error) => {
                state.mark_command_runner(&lane, CommandRunnerPhase::Crashed);
                error!(
                    lane = %lane,
                    %error,
                    "command lane task crashed"
                );
            }
        }
    })
}

#[allow(clippy::too_many_arguments)]
fn spawn_fs_state_watcher(
    source: mailwake::config::FsStateSourceConfig,
    runner: FsStateRunner,
    events_tx: mpsc::Sender<FsStateEvent>,
    events_rx: mpsc::Receiver<FsStateEvent>,
    state: Arc<RuntimeState>,
    watcher_id: String,
    initial_ready: Option<oneshot::Sender<()>>,
    shutdown: watch::Receiver<bool>,
) -> JoinHandle<()> {
    let state_for_task = Arc::clone(&state);
    let watcher_id_for_task = watcher_id.clone();
    let source_name = source.name.clone();
    let handle = tokio::spawn(async move {
        mailwake::fs_state::watch_fs_state_forever(
            source,
            runner,
            events_tx,
            events_rx,
            state_for_task,
            watcher_id_for_task,
            initial_ready,
            shutdown,
        )
        .await;
    });

    tokio::spawn(async move {
        if let Err(error) = handle.await {
            state.mark_watcher(&watcher_id, WatcherPhase::Crashed);
            error!(
                source = %source_name,
                %error,
                "fs_state watcher task crashed"
            );
        }
    })
}

#[allow(clippy::too_many_arguments)]
fn spawn_watcher(
    account: mailwake::config::AccountConfig,
    mailbox: mailwake::config::MailboxConfig,
    event_tx: mpsc::Sender<()>,
    state: Arc<RuntimeState>,
    watcher_id: String,
    initial_ready: Option<oneshot::Sender<()>>,
    shutdown: watch::Receiver<bool>,
    settings: mailwake::imap::WatcherSettings,
) -> JoinHandle<()> {
    let state_for_task = Arc::clone(&state);
    let account_name = account.name.clone();
    let mailbox_name = mailbox.name.clone();
    let handle = tokio::spawn(async move {
        imap::watch_mailbox_forever(
            account,
            mailbox,
            event_tx,
            state_for_task,
            initial_ready,
            shutdown,
            settings,
        )
        .await;
    });

    tokio::spawn(async move {
        if let Err(error) = handle.await {
            state.mark_watcher(&watcher_id, WatcherPhase::Crashed);
            error!(
                account = %account_name,
                mailbox = %mailbox_name,
                %error,
                "IMAP watcher task crashed"
            );
        }
    })
}

async fn await_task_shutdown(handles: Vec<JoinHandle<()>>, grace: Duration) {
    for handle in handles {
        match timeout(grace, handle).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => warn!(%error, "supervised task failed during shutdown"),
            Err(_) => warn!("timed out waiting for supervised task shutdown"),
        }
    }
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}

fn resolve_config_path(path: Option<&Path>) -> PathBuf {
    path.map(expand_tilde)
        .unwrap_or_else(|| default_config_path().unwrap_or_else(|| PathBuf::from("config.toml")))
}

fn default_config_path() -> Option<PathBuf> {
    env::var_os("HOME").map(|home| PathBuf::from(home).join(".config/mailwake/config.toml"))
}

fn expand_tilde(path: &Path) -> PathBuf {
    let Some(path_string) = path.to_str() else {
        return path.to_path_buf();
    };
    if path_string == "~" {
        return env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| path.to_path_buf());
    }
    if let Some(rest) = path_string.strip_prefix("~/")
        && let Some(home) = env::var_os("HOME")
    {
        return PathBuf::from(home).join(rest);
    }
    path.to_path_buf()
}
