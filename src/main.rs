use anyhow::{Context, Result, bail};
use clap::{ArgGroup, Parser, Subcommand};
use mailwake::auth;
use mailwake::command::{
    CommandOutputPolicy, ShellCommandExecutor, run_named_shell_command_with_policy,
};
use mailwake::config::{Config, MailboxConfig, SourceConfig, legacy_source_name};
use mailwake::debounce::DebounceRunner;
use mailwake::fs_state::{
    FsStateEvent, FsStateRunner, FsStateWatcherTask, ShellStateReader, StateReader,
};
use mailwake::gmail_api_poll::{GmailApiPollSettings, GmailApiPollTask};
use mailwake::imap;
use mailwake::lane::{
    CommandLaneRunner, CommandRequest, CommandTrigger, CommandTriggerTarget, LaneCommand,
};
use mailwake::state::{CommandRunnerPhase, RuntimeState, WatcherPhase};
use mailwake::system_resume::{SystemResumeRunner, SystemResumeWatcherTask};
use mailwake::systemd::{self, Notifier};
use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Duration, timeout};
use tracing::{debug, error, info, warn};
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
    #[command(
        name = "test-command",
        about = "Run one configured command",
        group(
            ArgGroup::new("target")
                .required(true)
                .args(["command", "account"])
        )
    )]
    TestCommand {
        #[arg(
            long,
            value_name = "NAME",
            conflicts_with_all = ["account", "mailbox"]
        )]
        command: Option<String>,
        #[arg(long, requires = "mailbox")]
        account: Option<String>,
        #[arg(long, requires = "account")]
        mailbox: Option<String>,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    init_tracing();
    let cli = Cli::parse();
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let exit_code = error
                .downcast_ref::<CodedExitError>()
                .map(|error| ExitCode::from(error.code))
                .unwrap_or(ExitCode::FAILURE);
            error!(%error, "mailwake failed");
            eprintln!("error: {error}");
            exit_code
        }
    }
}

#[derive(Clone, Debug, Error)]
#[error("{message}")]
struct CodedExitError {
    code: u8,
    message: String,
}

impl CodedExitError {
    fn permanent_auth(message: impl Into<String>) -> Self {
        Self {
            code: auth::REAUTH_REQUIRED_EXIT_CODE,
            message: message.into(),
        }
    }
}

async fn run(cli: Cli) -> Result<()> {
    let config_path = resolve_config_path(cli.config.as_deref());
    match cli.command {
        Some(Commands::CheckConfig) => check_config(&config_path),
        Some(Commands::TestCommand {
            command,
            account,
            mailbox,
        }) => {
            test_command(
                &config_path,
                command.as_deref(),
                account.as_deref(),
                mailbox.as_deref(),
            )
            .await
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

async fn test_command(
    path: &Path,
    command_name: Option<&str>,
    account_name: Option<&str>,
    mailbox_name: Option<&str>,
) -> Result<()> {
    let config =
        Config::load(path).with_context(|| format!("failed to load {}", path.display()))?;
    config.warn_for_insecure_options();
    let (command_name, command, timeout, output_policy, description) =
        configured_test_command(&config, command_name, account_name, mailbox_name)?;

    println!("running configured command for {description}");
    let outcome =
        run_named_shell_command_with_policy(&command_name, command, timeout, output_policy, None)
            .await?;
    println!("command finished with {}", outcome.description());
    if !outcome.success {
        bail!("configured command failed with {}", outcome.description());
    }
    Ok(())
}

fn configured_test_command<'a>(
    config: &'a Config,
    command_name: Option<&str>,
    account_name: Option<&str>,
    mailbox_name: Option<&str>,
) -> Result<(String, &'a str, Duration, CommandOutputPolicy, String)> {
    match (command_name, account_name, mailbox_name) {
        (Some(name), None, None) => {
            let command = config
                .commands
                .iter()
                .find(|command| command.name == name)
                .with_context(|| format!("command {name:?} not found"))?;
            Ok((
                command.name.clone(),
                &command.cmd,
                command.timeout(config),
                command.output_policy(config),
                format!("command={name:?}"),
            ))
        }
        (None, Some(account), Some(mailbox)) => {
            let (name, command, timeout) = mailbox_command(config, account, mailbox)?;
            Ok((
                name,
                command,
                timeout,
                config.command_output_policy(),
                format!("account={account:?} mailbox={mailbox:?}"),
            ))
        }
        _ => bail!("specify either --command or both --account and --mailbox"),
    }
}

fn mailbox_command<'a>(
    config: &'a Config,
    account_name: &str,
    mailbox_name: &str,
) -> Result<(String, &'a str, Duration)> {
    if let Some(account) = config
        .accounts
        .iter()
        .find(|account| account.name == account_name)
        && let Some(mailbox) = account
            .mailboxes
            .iter()
            .find(|mailbox| mailbox.name == mailbox_name)
    {
        return Ok((
            legacy_source_name(&account.name, &mailbox.name),
            &mailbox.on_notify,
            config.command_timeout(),
        ));
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
            return Ok((command.name.clone(), &command.cmd, command.timeout(config)));
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
    output_policy: CommandOutputPolicy,
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
                output_policy: config.command_output_policy(),
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
            output_policy: command.output_policy(config),
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
    let max_watcher_operation_timeout =
        config
            .sources
            .iter()
            .fold(
                config.imap_operation_timeout(),
                |max_timeout, source| match source {
                    SourceConfig::GmailApiPoll(source) => max_timeout.max(source.api_timeout()),
                    _ => max_timeout,
                },
            );
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
    let (startup_tx, startup_rx) = watch::channel(false);
    let (fatal_tx, mut fatal_rx) = mpsc::unbounded_channel::<CodedExitError>();
    let mut initial_receivers: Vec<oneshot::Receiver<Result<(), String>>> = Vec::new();
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
            .map(|command| {
                let command_name = Arc::<str>::from(command.name);
                LaneCommand {
                    name: Arc::clone(&command_name),
                    executor: Arc::new(ShellCommandExecutor::new(
                        command_name,
                        Arc::<str>::from(command.cmd),
                        command.timeout,
                        command.output_policy,
                    )),
                    min_interval: command.min_interval,
                }
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

            let initial_ready = if initial_connect_required {
                let (ready_tx, ready_rx) = oneshot::channel();
                initial_receivers.push(ready_rx);
                Some(ready_tx)
            } else {
                None
            };
            task_handles.push(spawn_watcher(
                ImapWatcherTask {
                    account: account.clone(),
                    mailbox,
                    event_tx,
                    state: Arc::clone(&state),
                    watcher_id,
                    initial_ready,
                    shutdown: shutdown_rx.clone(),
                    settings: mailwake::imap::WatcherSettings {
                        idle_refresh: config.idle_refresh(),
                        auth_helper_timeout: config.auth_helper_timeout(),
                        auth_helper_max_output_bytes: config.auth_helper_max_output_bytes(),
                        connect_timeout: config.connect_timeout(),
                        operation_timeout: config.imap_operation_timeout(),
                    },
                },
                fatal_tx.clone(),
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
                if source.run_on_startup {
                    task_handles.push(spawn_startup_event(
                        source.name.clone(),
                        event_tx.clone(),
                        startup_rx.clone(),
                        shutdown_rx.clone(),
                    ));
                }
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

                let initial_ready = if initial_connect_required {
                    let (ready_tx, ready_rx) = oneshot::channel();
                    initial_receivers.push(ready_rx);
                    Some(ready_tx)
                } else {
                    None
                };
                let mailbox = MailboxConfig {
                    name: source.mailbox.clone(),
                    on_notify: String::new(),
                    debounce_seconds: source.debounce_seconds,
                };
                task_handles.push(spawn_watcher(
                    ImapWatcherTask {
                        account,
                        mailbox,
                        event_tx,
                        state: Arc::clone(&state),
                        watcher_id,
                        initial_ready,
                        shutdown: shutdown_rx.clone(),
                        settings: mailwake::imap::WatcherSettings {
                            idle_refresh: config.idle_refresh(),
                            auth_helper_timeout: config.auth_helper_timeout(),
                            auth_helper_max_output_bytes: config.auth_helper_max_output_bytes(),
                            connect_timeout: config.connect_timeout(),
                            operation_timeout: config.imap_operation_timeout(),
                        },
                    },
                    fatal_tx.clone(),
                ));
            }
            SourceConfig::GmailApiPoll(source) => {
                let watcher_id = source.name.clone();
                state.register_watcher(watcher_id.clone());

                let (event_tx, event_rx) = mpsc::channel(64);
                let trigger = command_trigger(&command_senders, &source.on_event, &watcher_id)?;
                if source.run_on_startup {
                    task_handles.push(spawn_startup_event(
                        source.name.clone(),
                        event_tx.clone(),
                        startup_rx.clone(),
                        shutdown_rx.clone(),
                    ));
                }
                let debounce_runner = DebounceRunner::new(
                    "gmail_api",
                    source.name.clone(),
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

                let initial_ready = if initial_connect_required {
                    let (ready_tx, ready_rx) = oneshot::channel();
                    initial_receivers.push(ready_rx);
                    Some(ready_tx)
                } else {
                    None
                };
                let settings = GmailApiPollSettings {
                    auth_helper_timeout: config.auth_helper_timeout(),
                    auth_helper_max_output_bytes: config.auth_helper_max_output_bytes(),
                    poll_interval: source.poll_interval(),
                    api_timeout: source.api_timeout(),
                };
                task_handles.push(spawn_gmail_api_poll_watcher(
                    GmailApiPollTask {
                        source,
                        events: event_tx,
                        state: Arc::clone(&state),
                        watcher_id,
                        initial_ready,
                        shutdown: shutdown_rx.clone(),
                        settings,
                    },
                    fatal_tx.clone(),
                ));
            }
            SourceConfig::FsState(source) => {
                let watcher_id = source.name.clone();
                let run_on_startup = source.run_on_startup;
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
                if run_on_startup {
                    task_handles.push(spawn_fs_state_startup_event(
                        source.name.clone(),
                        events_tx.clone(),
                        startup_rx.clone(),
                        shutdown_rx.clone(),
                    ));
                }
                let initial_ready = if initial_connect_required {
                    let (ready_tx, ready_rx) = oneshot::channel();
                    initial_receivers.push(ready_rx);
                    Some(ready_tx)
                } else {
                    None
                };
                task_handles.push(spawn_fs_state_watcher(FsStateWatcherTask {
                    source,
                    runner,
                    events_tx,
                    events_rx,
                    state: Arc::clone(&state),
                    watcher_id,
                    initial_ready,
                    startup: None,
                    shutdown: shutdown_rx.clone(),
                }));
            }
            SourceConfig::SystemResume(source) => {
                let watcher_id = source.name.clone();
                state.register_watcher(watcher_id.clone());
                let trigger = command_trigger(&command_senders, &source.on_resume, &watcher_id)?;
                let runner = SystemResumeRunner::new(
                    source.name.clone(),
                    source.settle(),
                    trigger,
                    Some(Arc::clone(&state)),
                );
                let (events_tx, events_rx) = mpsc::channel(64);
                task_handles.push(spawn_system_resume_runner(
                    runner,
                    events_rx,
                    Arc::clone(&state),
                    watcher_id.clone(),
                    source.name.clone(),
                    shutdown_rx.clone(),
                ));

                let initial_ready = if initial_connect_required {
                    let (ready_tx, ready_rx) = oneshot::channel();
                    initial_receivers.push(ready_rx);
                    Some(ready_tx)
                } else {
                    None
                };
                task_handles.push(spawn_system_resume_watcher(SystemResumeWatcherTask {
                    source,
                    events_tx,
                    state: Arc::clone(&state),
                    watcher_id,
                    initial_ready,
                    shutdown: shutdown_rx.clone(),
                }));
            }
        }
    }

    if initial_connect_required {
        info!("waiting for initial watcher setup");
        for ready in initial_receivers {
            tokio::select! {
                result = ready => match result.context("watcher stopped before initial setup completed")? {
                    Ok(()) => {}
                    Err(error) => {
                        if error.starts_with(mailwake::imap::INITIAL_AUTH_FAILURE_PREFIX) {
                            return Err(CodedExitError::permanent_auth(format!(
                                "initial watcher setup failed: {error}"
                            ))
                            .into());
                        }
                        if error.starts_with(
                            mailwake::gmail_api_poll::INITIAL_AUTH_FAILURE_PREFIX,
                        ) {
                            return Err(CodedExitError::permanent_auth(format!(
                                "initial watcher setup failed: {error}"
                            ))
                            .into());
                        }
                        bail!("initial watcher setup failed: {error}");
                    }
                },
                fatal = fatal_rx.recv() => {
                    let fatal = fatal.context("fatal watcher channel closed")?;
                    let _ = shutdown_tx.send(true);
                    let _ = notifier.stopping();
                    await_task_shutdown(
                        task_handles,
                        std::cmp::max(max_watcher_operation_timeout, max_command_timeout)
                            + Duration::from_secs(5),
                    )
                    .await;
                    return Err(fatal.into());
                },
                _ = wait_for_shutdown_signal() => {
                    let _ = shutdown_tx.send(true);
                    let _ = notifier.stopping();
                    await_task_shutdown(
                        task_handles,
                        std::cmp::max(max_watcher_operation_timeout, max_command_timeout)
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
    let receiver_count = startup_tx.receiver_count();
    match startup_tx.send(true) {
        Ok(()) => info!(receiver_count, "broadcast run_on_startup signal"),
        Err(error) => warn!(receiver_count, %error, "failed to broadcast run_on_startup signal"),
    }

    tokio::spawn(systemd::run_status_task(
        notifier.clone(),
        Arc::clone(&state),
        shutdown_rx.clone(),
    ));

    tokio::select! {
        () = wait_for_shutdown_signal() => {
            info!("shutdown requested");
        }
        fatal = fatal_rx.recv() => {
            let fatal = fatal.context("fatal watcher channel closed")?;
            error!(%fatal, "fatal watcher failure");
            let _ = shutdown_tx.send(true);
            if let Err(error) = notifier.stopping() {
                warn!(%error, "failed to send systemd STOPPING notification");
            }
            await_task_shutdown(
                task_handles,
                std::cmp::max(max_watcher_operation_timeout, max_command_timeout)
                    + Duration::from_secs(5),
            )
            .await;
            return Err(fatal.into());
        }
    }
    let _ = shutdown_tx.send(true);
    if let Err(error) = notifier.stopping() {
        warn!(%error, "failed to send systemd STOPPING notification");
    }
    await_task_shutdown(
        task_handles,
        std::cmp::max(max_watcher_operation_timeout, max_command_timeout) + Duration::from_secs(5),
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

fn spawn_startup_event(
    source_name: String,
    events: mpsc::Sender<()>,
    mut startup: watch::Receiver<bool>,
    mut shutdown: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if *startup.borrow() {
                break;
            }
            tokio::select! {
                changed = startup.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_ok() && *shutdown.borrow() {
                        return;
                    }
                }
            }
        }

        match events.try_send(()) {
            Ok(()) => {
                info!(
                    source = %source_name,
                    "queued run_on_startup source event"
                );
            }
            Err(mpsc::error::TrySendError::Full(())) => {
                debug!(
                    source = %source_name,
                    "source event queue is full; coalescing run_on_startup event"
                );
            }
            Err(mpsc::error::TrySendError::Closed(())) => {
                warn!(
                    source = %source_name,
                    "source event queue is closed before run_on_startup event"
                );
            }
        }
    })
}

fn spawn_fs_state_startup_event(
    source_name: String,
    events: mpsc::Sender<FsStateEvent>,
    mut startup: watch::Receiver<bool>,
    mut shutdown: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if *startup.borrow() {
                break;
            }
            tokio::select! {
                changed = startup.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_ok() && *shutdown.borrow() {
                        return;
                    }
                }
            }
        }

        tokio::select! {
            result = events.send(FsStateEvent::Startup) => {
                if result.is_ok() {
                    info!(
                        source = %source_name,
                        "queued fs_state run_on_startup event"
                    );
                } else {
                    warn!(
                        source = %source_name,
                        "fs_state event queue is closed before run_on_startup event"
                    );
                }
            }
            _ = shutdown.changed() => {}
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

fn spawn_fs_state_watcher(task: FsStateWatcherTask) -> JoinHandle<()> {
    let state = Arc::clone(&task.state);
    let watcher_id = task.watcher_id.clone();
    let source_name = task.source.name.clone();
    let handle = tokio::spawn(async move {
        mailwake::fs_state::watch_fs_state_forever(task).await;
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

fn spawn_system_resume_runner(
    runner: SystemResumeRunner,
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
                "system_resume runner task crashed"
            );
        }
    })
}

fn spawn_system_resume_watcher(task: SystemResumeWatcherTask) -> JoinHandle<()> {
    let state = Arc::clone(&task.state);
    let watcher_id = task.watcher_id.clone();
    let source_name = task.source.name.clone();
    let handle = tokio::spawn(async move {
        mailwake::system_resume::watch_system_resume_forever(task).await;
    });

    tokio::spawn(async move {
        if let Err(error) = handle.await {
            state.mark_watcher(&watcher_id, WatcherPhase::Crashed);
            error!(
                source = %source_name,
                %error,
                "system_resume watcher task crashed"
            );
        }
    })
}

fn spawn_gmail_api_poll_watcher(
    task: GmailApiPollTask,
    fatal_tx: mpsc::UnboundedSender<CodedExitError>,
) -> JoinHandle<()> {
    let state = Arc::clone(&task.state);
    let watcher_id = task.watcher_id.clone();
    let source_name = task.source.name.clone();
    let handle =
        tokio::spawn(
            async move { mailwake::gmail_api_poll::watch_gmail_api_poll_forever(task).await },
        );

    tokio::spawn(async move {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) if error.is_permanent_auth_failure() => {
                state.mark_watcher(&watcher_id, WatcherPhase::Crashed);
                let message = format!(
                    "Gmail API poller for source {source_name:?} stopped after an authentication or permission failure: {error}"
                );
                error!(
                    source = %source_name,
                    %error,
                    "Gmail API poller stopped after an authentication or permission failure"
                );
                let _ = fatal_tx.send(CodedExitError::permanent_auth(message));
            }
            Ok(Err(error)) => {
                state.mark_watcher(&watcher_id, WatcherPhase::Crashed);
                error!(
                    source = %source_name,
                    %error,
                    "Gmail API poller task stopped unexpectedly"
                );
            }
            Err(error) => {
                state.mark_watcher(&watcher_id, WatcherPhase::Crashed);
                error!(
                    source = %source_name,
                    %error,
                    "Gmail API poller task crashed"
                );
            }
        }
    })
}

struct ImapWatcherTask {
    account: mailwake::config::AccountConfig,
    mailbox: mailwake::config::MailboxConfig,
    event_tx: mpsc::Sender<()>,
    state: Arc<RuntimeState>,
    watcher_id: String,
    initial_ready: Option<oneshot::Sender<Result<(), String>>>,
    shutdown: watch::Receiver<bool>,
    settings: mailwake::imap::WatcherSettings,
}

fn spawn_watcher(
    task: ImapWatcherTask,
    fatal_tx: mpsc::UnboundedSender<CodedExitError>,
) -> JoinHandle<()> {
    let state = Arc::clone(&task.state);
    let watcher_id = task.watcher_id.clone();
    let account_name = task.account.name.clone();
    let mailbox_name = task.mailbox.name.clone();
    let handle = tokio::spawn(async move {
        imap::watch_mailbox_forever(imap::MailboxWatchTask {
            account: task.account,
            mailbox: task.mailbox,
            events: task.event_tx,
            state: task.state,
            watcher_id: task.watcher_id,
            initial_ready: task.initial_ready,
            shutdown: task.shutdown,
            settings: task.settings,
        })
        .await
    });

    tokio::spawn(async move {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                state.mark_watcher(&watcher_id, WatcherPhase::Crashed);
                let message = format!(
                    "IMAP watcher for account {account_name:?} mailbox {mailbox_name:?} stopped after an authentication failure: {error}"
                );
                error!(
                    account = %account_name,
                    mailbox = %mailbox_name,
                    %error,
                    "IMAP watcher stopped after an authentication failure"
                );
                let _ = fatal_tx.send(CodedExitError::permanent_auth(message));
            }
            Err(error) => {
                state.mark_watcher(&watcher_id, WatcherPhase::Crashed);
                error!(
                    account = %account_name,
                    mailbox = %mailbox_name,
                    %error,
                    "IMAP watcher task crashed"
                );
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_accepts_named_command() {
        let cli = Cli::try_parse_from(["mailwake", "test-command", "--command", "local-push"])
            .expect("named command invocation should parse");
        let Some(Commands::TestCommand {
            command,
            account,
            mailbox,
        }) = cli.command
        else {
            panic!("expected test-command");
        };
        assert_eq!(command.as_deref(), Some("local-push"));
        assert!(account.is_none());
        assert!(mailbox.is_none());
    }

    #[test]
    fn test_command_keeps_legacy_account_mailbox_invocation() {
        let cli = Cli::try_parse_from([
            "mailwake",
            "test-command",
            "--account",
            "gmail",
            "--mailbox",
            "INBOX",
        ])
        .expect("legacy invocation should parse");
        let Some(Commands::TestCommand {
            command,
            account,
            mailbox,
        }) = cli.command
        else {
            panic!("expected test-command");
        };
        assert!(command.is_none());
        assert_eq!(account.as_deref(), Some("gmail"));
        assert_eq!(mailbox.as_deref(), Some("INBOX"));
    }

    #[test]
    fn test_command_rejects_ambiguous_invocation() {
        let result = Cli::try_parse_from([
            "mailwake",
            "test-command",
            "--command",
            "local-push",
            "--account",
            "gmail",
            "--mailbox",
            "INBOX",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_command_requires_complete_legacy_target() {
        assert!(Cli::try_parse_from(["mailwake", "test-command", "--account", "gmail"]).is_err());
        assert!(Cli::try_parse_from(["mailwake", "test-command", "--mailbox", "INBOX"]).is_err());
    }

    #[test]
    fn configured_test_command_finds_named_command() {
        let config = Config::parse_str(
            r#"
command_timeout_seconds = 30

[[commands]]
name = "local-push"
cmd = "echo push"
timeout_seconds = 7

[[sources]]
name = "local-state"
type = "fs_state"
watch_paths = ["/tmp/mailwake-example-state"]
on_change = "local-push"
"#,
        )
        .expect("config should parse");

        let (name, command, timeout, output_policy, description) =
            configured_test_command(&config, Some("local-push"), None, None)
                .expect("command should be found");
        assert_eq!(name, "local-push");
        assert_eq!(command, "echo push");
        assert_eq!(timeout, Duration::from_secs(7));
        assert_eq!(
            output_policy.mode,
            mailwake::command::CommandOutputMode::FailureTail
        );
        assert_eq!(description, "command=\"local-push\"");
    }

    #[test]
    fn configured_test_command_keeps_legacy_account_mailbox_lookup() {
        let config = Config::parse_str(
            r#"
command_timeout_seconds = 30

[[accounts]]
name = "gmail"
host = "imap.gmail.com"
username = "user@example.com"
auth = "xoauth2_cmd"
xoauth2_cmd = "gmail-oauth-token"

[[accounts.mailboxes]]
name = "INBOX"
on_notify = "echo sync"
"#,
        )
        .expect("config should parse");

        let (name, command, timeout, output_policy, description) =
            configured_test_command(&config, None, Some("gmail"), Some("INBOX"))
                .expect("legacy command should be found");
        assert_eq!(name, "gmail/INBOX");
        assert_eq!(command, "echo sync");
        assert_eq!(timeout, Duration::from_secs(30));
        assert_eq!(
            output_policy.mode,
            mailwake::command::CommandOutputMode::FailureTail
        );
        assert_eq!(description, "account=\"gmail\" mailbox=\"INBOX\"");
    }

    #[tokio::test]
    async fn fs_state_startup_event_is_queued_after_startup_signal() {
        let (event_tx, mut event_rx) = mpsc::channel(1);
        let (startup_tx, startup_rx) = watch::channel(false);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = spawn_fs_state_startup_event(
            "local-state".to_string(),
            event_tx,
            startup_rx,
            shutdown_rx,
        );

        assert!(
            timeout(Duration::from_millis(20), event_rx.recv())
                .await
                .is_err()
        );
        startup_tx.send(true).unwrap();
        assert_eq!(
            timeout(Duration::from_secs(1), event_rx.recv())
                .await
                .expect("timed out waiting for startup event"),
            Some(FsStateEvent::Startup)
        );
        handle.await.unwrap();
        drop(shutdown_tx);
    }

    #[tokio::test]
    async fn startup_event_is_queued_after_startup_signal() {
        let (event_tx, mut event_rx) = mpsc::channel(1);
        let (startup_tx, startup_rx) = watch::channel(false);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = spawn_startup_event(
            "remote-inbox".to_string(),
            event_tx,
            startup_rx,
            shutdown_rx,
        );

        assert!(
            timeout(Duration::from_millis(20), event_rx.recv())
                .await
                .is_err()
        );
        startup_tx.send(true).unwrap();
        assert_eq!(
            timeout(Duration::from_secs(1), event_rx.recv())
                .await
                .expect("timed out waiting for startup event"),
            Some(())
        );
        handle.await.unwrap();
        drop(shutdown_tx);
    }
}
