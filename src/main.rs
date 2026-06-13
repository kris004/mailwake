use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use mailwake::auth;
use mailwake::command::{CommandExecutor, ShellCommandExecutor, run_shell_command};
use mailwake::config::Config;
use mailwake::debounce::DebounceRunner;
use mailwake::imap;
use mailwake::state::{CommandRunnerPhase, RuntimeState, WatcherPhase};
use mailwake::systemd::{self, Notifier};
use std::env;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "mailwake", version, about = "IMAP IDLE command trigger")]
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
        help = "Wait for all watchers to complete initial login/select/IDLE before READY=1"
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
        "config ok: {} account(s), {} mailbox(es)",
        config.accounts.len(),
        config.mailbox_count()
    );
    Ok(())
}

async fn test_command(path: &Path, account_name: &str, mailbox_name: &str) -> Result<()> {
    let config =
        Config::load(path).with_context(|| format!("failed to load {}", path.display()))?;
    config.warn_for_insecure_options();
    let account = config
        .accounts
        .iter()
        .find(|account| account.name == account_name)
        .with_context(|| format!("account {account_name:?} not found"))?;
    let mailbox = account
        .mailboxes
        .iter()
        .find(|mailbox| mailbox.name == mailbox_name)
        .with_context(|| {
            format!("mailbox {mailbox_name:?} not found in account {account_name:?}")
        })?;

    println!("running configured command for account={account_name:?} mailbox={mailbox_name:?}");
    let outcome = run_shell_command(
        &mailbox.on_notify,
        config.log_command_output,
        config.command_timeout(),
    )
    .await?;
    println!("command finished with {}", outcome.description());
    if !outcome.success {
        bail!("configured command failed with {}", outcome.description());
    }
    Ok(())
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

    let state = Arc::new(RuntimeState::new(
        config.accounts.len(),
        config.mailbox_count(),
        config.watcher_stale(),
        config.command_timeout(),
    ));
    let notifier = Notifier::from_env(systemd_enabled);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut initial_receivers = Vec::new();

    for account in config.accounts.clone() {
        for mailbox in account.mailboxes.clone() {
            let watcher_id = format!("{}/{}", account.name, mailbox.name);
            state.register_watcher(watcher_id.clone());

            let (event_tx, event_rx) = mpsc::channel(64);
            let executor: Arc<dyn CommandExecutor> = Arc::new(ShellCommandExecutor::new(
                Arc::<str>::from(mailbox.on_notify.clone()),
                config.log_command_output,
                config.command_timeout(),
            ));
            let debounce = mailbox.debounce(&config);
            state.register_command_runner(watcher_id.clone());
            let debounce_runner = DebounceRunner::new(
                account.name.clone(),
                mailbox.name.clone(),
                debounce,
                executor,
                Some(Arc::clone(&state)),
            );
            spawn_command_runner(
                debounce_runner,
                event_rx,
                Arc::clone(&state),
                watcher_id.clone(),
                account.name.clone(),
                mailbox.name.clone(),
                shutdown_rx.clone(),
            );

            let (ready_tx, ready_rx) = oneshot::channel();
            initial_receivers.push(ready_rx);
            spawn_watcher(
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
                },
            );
        }
    }

    if initial_connect_required {
        info!("waiting for initial IMAP login/select/IDLE on every watcher");
        for ready in initial_receivers {
            tokio::select! {
                result = ready => result.context("watcher stopped before initial IMAP setup completed")?,
                _ = wait_for_shutdown_signal() => {
                    let _ = shutdown_tx.send(true);
                    let _ = notifier.stopping();
                    return Ok(());
                }
            }
        }
    }

    let ready_status = format!(
        "mailwake started; watching {} mailbox(es)",
        state.total_mailboxes()
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
    Ok(())
}

fn spawn_command_runner(
    runner: DebounceRunner,
    event_rx: mpsc::Receiver<()>,
    state: Arc<RuntimeState>,
    runner_id: String,
    account_name: String,
    mailbox_name: String,
    shutdown: watch::Receiver<bool>,
) {
    let handle = tokio::spawn(runner.run(event_rx, shutdown));
    tokio::spawn(async move {
        match handle.await {
            Ok(()) => state.mark_command_runner(&runner_id, CommandRunnerPhase::Stopped),
            Err(error) => {
                state.mark_command_runner(&runner_id, CommandRunnerPhase::Crashed);
                error!(
                    account = %account_name,
                    mailbox = %mailbox_name,
                    %error,
                    "command runner task crashed"
                );
            }
        }
    });
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
) {
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
    });
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
