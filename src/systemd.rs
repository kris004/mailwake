use crate::state::RuntimeState;
use std::env;
use std::ffi::{OsStr, OsString};
use std::io;
use std::mem;
use std::os::unix::ffi::OsStrExt;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::sleep;
use tracing::warn;

#[derive(Clone, Debug)]
pub struct Notifier {
    socket: Option<OsString>,
    watchdog_interval: Option<Duration>,
}

impl Notifier {
    pub fn from_env(enabled: bool) -> Self {
        if !enabled {
            return Self::disabled();
        }
        let socket = env::var_os("NOTIFY_SOCKET");
        let watchdog_interval =
            if watchdog_pid_allows(env::var("WATCHDOG_PID").ok().as_deref(), std::process::id()) {
                env::var("WATCHDOG_USEC")
                    .ok()
                    .and_then(|value| value.parse::<u64>().ok())
                    .filter(|usec| *usec > 0)
                    .map(Duration::from_micros)
            } else {
                None
            };
        Self {
            socket,
            watchdog_interval,
        }
    }

    pub fn disabled() -> Self {
        Self {
            socket: None,
            watchdog_interval: None,
        }
    }

    pub fn is_available(&self) -> bool {
        self.socket.is_some()
    }

    pub fn watchdog_interval(&self) -> Option<Duration> {
        self.watchdog_interval
    }

    pub fn ready(&self, status: &str) -> io::Result<()> {
        let status = sanitize_field(status);
        self.notify(&[("READY", "1"), ("STATUS", status.as_str())])
    }

    pub fn status(&self, status: &str) -> io::Result<()> {
        let status = sanitize_field(status);
        self.notify(&[("STATUS", status.as_str())])
    }

    pub fn watchdog(&self) -> io::Result<()> {
        self.notify(&[("WATCHDOG", "1")])
    }

    pub fn stopping(&self) -> io::Result<()> {
        self.notify(&[("STOPPING", "1")])
    }

    fn notify(&self, fields: &[(&str, &str)]) -> io::Result<()> {
        let Some(socket) = &self.socket else {
            return Ok(());
        };
        let mut message = String::new();
        for (index, (key, value)) in fields.iter().enumerate() {
            if index > 0 {
                message.push('\n');
            }
            message.push_str(key);
            message.push('=');
            message.push_str(value);
        }
        send_datagram(socket.as_os_str(), message.as_bytes())
    }
}

pub async fn run_status_task(
    notifier: Notifier,
    state: std::sync::Arc<RuntimeState>,
    mut shutdown: watch::Receiver<bool>,
) {
    let interval = notifier
        .watchdog_interval()
        .map(|watchdog| std::cmp::max(Duration::from_secs(1), watchdog / 2))
        .unwrap_or(Duration::from_secs(60));

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_ok() && *shutdown.borrow() {
                    break;
                }
            }
            () = sleep(interval) => {
                let status = state.status_message();
                if let Err(error) = notifier.status(&status) {
                    warn!(%error, "failed to send systemd status notification");
                }
                if notifier.watchdog_interval().is_some() {
                    if let Some(reason) = state.health_problem() {
                        warn!(%reason, "runtime health check failed; withholding systemd watchdog ping");
                        let _ = notifier.status(&format!("mailwake unhealthy: {reason}"));
                    } else {
                        if let Err(error) = notifier.watchdog() {
                            warn!(%error, "failed to send systemd watchdog notification");
                        }
                    }
                }
            }
        }
    }
}

fn sanitize_field(value: &str) -> String {
    value.replace(['\r', '\n'], " ")
}

fn watchdog_pid_allows(value: Option<&str>, current_pid: u32) -> bool {
    match value {
        None => true,
        Some(value) => value.parse::<u32>().is_ok_and(|pid| pid == current_pid),
    }
}

fn send_datagram(socket: &OsStr, message: &[u8]) -> io::Result<()> {
    let socket_bytes = socket.as_bytes();
    if socket_bytes.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "empty NOTIFY_SOCKET",
        ));
    }

    // systemd uses '@name' in NOTIFY_SOCKET to represent Linux abstract Unix
    // sockets. sockaddr_un represents that as a leading NUL byte.
    let is_abstract = socket_bytes[0] == b'@';
    let path_bytes = if is_abstract {
        &socket_bytes[1..]
    } else {
        socket_bytes
    };

    let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }

    let result = send_datagram_with_fd(fd, path_bytes, is_abstract, message);
    let close_result = unsafe { libc::close(fd) };
    if result.is_ok() && close_result < 0 {
        return Err(io::Error::last_os_error());
    }
    result
}

fn send_datagram_with_fd(
    fd: libc::c_int,
    path_bytes: &[u8],
    is_abstract: bool,
    message: &[u8],
) -> io::Result<()> {
    let mut addr: libc::sockaddr_un = unsafe { mem::zeroed() };
    addr.sun_family = libc::AF_UNIX as libc::sa_family_t;

    let path_capacity = addr.sun_path.len();
    let required = path_bytes.len() + usize::from(is_abstract);
    if required > path_capacity {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "NOTIFY_SOCKET path is too long",
        ));
    }

    let start = usize::from(is_abstract);
    for (index, byte) in path_bytes.iter().enumerate() {
        addr.sun_path[start + index] = *byte as libc::c_char;
    }

    let base = (&addr as *const libc::sockaddr_un).cast::<u8>() as usize;
    let path = (&addr.sun_path as *const libc::c_char).cast::<u8>() as usize;
    let offset = path - base;
    let addr_len = (offset + required) as libc::socklen_t;

    let sent = unsafe {
        libc::sendto(
            fd,
            message.as_ptr().cast(),
            message.len(),
            libc::MSG_NOSIGNAL,
            (&addr as *const libc::sockaddr_un).cast::<libc::sockaddr>(),
            addr_len,
        )
    };
    if sent < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_notifier_is_noop() {
        let notifier = Notifier::disabled();
        notifier.ready("mailwake started").expect("noop ready");
        notifier.watchdog().expect("noop watchdog");
        notifier.stopping().expect("noop stopping");
    }

    #[test]
    fn status_fields_are_single_line() {
        assert_eq!(sanitize_field("one\ntwo\rthree"), "one two three");
    }

    #[test]
    fn watchdog_pid_must_match_if_present() {
        assert!(watchdog_pid_allows(None, 42));
        assert!(watchdog_pid_allows(Some("42"), 42));
        assert!(!watchdog_pid_allows(Some("43"), 42));
        assert!(!watchdog_pid_allows(Some("not-a-pid"), 42));
    }
}
