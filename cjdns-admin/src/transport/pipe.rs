//! Abstractions for target-specific pipe endpoints (Unix domain sockets, Windows named pipes).

#[cfg(unix)]
mod sys {
    use std::{
        env, io,
        path::{Path, PathBuf},
    };

    use tokio::{fs, net::UnixStream};

    pub type ReadHalf = tokio::net::unix::OwnedReadHalf;
    pub type WriteHalf = tokio::net::unix::OwnedWriteHalf;

    pub async fn connect(path: &Path) -> io::Result<(ReadHalf, WriteHalf)> {
        UnixStream::connect(path).await.map(UnixStream::into_split)
    }

    pub async fn full_path(path: &Path) -> PathBuf {
        #[cfg(target_os = "android")]
        const DEFAULT_PATH: &str = "/data/local/tmp";
        #[cfg(not(target_os = "android"))]
        const DEFAULT_PATH: &str = "/tmp";

        let try_prefix = async |prefix: &Path| {
            let full = prefix.join(path);
            let exists = fs::try_exists(&full).await;
            if let Err(e) = &exists {
                log::info!("Attempted path '{}': {e}", full.to_string_lossy());
            }
            exists.unwrap_or(false).then_some(full)
        };
        let try_envvar = async |envvar| match env::var_os(envvar) {
            Some(prefix) => try_prefix(prefix.as_ref()).await,
            _ => None,
        };

        if let Some(xdg) = try_envvar("XDG_RUNTIME_DIR").await {
            return xdg;
        }
        if let Some(tmp) = try_envvar("TMPDIR").await {
            return tmp;
        }
        #[cfg(target_os = "android")]
        if let Some(home) = try_envvar("HOME").await {
            return home;
        }
        #[cfg(not(any(target_os = "android", target_os = "macos")))]
        if let Some(run) = try_prefix("/run".as_ref()).await {
            return run;
        }
        #[cfg(not(target_os = "android"))]
        if let Some(run) = try_prefix("/var/run".as_ref()).await {
            return run;
        }

        Path::new(DEFAULT_PATH).join(path)
    }
}

#[cfg(windows)]
mod sys {
    use std::{
        io,
        path::{Path, PathBuf},
    };

    use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeClient};

    pub type ReadHalf = tokio::io::ReadHalf<NamedPipeClient>;
    pub type WriteHalf = tokio::io::WriteHalf<NamedPipeClient>;

    pub async fn connect(path: &Path) -> io::Result<(ReadHalf, WriteHalf)> {
        ClientOptions::new().open(path).map(tokio::io::split)
    }

    pub async fn full_path(path: &Path) -> PathBuf {
        const DEFAULT_PATH: &str = r"\\.\pipe";

        Path::new(DEFAULT_PATH).join(path)
    }
}

#[cfg(not(any(unix, windows)))]
compile_error!("Unsupported system: This crate only works on Unix or Windows targets.");

use std::{
    io,
    path::{Path, PathBuf},
};

use eyre::eyre;

/// The underlying type for the read half.
///
/// On Unix and Unix-like targets, this resolves to [`tokio::net::unix::OwnedReadHalf`].
///
/// On Windows targets, this resolves to a [`tokio::io::ReadHalf`] wrapper over
/// [`tokio::net::windows::named_pipe::NamedPipeClient`].
pub type ReadHalf = sys::ReadHalf;

/// The underlying type for the write half.
///
/// On Unix and Unix-like targets, this resolves to [`tokio::net::unix::OwnedWriteHalf`].
///
/// On Windows targets, this resolves to a [`tokio::io::WriteHalf`] wrapper over
/// [`tokio::net::windows::named_pipe::NamedPipeClient`].
pub type WriteHalf = sys::WriteHalf;

/// Connect to a local pipe, depending on the target platform.
///
/// On Unix and Unix-like targets, this uses Unix domain sockets.
///
/// On Windows targets, this uses full duplex named pipes.
///
/// ## Arguments
///
///  * `path` - Absolute path to socket or pipe, e.g. `"/tmp/cjdroute.sock"` on Unix,
///    or `r"\\.\pipe\cjdroute.sock"` on Windows.
///
/// ## Returns
///
/// On success, this returns a pair of read and write halves, ready to be wrapped in
/// [`transport::Rx`] and [`transport::Tx`] correspondingly.
///
/// On error, this returns the system-level error of the connection attempt.
///
/// [`transport::Rx`]: crate::transport::Rx
/// [`transport::Tx`]: crate::transport::Tx
pub async fn connect(path: impl AsRef<Path>) -> io::Result<(ReadHalf, WriteHalf)> {
    let path = path.as_ref();
    sys::connect(path)
        .await
        .map_err(|e| io::Error::new(e.kind(), eyre!("Failed to connect to path '{}': {e}", path.to_string_lossy())))
}

/// Resolve a path to the socket or pipe.
///
/// ## Arguments
///
///  * `path` - Path to a socket or pipe which may be relative (e.g. `"cjdroute.sock"`) or absolute.
///
/// ## Returns
///
/// An absolute path which is the most reasonable candidate for the socket or pipe. If an absolute path
/// was supplied, it will be returned unchanged. Otherwise, target-specific heuristics will be applied
/// in an attempt to locate the intended socket or pipe. In particular, on Unix, it is important to ensure
/// that the socket already exists by the time this function is invoked with a relative path.
pub async fn full_path(path: impl AsRef<Path>) -> PathBuf {
    let path = path.as_ref();
    if path.is_absolute() {
        path.to_owned()
    } else {
        sys::full_path(path).await
    }
}
