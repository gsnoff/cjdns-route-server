//! Errors.

use std::{
    any::TypeId,
    fmt::{self, Formatter},
    sync::Arc,
    time::Duration,
};

use thiserror::Error;
use tokio::io;

use crate::ConnectionOptions;

/// This wrapper is needed because underlying `ConnectionOptions` is not intended to be made public type.
/// It is only useful to be printed on the screen.
#[derive(Clone, Default, PartialEq, Eq, Debug)]
pub struct ConnOptions(ConnectionOptions);

impl ConnOptions {
    pub(crate) fn wrap(opts: &ConnectionOptions) -> Self {
        ConnOptions(opts.clone())
    }

    fn descr(&self) -> String {
        use crate::ConnectionEndpoint::*;

        let mut msg = match &self.0.endpoint {
            Udp { addr, port } => format!("({addr}:{port})"),
            Pipe { path } => format!("({path})"),
        };

        if let Some(ref config_file) = self.0.used_config_file {
            msg += &format!(" using cjdnsadmin file at [{}]", config_file);
        }

        msg
    }
}

/// Error type for all cjdns admin operations.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Connection error - check the remote IP address and port.
    #[error("Could not find cjdns {}, see: https://github.com/cjdelisle/cjdnsadmin#connecting", .0.descr())]
    ConnectError(ConnOptions),

    /// Authentication error - check the password.
    #[error("Could not authenticate with CJDNS {}, see: https://github.com/cjdelisle/cjdnsadmin#authentication-issues", .0.descr())]
    AuthError(ConnOptions),

    /// Failed to read cjdnsadmin config file (`~/.cjdnsadmin` by default).
    #[error("Error reading config file: {0}")]
    ConfigFileRead(#[source] io::Error),

    /// Error parsing cjdnsadmin config file (`~/.cjdnsadmin` by default) - must be a valid JSON file.
    #[error("Bad config file: JSON parse error: {0}")]
    BadConfigFile(#[source] serde_json::Error),

    /// Failed to parse IPv4/IPv6 address.
    #[error("Address parse error: {0}")]
    BadNetworkAddress(#[source] std::net::AddrParseError),

    /// Server has closed the connection (valid for e.g. framed SOCK_STREAM UDS).
    /// This corresponds to `conn::dispatch::DispatchState::Completed` state.
    #[error("Connection is closed")]
    ConnectionClosed,

    /// Network I/O error.
    #[error("Network error: {0}")]
    NetworkOperation(#[source] io::Error),

    /// Failed to serialize/deserialize protocol message (using *bencode*).
    #[error("Encoding error: {0}")]
    Protocol(#[source] eyre::ErrReport),

    /// Remote invocation failed and returned `error` message.
    #[error("Remote call error: {0}")]
    RemoteError(String),

    /// Repeating transaction id within the same session. Supposed to be internal error.
    #[allow(missing_docs)]
    #[error("Repeating txid: {txid}")]
    RepeatingTx { txid: String },

    /// Network timeout error.
    #[error("Timeout occured: {0:?}")]
    TimeOut(Duration),

    /// Error on dispatch task side.
    #[error(transparent)]
    DispatchError(#[from] DispatchError),

    /// Dispatch task failed to deliver incoming message to its destination.
    #[error("Dispatch destination error: {0}")]
    DispatchDestination(&'static str),
}

/// Error type for dispatch-specific unrecoverable connection states.
#[derive(Debug, Clone, Error)]
#[non_exhaustive]
pub enum DispatchError {
    /// Dispatch task is unresponsive.
    /// This likely means that the task itself registers as active, however its request queue is in an invalid state.
    #[error("Dispatch task unresponsive")]
    Unresponsive,

    /// Dispatch task was canceled, e.g. with a call to `abort()`.
    #[error("Dispatch task canceled")]
    Canceled,

    /// Dispatch task has panicked.
    #[error("Dispatch task panicked with: {0}")]
    Panicked(PanicPayload),

    /// Dispatch task has encountered a fatal error.
    #[error(transparent)]
    FatalError(#[from] Arc<Error>),

    /// `DispatchState::refresh()` call has panicked for some reason.
    #[error("Dispatch handle is poisoned")]
    Poisoned,
}

/// Information about the panic payload.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum PanicPayload {
    /// The payload is an owned dynamic string message, likely constructed using argument formatting.
    Text(Arc<String>),

    /// The payload is a static string message, likely represented as a string literal.
    StaticText(&'static str),

    /// The payload is something unsupported that was sent via [`std::panic::panic_any`].
    UnsupportedType(TypeId),
}

impl fmt::Display for PanicPayload {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        use PanicPayload::*;
        match self {
            Text(text) => text.fmt(f),
            StaticText(text) => text.fmt(f),
            UnsupportedType(type_id) => write!(f, "Unsupported payload type: {type_id:?}"),
        }
    }
}
