//! CJDNS Admin lib

#![deny(missing_docs)]

pub use crate::config::Opts;
pub use crate::conn::Connection;
pub use crate::errors::Error;
pub use crate::func_args::{ArgName, ArgValue, ArgValues};
pub use crate::func_list::{Arg, Args, ArgType, Func, Funcs};
pub use crate::func_ret::ReturnValue;

mod config;
mod conn;
mod errors;
mod func_args;
mod func_list;
mod func_ret;
mod txid;
pub mod msgs;

#[derive(Clone, Default, PartialEq, Eq, Debug)]
struct ConnectionOptions {
    addr: String,
    port: u16,
    password: String,
    used_config_file: Option<String>,
}

/// Connect to the running cjdns router instance.
/// If `opts` is not provided, the default config file is read.
/// or only specified config file name,
/// the corresponding config file is read.
pub async fn connect(opts: Option<Opts>) -> Result<Connection, Error> {
    let opts = opts.unwrap_or_default().into_connection_options().await?;
    conn::Connection::new(opts).await
}