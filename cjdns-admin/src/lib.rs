//! CJDNS Admin lib

#![deny(missing_docs)]

pub extern crate cjdns_bencode as bencode;

mod config;
mod conn;
mod errors;
mod func_list;
mod transport;
mod txid;

use crate::config::{DEFAULT_ADDR, DEFAULT_PASSWORD, DEFAULT_PORT};

pub use crate::config::{EndpointType, Opts};
pub use crate::conn::{Connection, Response};
pub use crate::errors::*;
pub use crate::func_list::{Arg, ArgType, Args, Func, Funcs};

#[derive(Clone, PartialEq, Eq, Debug)]
struct ConnectionOptions {
    endpoint: ConnectionEndpoint,
    password: String,
    used_config_file: Option<String>,
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        ConnectionOptions {
            endpoint: ConnectionEndpoint::Udp {
                addr: DEFAULT_ADDR.to_owned(),
                port: DEFAULT_PORT,
            },
            password: DEFAULT_PASSWORD.to_owned(),
            used_config_file: None,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
enum ConnectionEndpoint {
    Udp { addr: String, port: u16 },
    Pipe { path: String },
}

/// Connect to the running cjdns router instance.
/// If `opts` is not provided, the default config file is read.
/// or only specified config file name,
/// the corresponding config file is read.
pub async fn connect(opts: Option<Opts>) -> Result<Connection, Error> {
    let opts = opts.unwrap_or_default().into_connection_options().await?;
    conn::Connection::new(opts).await
}

#[doc(hidden)]
#[macro_export]
macro_rules! __expand_arg {
    ($dict:expr; $arg_name:ident = $arg_value:expr) => {
        $dict.insert(stringify!{$arg_name}, $arg_value)
    };
    ($dict:expr; $arg_name:ident? = $arg_value:expr) => {
        if let Some(arg_value) = $arg_value {
            $crate::__expand_arg!($dict; $arg_name = arg_value);
        }
    };
    ($dict:expr; $arg_name:ident) => {
        $crate::__expand_arg!($dict; $arg_name = $arg_name)
    };
    ($dict:expr; $arg_name:ident?) => {
        $crate::__expand_arg!($dict; $arg_name? = $arg_name)
    };
    ($dict:expr; $arg_name:literal = $arg_value:expr) => {
        $dict.insert($arg_name, $arg_value)
    };
    ($dict:expr; $arg_name:literal? = $arg_value:expr) => {
        if let Some(arg_value) = $arg_value {
            $crate::__expand_arg!($dict; $arg_name = arg_value);
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __expand_args {
    // Handle comma separator eagerly
    ($dict:expr; [$( $acc:tt )+] $(, $( $tail:tt )* )?) => {
        $crate::__expand_arg!($dict; $($acc)+);
        $($crate::__expand_args!($dict; [] $($tail)*))?
    };
    // Munch other tokens into accumulator
    ($dict:expr; [$( $acc:tt )*] $head:tt $( $tail:tt )*) => {
        $crate::__expand_args!($dict; [$($acc)* $head] $($tail)*);
    };
    // Matches after trailing comma
    ($dict:expr; []) => {};
}

/// Helper macro to easily construct a nested `Dict` object.
///
/// ## Argument syntax
///
/// This macro accepts a comma-separated list of argument assignments,
/// each of which may be any of the following:
///
///  * `name = value`, for required argument with explicit value expression
///  * `name? = value`, for optional argument with explicit value expression evaluating to an `Option`,
///    which will be assigned the `Some` value or skipped in case of `None`
///  * `name`, shorthand form for when there exists a variable with the same name in the current scope,
///    in order to use its value for assignment
///  * `name?`, shorthand form for using a variable with the same name of the `Option` type
///  * `"name" = value`, for cases when an argument name cannot be expressed as a legitimate Rust identifier,
///    e.g. it contains characters like hyphen (`-`)
///  * `"name"? = value`, same for optional arguments with `Option` value
///
/// ## Examples
///
/// ```no_run
/// # use cjdns_admin::{dict, cjdns_invoke};
/// # async fn test_dict() -> Result<(), Box<dyn std::error::Error>> {
/// # let mut conn = cjdns_admin::connect(None).await?;
/// let (bar, baz) = ("value3", Some("value4"));
/// let res = cjdns_invoke!(
///     conn,
///     "FuncName",
///     foo = 42,
///     args = dict!(
///         arg1 = "value1",
///         arg2? = Some("value2"),
///         arg3 = dict!(bar, baz?),
///         "arg-with-hyphens" = "value5",
///     ),
/// ).await?;
/// # Ok(())}
/// ```
#[macro_export]
macro_rules! dict {
    ($( $args:tt )*) => {{
        let mut d = $crate::bencode::object::Dict::new();
        $crate::__expand_args!(d; [] $($args)*);
        d
    }}
}

/// Helper macro to easily invoke remote function with arguments.
/// See the [`dict`] macro for argument syntax.
///
/// ## Examples
///
/// ```no_run
/// # use cjdns_admin::cjdns_invoke;
/// # async fn test_invoke() -> Result<(), Box<dyn std::error::Error>> {
/// # let mut conn = cjdns_admin::connect(None).await?;
/// # let (arg4, arg5) = ("arg4", Some("arg5"));
/// let res = cjdns_invoke!(conn, "FuncName").await?;
/// let res = cjdns_invoke!(conn, "FuncName", arg1 = 42, arg2 = "foobar", arg3? = Some("baz"), arg4, arg5?).await?;
/// # Ok(())}
/// ```
#[macro_export]
macro_rules! cjdns_invoke {
    ($cjdns:expr, $fn_name:literal) => {
        $cjdns.invoke($fn_name, $crate::bencode::object::Dict::new())
    };
    ($cjdns:expr, $fn_name:literal, $( $args:tt )*) => {
        $cjdns.invoke($fn_name, $crate::dict!($($args)*))
    };
}

/// Helper macro to easily invoke remote function with arguments and subscribe to stream.
/// See the [`dict`] macro for argument syntax.
///
/// ## Examples
///
/// ```no_run
/// # use cjdns_admin::cjdns_invoke_subscribe;
/// # async fn test_invoke_subscribe() -> Result<(), Box<dyn std::error::Error>> {
/// # let mut conn = cjdns_admin::connect(None).await?;
/// # let (arg4, arg5) = ("arg4", Some("arg5"));
/// let res = cjdns_invoke_subscribe!(conn, "FuncName").await?;
/// let res = cjdns_invoke_subscribe!(conn, "FuncName", arg1 = 42, arg2 = "foobar", arg3? = Some("baz"), arg4, arg5?).await?;
/// # Ok(())}
/// ```
#[macro_export]
macro_rules! cjdns_invoke_subscribe {
    ($cjdns:expr, $fn_name:literal) => {
        $cjdns.invoke_subscribe($fn_name, $crate::bencode::object::Dict::new())
    };
    ($cjdns:expr, $fn_name:literal, $( $args:tt )*) => {
        $cjdns.invoke_subscribe($fn_name, $crate::dict!($($args)*))
    };
}
