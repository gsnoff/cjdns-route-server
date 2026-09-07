//! Routines for connection to the CJDNS Router.

mod dispatch;

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use bencode::object::{Dict, Get as _, Object};
use cjdns_bytes::message::Message;
use eyre::eyre;
use sodiumoxide::crypto::hash::sha256::hash;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::time;

use self::dispatch::Dispatch;
use crate::errors::{ConnOptions, Error};
use crate::func_list::Funcs;
use crate::transport::{pipe, Rx, Tx};
use crate::txid::Counter;
use crate::{dict, ConnectionEndpoint, ConnectionOptions};

const PING_TIMEOUT: Duration = Duration::from_millis(1_000);
const DEFAULT_TIMEOUT: Duration = Duration::from_millis(10_000);

/// Admin connection to the CJDNS node.
///
/// Cloneable: cloned connection uses same underlying transport and is thread-safe.
#[derive(Clone)]
pub struct Connection {
    state: Arc<ConnectionSharedState>,
    password: String,

    /// List of available remote functions.
    pub functions: Funcs,
}

impl Connection {
    pub(super) async fn new(opts: ConnectionOptions) -> Result<Self, Error> {
        let mut conn = Connection {
            state: Arc::new(ConnectionSharedState::new(&opts.endpoint).await?),
            password: opts.password.clone(),
            functions: Funcs::default(),
        };

        conn.probe_connection(opts).await?;
        let fns = conn.load_available_functions().await?;
        conn.functions = fns;

        Ok(conn)
    }

    async fn probe_connection(&mut self, opts: ConnectionOptions) -> Result<(), Error> {
        self.call_func("ping", Dict::new(), true, PING_TIMEOUT)
            .await
            .map_err(|_| Error::ConnectError(ConnOptions::wrap(&opts)))?;

        if !self.password.is_empty() {
            self.call_func("AuthorizedPasswords_list", Dict::new(), false, DEFAULT_TIMEOUT)
                .await
                .map_err(|_| Error::AuthError(ConnOptions::wrap(&opts)))?;
        }

        Ok(())
    }

    async fn load_available_functions(&mut self) -> Result<Funcs, Error> {
        let mut res = Funcs::new();

        for i in 0.. {
            let ret = self.call_func("Admin_availableFunctions", dict!(page = i), false, DEFAULT_TIMEOUT).await?;
            let funcs = ret
                .message
                .get_dict("availableFunctions")
                .map_err(|e| Error::Protocol(eyre!("Failed getting availableFunctions {e}")))?;

            if funcs.is_empty() {
                break; // Empty answer - no more pages
            }

            res.add_funcs(funcs).map_err(Error::Protocol)?;
        }

        Ok(res)
    }

    /// Call remote function on CJDNS router.
    pub async fn invoke(&self, remote_fn_name: &str, args: Dict<'_>) -> Result<Dict<'static>, Error> {
        let res = self.call_func(remote_fn_name, args, false, DEFAULT_TIMEOUT).await?;
        if res.stream.is_some() {
            log::warn!("Remote fn {remote_fn_name}: dropping stream subscription; use invoke_subscribe");
        }
        Ok(res.message)
    }

    /// Call remote function on CJDNS router and subscribe to stream.
    pub async fn invoke_subscribe(&self, remote_fn_name: &str, args: Dict<'_>) -> Result<Response, Error> {
        self.call_func(remote_fn_name, args, false, DEFAULT_TIMEOUT).await
    }

    async fn call_func(&self, remote_fn_name: &str, args: Dict<'_>, disable_auth: bool, timeout: Duration) -> Result<Response, Error> {
        let call = async {
            if disable_auth || self.password.is_empty() {
                self.call_func_no_auth(remote_fn_name, args).await
            } else {
                self.call_func_auth(remote_fn_name, args).await
            }
        };
        time::timeout(timeout, call).await.map_err(|_| Error::TimeOut(timeout))?
    }

    async fn call_func_no_auth(&self, remote_fn_name: &str, args: Dict<'_>) -> Result<Response, Error> {
        let txid = self.state.next_txid();
        self.state.send_request(dict!(txid = &txid, q = remote_fn_name, args), &txid).await
    }

    async fn call_func_auth(&self, remote_fn_name: &str, args: Dict<'_>) -> Result<Response, Error> {
        // Ask cjdns for a cookie first
        let cookie = {
            let resp = self.call_func_no_auth("cookie", Dict::new()).await?;
            resp.message
                .try_get_str("cookie")
                .map_err(|e| Error::Protocol(eyre!("Error getting cookie: {e}")))?
                .ok_or_else(|| Error::Protocol(eyre!("cookie missing")))?
                .to_string()
        };

        // Hash password with salt
        let passwd_hash = {
            let cookie_passwd = self.password.clone() + &cookie;
            let digest = hash(cookie_passwd.as_bytes());
            hex::encode(digest)
        };

        let txid = self.state.next_txid();

        // Prepare message with initial hash
        let mut req = dict!(txid = &txid, q = "auth", aq = remote_fn_name, args, cookie, hash = passwd_hash);

        // Update message's hash
        let msg_hash = {
            let mut msg = Message::new();
            cjdns_bencode::standard::serialize(&mut msg, &Object::from(req.clone()), 0).map_err(Error::Protocol)?;
            let digest = hash(&msg.as_vec());
            hex::encode(digest)
        };
        req.insert("hash", msg_hash);

        // Send/receive
        self.state.send_request(req, &txid).await
    }
}

struct ConnectionSharedState {
    tx: Mutex<Tx>,
    dispatch: RwLock<Dispatch>,
    counter: Counter,
}

impl ConnectionSharedState {
    async fn new(endpoint: &ConnectionEndpoint) -> Result<Self, Error> {
        let (tx, rx) = match endpoint {
            ConnectionEndpoint::Udp { addr, port } => {
                let ip_addr = addr.parse::<IpAddr>().map_err(Error::BadNetworkAddress)?;
                let remote_address = SocketAddr::new(ip_addr, *port);

                let local_address = match ip_addr {
                    IpAddr::V4(_) => "0.0.0.0:0",
                    IpAddr::V6(_) => "[::]:0",
                };
                let socket = Arc::new(UdpSocket::bind(local_address).await.map_err(Error::NetworkOperation)?);
                socket.connect(&remote_address).await.map_err(Error::NetworkOperation)?;
                (Tx::from_udp(Arc::clone(&socket)), Rx::from_udp(socket))
            }
            ConnectionEndpoint::Pipe { path } => {
                let (read_half, write_half) = pipe::connect(pipe::full_path(path).await).await.map_err(Error::NetworkOperation)?;
                (Tx::from_pipe(write_half), Rx::from_pipe(read_half))
            }
        };
        Ok(ConnectionSharedState {
            tx: Mutex::new(tx),
            dispatch: RwLock::new(Dispatch::start(rx, false)),
            counter: Counter::new_random(),
        })
    }

    fn next_txid(&self) -> String {
        self.counter.next().to_string()
    }

    async fn send_request(&self, req: Dict<'_>, txid: &str) -> Result<Response, Error> {
        let mut msg = Message::new();
        cjdns_bencode::standard::serialize(&mut msg, &Object::from(req), 0).map_err(Error::Protocol)?;
        log::trace!("Sending message: {}", String::from_utf8_lossy(&msg.as_vec()));

        // Register request for dispatch
        let reg_result = self.dispatch.read().await.register_request(txid.to_owned()).await;
        // Read guard is dropped here
        let receiver = match reg_result {
            Ok(r) => r,
            Err(_) => return Err(self.dispatch.write().await.query_error()),
        };

        // Send request
        self.tx.lock().await.send(msg.as_vec().into()).await.map_err(Error::NetworkOperation)?;

        // Await for response
        match receiver.await {
            Ok(r) => r,
            Err(_) => Err(self.dispatch.write().await.query_error()),
        }
    }
}

/// Represents both the immediate response to sent request, and the stream prompted by the request, if any.
#[derive(Debug)]
pub struct Response {
    /// Response message.
    pub message: Dict<'static>,

    /// Stream subscription, if stream ID was specified.
    ///
    /// In CJDNS bencode protocol, there is no designated stream terminator message.
    /// If [`recv`] returns `None`, or [`try_recv`] returns [`Disconnected`], this means either that the dispatch task is down,
    /// or that the queue was overrun, and the sender was dropped in order to avoid deadlocks
    /// in the interaction of the dispatch task with its client. In general, this behavior is considered safer than
    /// dropping individual messages from the stream.
    ///
    /// [`recv`]: tokio::sync::mpsc::Receiver::recv
    /// [`try_recv`]: tokio::sync::mpsc::Receiver::try_recv
    /// [`Disconnected`]: tokio::sync::mpsc::error::TryRecvError::Disconnected
    pub stream: Option<mpsc::Receiver<Dict<'static>>>,
}
