//! Dispatch routines for incoming messages based on `txid` and `streamId`.

use std::{mem, sync::Arc};

use futures_util::FutureExt as _;
use tokio::{
    select,
    sync::{mpsc, oneshot, Notify},
    task::{self, JoinHandle},
};

use super::Response;
use crate::{
    errors::{DispatchError, Error, PanicPayload},
    transport::Rx,
};

const REQUEST_QUEUE_BOUND: usize = 0x10;
const STREAM_QUEUE_BOUND: usize = 0x100;
const STATE_CLEANUP_PERIOD: u64 = 0x100;
const BENCODE_MAX_RECURSION: u32 = 0x20;

pub struct Dispatch {
    state: DispatchState,
    request_queue: mpsc::Sender<RequestInfo>,
    abort_on_drop: bool,
}

impl Dispatch {
    /// Start a dispatch task instance in `tokio` runtime context.
    ///
    /// ## Arguments
    ///
    ///  * `rx` - Transport read half.
    ///  * `abort_on_drop` - Whether to abort the `tokio` task automatically once the `Dispatch` object is dropped.
    ///    On `false` this will instead attempt to shut down gracefully, by having the request queue closed,
    ///    as it will no longer have any senders.
    pub fn start(rx: Rx, abort_on_drop: bool) -> Self {
        let (request_queue, reqs) = mpsc::channel(REQUEST_QUEUE_BOUND);
        Dispatch {
            state: DispatchState::Pending(task::spawn(do_dispatch(rx, reqs))),
            request_queue,
            abort_on_drop,
        }
    }

    /// Abort the dispatch task immediately.
    pub fn abort(&self) {
        if let DispatchState::Pending(j) = &self.state {
            j.abort();
        }
    }

    /// Try to reason about the erroneous state which caused the request processing failure.
    ///
    /// This exists as a separate function since the owner of `Dispatch` instance may have it wrapped
    /// in something like [`tokio::sync::RwLock`], and write guards are really unnecessary outside of
    /// [`tokio::task::JoinHandle`] result checks.
    pub fn query_error(&mut self) -> Error {
        self.state.refresh();
        match &self.state {
            DispatchState::Pending(_) => Error::DispatchError(DispatchError::Unresponsive),
            DispatchState::Completed => Error::ConnectionClosed,
            DispatchState::Aborted(err) => Error::DispatchError(err.clone()),
        }
    }

    /// Register an outgoing request by its `txid` for dispatch.
    ///
    /// ## Arguments
    ///
    ///  * `txid` - CJDNS admin API transaction ID.
    ///
    /// ## Returns
    ///
    /// On success, this returns a oneshot receiver for response message, including a stream receiver
    /// if a stream ID was provided. The dispatch task will be ready to process and dispatch a response,
    /// and the caller may proceed to send the request message immediately.
    ///
    /// On error, this returns [`DispatchError::Unresponsive`] as preliminary error value.
    /// The actual cause will have to be queried via [`query_error`],
    /// possibly after swapping [`RwLockReadGuard`] for [`RwLockWriteGuard`].
    ///
    /// [`DispatchError::Unresponsive`]: crate::errors::DispatchError::Unresponsive
    /// [`query_error`]: Self::query_error
    /// [`RwLockReadGuard`]: tokio::sync::RwLockReadGuard
    /// [`RwLockWriteGuard`]: tokio::sync::RwLockWriteGuard
    pub async fn register_request(&self, txid: String) -> Result<ResponseReceiver, DispatchError> {
        let (response_slot, receiver) = oneshot::channel();
        let ready = Arc::new(Notify::new());
        if self
            .request_queue
            .send(RequestInfo {
                txid,
                response_slot,
                ready: Arc::clone(&ready),
            })
            .await
            .is_ok()
        {
            ready.notified().await;
            Ok(receiver)
        } else {
            Err(DispatchError::Unresponsive)
        }
    }
}

impl Drop for Dispatch {
    fn drop(&mut self) {
        if self.abort_on_drop {
            self.abort();
        }
    }
}

pub type ResponseReceiver = oneshot::Receiver<Result<Response, Error>>;

type ResponseSlot = oneshot::Sender<Result<Response, Error>>;

struct RequestInfo {
    /// Transaction ID associated with the request.
    txid: String,

    /// Oneshot instance to send response value or error to.
    response_slot: ResponseSlot,

    /// Used to notify the parent task when the dispatch task is ready to handle the response.
    ready: Arc<Notify>,
}

/// State of the dispatch task.
enum DispatchState {
    /// The task handle is present.
    ///
    /// This does not necessarily mean the task itself is active, though.
    /// One might want to refresh the state before querying it.
    Pending(JoinHandle<Result<(), Error>>),

    /// The task is completed due to the transport's receiving half (`Rx`) being closed.
    Completed,

    /// The task is aborted due to an internal error.
    Aborted(DispatchError),
}

impl DispatchState {
    fn refresh(&mut self) {
        use DispatchState::*;
        *self = match mem::replace(self, Aborted(DispatchError::Poisoned)) {
            Pending(j) if j.is_finished() => match j.now_or_never().unwrap() {
                Ok(Ok(_)) => Completed,
                Ok(Err(err)) => Aborted(DispatchError::FatalError(Arc::new(err))),
                Err(err) => {
                    if let Ok(p) = err.try_into_panic() {
                        Aborted(DispatchError::Panicked(
                            p.downcast::<&'static str>()
                                .map(|s| PanicPayload::StaticText(*s))
                                .or_else(|p| p.downcast::<String>().map(|s| PanicPayload::Text(Arc::from(s))))
                                .unwrap_or_else(|p| PanicPayload::UnsupportedType((*p).type_id())),
                        ))
                    } else {
                        Aborted(DispatchError::Canceled)
                    }
                }
            },
            other => other,
        };
    }
}

async fn do_dispatch(mut rx: Rx, mut reqs: mpsc::Receiver<RequestInfo>) -> Result<(), Error> {
    let mut state = inner::State::default();
    let mut rx_cnt = 0u64;
    loop {
        select! {
            result = rx.recv() => match result {
                Ok(Some(msg)) => {
                    let msg = msg.freeze();
                    log::trace!("Received message: {}", String::from_utf8_lossy(&msg));
                    if let Err(err) = state.dispatch_message(msg.clone()) {
                        log::warn!("{err}; message: {}", String::from_utf8_lossy(&msg));
                    }
                }
                Ok(None) => {
                    log::debug!("Dispatch: Remote host closed connection");
                    return Ok(());
                }
                Err(err) => return Err(Error::NetworkOperation(err)),
            },
            req = reqs.recv() => if let Some(req) = req {
                state.handle_request(req);
            } else {
                log::debug!("Dispatch: Queue shutdown");
                return Ok(());
            }
        }

        rx_cnt += 1;
        if rx_cnt.is_multiple_of(STATE_CLEANUP_PERIOD) {
            state.cleanup();
        }
    }
}

mod inner {
    use std::collections::hash_map::{Entry, HashMap};

    use bencode::object::{Dict, Get as _};
    use bytes::{Buf, Bytes};
    use eyre::eyre;
    use tokio::sync::mpsc;

    use super::{RequestInfo, Response, ResponseSlot, BENCODE_MAX_RECURSION, STREAM_QUEUE_BOUND};
    use crate::Error;

    #[derive(Default)]
    pub struct State {
        requests: HashMap<String, ResponseSlot>,
        streams: HashMap<String, mpsc::Sender<Dict<'static>>>,
    }

    impl State {
        pub fn cleanup(&mut self) {
            self.requests.retain(|_, slot| !slot.is_closed());
            self.streams.retain(|_, tx| !tx.is_closed());
        }

        pub fn dispatch_message(&mut self, msg: Bytes) -> Result<(), Error> {
            let message = decode_message(msg.clone())?;
            let meta = MessageMetadata::read_from(&message)?;

            // Route message depending on known txid and streamId values
            if let Some(slot) = self.requests.remove(meta.txid) {
                let response;
                if let Some(emsg) = meta.error {
                    response = Err(Error::RemoteError(emsg.to_owned()));
                } else {
                    let stream = meta.stream_id.map(|s| {
                        let (tx, rx) = mpsc::channel(STREAM_QUEUE_BOUND);
                        self.streams.insert(s.to_owned(), tx);
                        rx
                    });
                    response = Ok(Response { message, stream });
                }
                slot.send(response)
                    .map_err(|_| Error::DispatchDestination("Recipient for response message is closed"))
            } else if let Some(stream_id) = meta.stream_id {
                if let Some(stream) = self.streams.get(stream_id) {
                    let stream_id = stream_id.to_owned();
                    stream.try_send(message).map_err(|_| {
                        self.streams.remove(&stream_id);
                        Error::DispatchDestination("Failed to handle stream message, unsubscribing")
                    })
                } else {
                    Err(Error::DispatchDestination("Unknown streamId in incoming message"))
                }
            } else {
                Err(Error::DispatchDestination("Unknown txid in incoming message"))
            }
        }

        pub fn handle_request(&mut self, req: RequestInfo) {
            match self.requests.entry(req.txid) {
                Entry::Occupied(entry) => {
                    if req.response_slot.send(Err(Error::RepeatingTx { txid: entry.key().clone() })).is_err() {
                        log::debug!("Recipient for recurring txid is closed: {}", entry.key());
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(req.response_slot);
                }
            }
            req.ready.notify_one();
        }
    }

    fn decode_message(msg: Bytes) -> Result<Dict<'static>, Error> {
        let mut reader = msg.clone().reader();
        let res = cjdns_bencode::standard::Parser::<BENCODE_MAX_RECURSION, true>::parse(&mut reader)
            .map_err(|e| Error::Protocol(eyre::eyre!("Error parsing incoming message: {e}")))?
            .into_dict()
            .map_err(|_| Error::Protocol(eyre::eyre!("Incoming message is not a dict")))?;

        Ok(res.into_owned())
    }

    struct MessageMetadata<'a> {
        txid: &'a str,
        error: Option<&'a str>,
        stream_id: Option<&'a str>,
    }

    impl<'a> MessageMetadata<'a> {
        fn read_from(src: &'a Dict<'a>) -> Result<Self, Error> {
            let txid = src
                .try_get_str("txid")
                .map_err(|e| Error::Protocol(eyre!("Error getting txid: {e}")))?
                .ok_or_else(|| Error::Protocol(eyre!("txid missing")))?;
            let error = match src.try_get_str("error") {
                Ok(Some(emsg)) if emsg.is_empty() || emsg.eq_ignore_ascii_case("none") => None,
                Ok(emsg) => emsg,
                Err(e) => {
                    log::info!("Dispatch: txid '{txid}': Failed to get error message: {e}");
                    Some("unrecognized error")
                }
            };
            let stream_id = match src.try_get_str("streamId") {
                Ok(id) => id,
                Err(e) => {
                    log::info!("Dispatch: txid '{txid}': Failed to get streamId: {e}");
                    None
                }
            };
            Ok(MessageMetadata { txid, error, stream_id })
        }
    }
}

#[cfg(test)]
mod test {
    use std::io::Read;

    use bencode::object::{Get as _, Object};
    use bytes::BytesMut;
    use cjdns_bytes::message::Message;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    use super::Dispatch;
    use crate::{conn::Rx, dict, Error};

    #[tokio::test]
    async fn test_dispatch() {
        const ERROR_MSG: &str = "everything went wrong exactly as planned";
        const MOTD: &str = "Code works on my machine. Good luck.";

        let (tx, rx) = mpsc::channel(0x10);
        let disp = Dispatch::start(Rx::from_mock(ReceiverStream::new(rx)), true);

        let foo_res = disp.register_request("foo".to_owned()).await.unwrap();
        let bar_res = disp.register_request("bar".to_owned()).await.unwrap();
        let baz_res = disp.register_request("baz".to_owned()).await.unwrap();

        let msgs = [
            dict!(txid = "foo", streamId = "fooStream", status = "ok", error = "none"),
            dict!(txid = "foo", streamId = "fooStream", value = 1),
            dict!(txid = "foo", streamId = "fooStream", value = 2),
            dict!(txid = "foo", streamId = "fooStream", value = 3),
            dict!(txid = "baz", error = ERROR_MSG),
            dict!(txid = "foo", streamId = "fooStream", value = 42),
            dict!(txid = "bar", status = "ok", motd = MOTD),
        ];

        for msg in msgs {
            let mut mbuf = Message::new();
            cjdns_bencode::standard::serialize(&mut mbuf, &Object::from(msg), 0).unwrap();
            let mut bytes = BytesMut::new();
            bytes.extend(mbuf.bytes().map(Result::unwrap));
            tx.try_send(bytes).unwrap();
        }

        let foo = foo_res.await.unwrap().unwrap();
        let bar = bar_res.await.unwrap().unwrap();
        let baz_err = baz_res.await.unwrap().unwrap_err();

        for res in [&foo, &bar] {
            assert_eq!(res.message.get_str("status").unwrap(), "ok");
        }

        assert_eq!(format!("{baz_err:?}"), format!("{:?}", Error::RemoteError(ERROR_MSG.to_owned())));

        // At this point, the dispatch task would have processed all messages down to `txid = "bar"`
        let mut foo_stream = foo.stream.unwrap();
        let mut values = [1, 2, 3, 42].iter().copied();
        while let Ok(msg) = foo_stream.try_recv() {
            assert_eq!(msg.get_int("value").unwrap(), values.next().unwrap());
        }
        assert!(values.next().is_none());

        assert!(bar.stream.is_none());

        assert_eq!(bar.message.get_str("motd").unwrap(), MOTD.to_owned());
    }

    // TODO integration tests
}
