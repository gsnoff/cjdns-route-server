//! Abstractions for read transport half.

use std::{io, pin::Pin, sync::Arc};

use bytes::BytesMut;
use futures_util::{Stream, StreamExt as _};
use tokio::net::UdpSocket;
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

use super::pipe::ReadHalf;

const MAX_MTU: usize = 0x10000;

/// Read half that can be either UDP or Unix domain socket / Windows named pipe
/// with framing compatible with `cjdns/interface/FramingIface.c`.
///
/// This also provides a mock variant for isolated unit testing.
pub enum Rx {
    Udp(UdpRx),
    Pipe(PipeRx),
    Mock(MockRx),
}

impl Rx {
    /// Wrap a reference-counted UDP socket.
    pub fn from_udp(socket: Arc<UdpSocket>) -> Self {
        Rx::Udp(UdpRx::new(socket))
    }

    /// Wrap an owned read half of a Unix domain socket or a Windows named pipe,
    /// depending on the target system.
    pub fn from_pipe(read_half: ReadHalf) -> Self {
        Rx::Pipe(PipeRx::new(read_half))
    }

    /// Wrap an arbitrary object implementing [`Stream`] for mock testing.
    ///
    /// [`Stream`]: futures_util::Stream
    #[allow(dead_code)] // Not really cleaner than #[cfg(test)] due to transitive dependencies
    pub fn from_mock<S>(stream: S) -> Self
    where
        S: 'static + Stream<Item = BytesMut> + Send + Sync,
    {
        Rx::Mock(MockRx::new(stream))
    }

    /// Receive a message using the underlying transport.
    ///
    /// # Return
    ///
    /// Returns `Ok(Some(msg))` with the message in `msg` if it was successfully read,
    /// or `Ok(None)` if the connection was gracefully shut down by the other side.
    ///
    /// # Cancel safety
    ///
    /// This method is always cancel safe, regardless of the underlying transport.
    pub async fn recv(&mut self) -> io::Result<Option<BytesMut>> {
        match self {
            Rx::Udp(udp) => udp.recv().await.map(Some),
            Rx::Pipe(pipe) => pipe.recv().await,
            Rx::Mock(mock) => mock.recv().await,
        }
    }
}

pub struct UdpRx {
    socket: Arc<UdpSocket>,
    buffer: BytesMut,
}

impl UdpRx {
    pub fn new(socket: Arc<UdpSocket>) -> Self {
        UdpRx {
            socket,
            buffer: BytesMut::with_capacity(MAX_MTU),
        }
    }

    pub async fn recv(&mut self) -> io::Result<BytesMut> {
        loop {
            self.buffer.reserve(MAX_MTU);
            let len = self.socket.recv_buf(&mut self.buffer).await?; // Cancel safe
            if len > 0 {
                return Ok(self.buffer.split());
            }
            // Ignore empty payloads, treat them as heartbeat / hole punching
        }
    }
}

pub struct PipeRx {
    framed_read: FramedRead<ReadHalf, LengthDelimitedCodec>,
}

impl PipeRx {
    pub fn new(read_half: ReadHalf) -> Self {
        // LengthDelimitedCodec's default settings are already exactly those that we need
        // (4 byte header, big endian, zero offset, no adjustment for payload size)
        // https://docs.rs/tokio-util/0.7.10/tokio_util/codec/length_delimited/struct.Builder.html#impl-Builder
        PipeRx {
            framed_read: LengthDelimitedCodec::builder().max_frame_length(MAX_MTU).new_read(read_half),
        }
    }

    pub async fn recv(&mut self) -> io::Result<Option<BytesMut>> {
        // Also cancel safe
        self.framed_read.next().await.transpose()
    }
}

pub struct MockRx {
    stream: Pin<Box<dyn Stream<Item = BytesMut> + Send + Sync>>,
}

impl MockRx {
    pub fn new<S>(stream: S) -> Self
    where
        S: 'static + Stream<Item = BytesMut> + Send + Sync,
    {
        MockRx { stream: Box::pin(stream) }
    }

    pub async fn recv(&mut self) -> io::Result<Option<BytesMut>> {
        Ok(self.stream.next().await)
    }
}
