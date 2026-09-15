//! Abstractions for write transport half.

use std::{io, sync::Arc};

use bytes::Bytes;
use futures_util::SinkExt as _;
use tokio::net::UdpSocket;
use tokio_util::codec::{FramedWrite, LengthDelimitedCodec};

use super::pipe::WriteHalf;

/// Write half that can be either UDP or Unix domain socket / Windows named pipe
/// with framing compatible with `cjdns/interface/FramingIface.c`.
pub enum Tx {
    Udp(UdpTx),
    Pipe(PipeTx),
}

impl Tx {
    /// Wrap a reference-counted UDP socket.
    pub fn from_udp(socket: Arc<UdpSocket>) -> Self {
        Tx::Udp(UdpTx::new(socket))
    }

    /// Wrap an owned write half of a Unix domain socket or a Windows named pipe,
    /// depending on the target system.
    pub fn from_pipe(write_half: WriteHalf) -> Self {
        Tx::Pipe(PipeTx::new(write_half))
    }

    /// Send a message using the underlying transport.
    pub async fn send(&mut self, msg: Bytes) -> io::Result<()> {
        match self {
            Tx::Udp(udp) => udp.send(msg).await,
            Tx::Pipe(pipe) => pipe.send(msg).await,
        }
    }
}

pub struct UdpTx {
    socket: Arc<UdpSocket>,
}

impl UdpTx {
    pub fn new(socket: Arc<UdpSocket>) -> Self {
        UdpTx { socket }
    }

    pub async fn send(&self, msg: Bytes) -> io::Result<()> {
        self.socket.send(&msg).await?;
        Ok(())
    }
}

pub struct PipeTx {
    framed_write: FramedWrite<WriteHalf, LengthDelimitedCodec>,
}

impl PipeTx {
    pub fn new(write_half: WriteHalf) -> Self {
        // LengthDelimitedCodec's default settings are already exactly those that we need
        // (4 byte header, big endian, zero offset, no adjustment for payload size)
        // https://docs.rs/tokio-util/0.7.10/tokio_util/codec/length_delimited/struct.Builder.html#impl-Builder
        PipeTx {
            framed_write: FramedWrite::new(write_half, LengthDelimitedCodec::new()),
        }
    }

    pub async fn send(&mut self, msg: Bytes) -> io::Result<()> {
        // Flushing into UDS is cheap, and the usage pattern is interactive
        self.framed_write.send(msg).await
    }
}
