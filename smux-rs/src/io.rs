//! Thin async I/O wrapper around `Arc<Stream>` with flush notify.
//!
//! Historically `SmuxIo` also carried KCP send-window backpressure
//! (`with_backpressure`). That coupling is removed: backpressure belongs on
//! the transport (`KcpStream`), not SMUX. `SmuxIo` remains a convenience newtype
//! so standalone `SmuxConn` and call sites that already hold a flush notify
//! can keep a small wrapper; prefer `Arc<Stream>` + `set_flush_notify` for new code.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::stream::{poll_read_into, Stream, StreamError};

/// Async I/O wrapper around an SMUX stream.
///
/// Implements `knet::AsyncRead + AsyncWrite`. Writing notifies `flush_notify`
/// so the session flush loop drains promptly.
pub struct SmuxIo {
    stream: Arc<Stream>,
    /// Wake the flush loop immediately when new data is written.
    flush_notify: Arc<knet::Notify>,
}

impl SmuxIo {
    /// Get the stream ID.
    #[inline]
    pub fn id(&self) -> u32 {
        self.stream.id()
    }

    /// Create a new `SmuxIo` that wakes `flush_notify` on write / shutdown.
    pub fn new(stream: Arc<Stream>, flush_notify: Arc<knet::Notify>) -> Self {
        // Keep Stream's optional notify in sync so direct Stream async writes
        // (if any) also wake the same loop.
        stream.set_flush_notify(flush_notify.clone());
        SmuxIo {
            stream,
            flush_notify,
        }
    }

    /// Shared `poll_write` logic.
    #[inline]
    fn do_poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        // Write-side flow control belongs to the stream's window even though
        // backpressure on the *transport* lives in `KcpStream`: the stream's
        // send buffer is where a fast producer with a slow consumer would
        // otherwise accumulate without bound (Go's `writeV2` blocks the caller
        // on the same window). The client and server pipes write through this
        // path, so it must not bypass `Stream::poll_send_capacity`.
        if self.stream.poll_send_capacity(cx).is_pending() {
            return Poll::Pending;
        }
        match self.stream.write(buf) {
            Ok(n) => {
                self.flush_notify.notify_one();
                Poll::Ready(Ok(n))
            }
            Err(StreamError::Closed) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "SMUX stream closed",
            ))),
            Err(_) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "SMUX stream write error",
            ))),
        }
    }
}

// ─── tokio AsyncRead / AsyncWrite ─────────────────────────────────────────────

impl knet::AsyncRead for SmuxIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut knet::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let space = buf.initialize_unfilled();
        match poll_read_into(&this.stream, cx.waker(), space) {
            Poll::Ready(Ok(0)) => Poll::Ready(Ok(())),
            Poll::Ready(Ok(n)) => {
                buf.advance(n);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl knet::AsyncWrite for SmuxIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().do_poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        log::debug!(
            "SmuxIo::poll_shutdown: marking stream {} local_closed",
            this.stream.id()
        );
        this.stream.mark_local_closed();
        this.flush_notify.notify_one();
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::Waker;

    /// The client and server pipes write through `SmuxIo`, so this path must
    /// apply the peer's window like `Stream` does. Regression: it used to
    /// bypass the check entirely, buffering everything a fast producer handed
    /// it (20 MB on one stream whose peer had consumed 1.2 MB).
    #[test]
    fn smux_io_write_blocks_at_the_peer_window() {
        let stream = Arc::new(Stream::with_buffer(1, 2 * 1024 * 1024));
        stream.apply_peer_update(0, 32 * 1024);
        let mut io = SmuxIo::new(stream.clone(), Arc::new(knet::Notify::new()));
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let chunk = vec![b'x'; 8 * 1024];

        for _ in 0..4 {
            assert!(matches!(
                io.do_poll_write(&mut cx, &chunk),
                Poll::Ready(Ok(_))
            ));
        }
        assert_eq!(stream.pending_send(), 32 * 1024);

        // The buffer holds the peer's whole window: the pipe parks here.
        assert!(matches!(io.do_poll_write(&mut cx, &chunk), Poll::Pending));

        // Flush drains it and the peer reports it consumed the data.
        let mut out = bytes::BytesMut::new();
        assert_eq!(stream.drain_send_max(&mut out, usize::MAX), 32 * 1024);
        stream.apply_peer_update(32 * 1024, 32 * 1024);
        assert!(matches!(
            io.do_poll_write(&mut cx, &chunk),
            Poll::Ready(Ok(_))
        ));
    }
}
