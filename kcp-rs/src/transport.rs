//! Datagram transport layer for the async `KcpStream`.
//!
//! [`PacketTransport`] is the pluggable packet-delivery abstraction under
//! [`crate::KcpStream`]; [`PeerTransport`] is the per-peer transport handed to
//! streams accepted by the shared-socket server demultiplexer in
//! [`crate::KcpListener`] (the receive task pushes into [`PeerQueue`]; the
//! session's input loop pops).

use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

/// How long `send_all` backs off when the shared socket's kernel send buffer
/// is full, before retrying the non-blocking send. There is no readiness wait
/// here on purpose: tokio's socket readiness is edge-triggered (EPOLLET), and
/// the buffer can drain between the failed send and the wait — the edge fires
/// before anyone is listening and the future never resolves. A bounded
/// sleep-retry keeps every blocked sender alive on its own timer, with no
/// listener-side task to depend on and no spin when the buffer is not full.
const TX_RETRY_SLEEP: Duration = Duration::from_millis(5);
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;

/// Max UDP datagram size for the input-loop recv buffers and peer queues.
pub(crate) const MAX_DATAGRAM: usize = 2048;
/// Bound retained per-peer receive storage after a transient queue burst.
pub(crate) const MAX_RETAINED_PEER_BUFFERS: usize = 64;

// ─── PacketTransport ──────────────────────────────────────────────────────────

/// Pluggable datagram layer under [`crate::KcpStream`].
///
/// Implementations: [`knet::DatagramSocket`] (plain UDP / TcpRaw) and
/// `kcptun_common::CryptoTransport` (encrypt/decrypt wrapper).
///
/// Uses `#[async_trait]` so async methods are object-safe without hand-written
/// future return types.  Each implementation saves ~15 lines of
/// `Box::pin(async move { ... })` boilerplate.
#[async_trait::async_trait]
pub trait PacketTransport: Send + Sync {
    /// Read one datagram into `buf`. Returns bytes written.
    async fn recv(&self, buf: &mut [u8]) -> io::Result<usize>;

    /// Non-blocking read; `WouldBlock` when nothing ready.
    fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize>;

    /// Decrypt a single raw datagram **in place**, without going through the
    /// internal queue. Returns the plaintext length, or 0 on bad packets.
    ///
    /// The default implementation is identity (no crypto). `CryptoTransport`
    /// overrides this to call its decrypt path directly, avoiding the
    /// push/pop queue round-trip when the worker processes packets serially.
    fn decrypt_packet_in_place(&self, buf: &mut [u8], n: usize) -> usize {
        let _ = buf;
        n
    }

    /// Read one datagram into reusable owned storage.
    ///
    /// The default delegates to [`recv`](Self::recv). Queue-backed transports
    /// may override this to transfer packet ownership without another copy.
    async fn recv_vec(&self, buf: &mut Vec<u8>) -> io::Result<usize> {
        let n = self.recv(buf.as_mut_slice()).await?;
        buf.truncate(n);
        Ok(n)
    }

    /// Non-blocking counterpart to [`recv_vec`](Self::recv_vec).
    fn try_recv_vec(&self, buf: &mut Vec<u8>) -> io::Result<usize> {
        let n = self.try_recv(buf.as_mut_slice())?;
        buf.truncate(n);
        Ok(n)
    }

    /// Batch-send on a connected socket.
    async fn send_batch(&self, packets: &[Bytes]) -> io::Result<()>;

    /// Batch-send to an explicit peer (unconnected socket).
    async fn send_batch_to(&self, packets: &[Bytes], target: SocketAddr) -> io::Result<()>;

    /// High-priority send (ACK path). Default = [`send_batch`](Self::send_batch).
    /// Crypto wrappers use a separate buffer here to avoid lock contention.
    async fn send_urgent(&self, packets: &[Bytes]) -> io::Result<()> {
        self.send_batch(packets).await
    }

    /// High-priority send_to (ACK path, unconnected). Default = send_batch_to.
    async fn send_urgent_to(&self, packets: &[Bytes], target: SocketAddr) -> io::Result<()> {
        self.send_batch_to(packets, target).await
    }

    /// Non-blocking batch send (connected socket). Returns the number of
    /// datagrams handed to the kernel, stopping at the first `WouldBlock`
    /// (socket send buffer full); the caller must re-queue `packets[sent..]`
    /// for a later send (e.g. via the flush loop).
    ///
    /// Default: unavailable → `Err(WouldBlock)`, so callers fall back to the
    /// async flush-loop path (existing behavior). `knet::DatagramSocket`
    /// overrides this with a real non-blocking send.
    fn try_send_batch(&self, _packets: &[Bytes]) -> io::Result<usize> {
        Err(io::Error::from(io::ErrorKind::WouldBlock))
    }

    /// Non-blocking batch send to an explicit peer (unconnected socket).
    /// Returns the number of datagrams handed to the kernel, stopping at the
    /// first `WouldBlock` (socket send buffer full); the caller must re-queue
    /// `packets[sent..]` for a later send.
    ///
    /// Default: unavailable → `Err(WouldBlock)`, so callers fall back to the
    /// async flush-loop path (existing behavior). `knet::DatagramSocket`
    /// overrides this with a real non-blocking send.
    fn try_send_batch_to(&self, _packets: &[Bytes], _target: SocketAddr) -> io::Result<usize> {
        Err(io::Error::from(io::ErrorKind::WouldBlock))
    }

    /// Non-blocking batch receive into the caller's buffer pool. Returns the
    /// number of datagrams received. Default: one via [`try_recv`](Self::try_recv)
    /// into `pool[0]`.
    fn try_recv_batch(&self, pool: &mut [Vec<u8>]) -> io::Result<usize> {
        if pool.is_empty() {
            return Ok(0);
        }
        match self.try_recv(&mut pool[0]) {
            Ok(n) if n > 0 => {
                pool[0].truncate(n);
                Ok(1)
            }
            Ok(_) => Ok(0),
            Err(e) => Err(e),
        }
    }

    /// Whether [`try_recv_batch`](Self::try_recv_batch) can receive multiple
    /// datagrams per call (vs. the default single). The input loop uses this to
    /// switch to the batch drain (recvmmsg on Linux).
    fn supports_recv_batch(&self) -> bool {
        false
    }

    fn local_addr(&self) -> io::Result<SocketAddr>;

    /// Set the TTL (hop limit) on the underlying socket.
    /// Default: `Unsupported` (no raw socket to configure).
    fn set_ttl(&self, _ttl: u32) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "set_ttl not available on this transport",
        ))
    }

    /// Get the TTL (hop limit) from the underlying socket.
    /// Default: `Unsupported` (no raw socket to query).
    fn ttl(&self) -> io::Result<u32> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "ttl not available on this transport",
        ))
    }
}

#[async_trait::async_trait]
impl PacketTransport for knet::DatagramSocket {
    async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        // Call inherent method (not trait) to avoid recursion.
        knet::DatagramSocket::recv(self, buf).await
    }

    fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        knet::DatagramSocket::try_recv(self, buf)
    }

    async fn send_batch(&self, packets: &[Bytes]) -> io::Result<()> {
        knet::DatagramSocket::send_batch(self, packets).await
    }

    async fn send_batch_to(&self, packets: &[Bytes], target: SocketAddr) -> io::Result<()> {
        knet::DatagramSocket::send_batch_to(self, packets, target).await
    }

    fn try_send_batch(&self, packets: &[Bytes]) -> io::Result<usize> {
        knet::DatagramSocket::try_send_batch(self, packets)
    }

    fn try_send_batch_to(&self, packets: &[Bytes], target: SocketAddr) -> io::Result<usize> {
        knet::DatagramSocket::try_send_batch_to(self, packets, target)
    }

    fn try_recv_batch(&self, pool: &mut [Vec<u8>]) -> io::Result<usize> {
        knet::DatagramSocket::try_recv_batch(self, pool)
    }

    fn supports_recv_batch(&self) -> bool {
        true
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        knet::DatagramSocket::local_addr(self)
    }

    fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        knet::DatagramSocket::set_ttl(self, ttl)
    }

    fn ttl(&self) -> io::Result<u32> {
        knet::DatagramSocket::ttl(self)
    }
}

// ─── Per-peer queue + transport (KcpListener demux) ───────────────────────────

/// FIFO of inbound datagrams for a single peer. The listener's receive task
/// pushes datagrams in; the session's input loop pops them.
pub(crate) struct PeerQueue {
    buffers: Mutex<PeerBuffers>,
    notify: knet::Notify,
    closed: AtomicBool,
}

struct PeerBuffers {
    packets: VecDeque<Vec<u8>>,
    spare: Vec<Vec<u8>>,
}

impl PeerQueue {
    pub(crate) fn new() -> Self {
        // Keep only a tiny spare-vector index up front. Packet-sized buffers
        // are allocated lazily as traffic arrives, avoiding 128KiB of eager
        // storage for every idle peer; the recycle cap still bounds retained
        // memory after bursts.
        let spare = Vec::with_capacity(2);
        Self {
            buffers: Mutex::new(PeerBuffers {
                packets: VecDeque::new(),
                spare,
            }),
            notify: knet::Notify::new(),
            closed: AtomicBool::new(false),
        }
    }

    /// Queue one datagram, bounded. Returns `false` when `cap` is already
    /// reached or the queue has been closed; the datagram is recycled here, so
    /// the caller must not use it again. The closed check shares the buffer
    /// lock with [`mark_closed`](Self::mark_closed), so a datagram cannot land
    /// in the queue after that call has drained it — the input loop has stopped
    /// reading by then, and the datagram would never reach the buffer pool.
    /// KCP retransmission recovers a dropped datagram; an unbounded queue would
    /// let one fast peer pin the listener's memory.
    pub(crate) fn push(&self, pkt: Vec<u8>, cap: usize) -> bool {
        let mut buffers = self.buffers.lock();
        if self.closed.load(Ordering::Acquire) || buffers.packets.len() >= cap {
            drop(buffers);
            crate::sharded::recycle_buf(pkt);
            return false;
        }
        buffers.packets.push_back(pkt);
        drop(buffers);
        self.notify.notify_one();
        true
    }

    fn pop(&self) -> Option<Vec<u8>> {
        let mut buffers = self.buffers.lock();
        buffers.packets.pop_front()
    }

    /// Move a queued datagram into the consumer buffer and recycle the
    /// consumer's previous allocation back to the listener.
    fn pop_into(&self, buf: &mut Vec<u8>) -> Option<usize> {
        let mut buffers = self.buffers.lock();
        let mut pkt = buffers.packets.pop_front()?;
        std::mem::swap(buf, &mut pkt);
        let n = buf.len();
        if buffers.spare.len() < MAX_RETAINED_PEER_BUFFERS {
            pkt.resize(MAX_DATAGRAM, 0);
            buffers.spare.push(pkt);
        }
        Some(n)
    }

    /// Pop up to `pool.len()` queued datagrams under **one lock**, swapping each
    /// consumer buffer into the queue's spare pool (recycles capacity). Returns
    /// the number popped; `WouldBlock` when the queue is empty. Keeps queued
    /// order, so a peer's input loop drains a whole burst and batches its ACKs.
    fn pop_batch(&self, pool: &mut [Vec<u8>]) -> io::Result<usize> {
        let mut buffers = self.buffers.lock();
        let mut n = 0;
        while n < pool.len() {
            let mut pkt = match buffers.packets.pop_front() {
                Some(p) => p,
                None => break,
            };
            std::mem::swap(&mut pool[n], &mut pkt);
            if buffers.spare.len() < MAX_RETAINED_PEER_BUFFERS {
                pkt.resize(MAX_DATAGRAM, 0);
                buffers.spare.push(pkt);
            }
            n += 1;
        }
        if n == 0 {
            Err(io::Error::from(io::ErrorKind::WouldBlock))
        } else {
            Ok(n)
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub(crate) fn mark_closed(&self) {
        // The flag is stored while the buffer lock is held, which is the same
        // lock `push` holds when it checks the flag. A datagram that arrives
        // after this call therefore sees the queue closed and is recycled,
        // instead of landing in a queue the input loop has stopped reading.
        let queued = {
            let mut buffers = self.buffers.lock();
            self.closed.store(true, Ordering::Release);
            std::mem::take(&mut buffers.packets)
        };
        for pkt in queued {
            crate::sharded::recycle_buf(pkt);
        }
        self.notify.notify_waiters();
    }
}

/// `PacketTransport` for one accepted peer: reads inbound from its
/// [`PeerQueue`] and writes outbound on the shared listen socket addressed to
/// that peer.
///
/// Dropping the transport (i.e. dropping the accepted `KcpStream`) closes the
/// peer queue so the listener reaps it and can accept a fresh connection from
/// the same address.
/// Write-lock shards for the shared listen socket of one listener.
///
/// A listener serves many peers from one unconnected UDP socket, so every
/// session's `sendto`/`sendmmsg` lands on the same fd and the same kernel send
/// buffer. The syscall itself is safe concurrently — a datagram goes out whole
/// or not at all — but the buffer is socket-wide: one hot session filling it
/// makes every session observe `WouldBlock`, and letting each of them wait on
/// the fd's `writable()` wakes them all at once. Each shard serializes only
/// its own peers' syscalls. Same peer always maps to the same shard (including
/// across re-dials), so its datagrams stay ordered. Callers must drop the
/// guard before awaiting anything.
pub(crate) struct SharedSendLock {
    shards: [Mutex<()>; SEND_LOCK_SHARDS],
}

/// Peer → shard count. 8 is enough to cut 32-session `send_lock` contention to
/// a ~4:1 expected bucket without paying for a lock per session.
const SEND_LOCK_SHARDS: usize = 8;

impl SharedSendLock {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            shards: std::array::from_fn(|_| Mutex::new(())),
        })
    }

    /// Lock the shard that owns `peer`.
    pub(crate) fn lock_peer(&self, peer: SocketAddr) -> parking_lot::MutexGuard<'_, ()> {
        self.shards[Self::shard_of(peer)].lock()
    }

    fn shard_of(peer: SocketAddr) -> usize {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        peer.hash(&mut h);
        (h.finish() as usize) % SEND_LOCK_SHARDS
    }
}

pub(crate) struct PeerTransport {
    pub(crate) queue: Arc<PeerQueue>,
    pub(crate) socket: Arc<knet::DatagramSocket>,
    pub(crate) peer: SocketAddr,
    /// Serializes `sendto`/`sendmmsg` on the shared listen socket. `None` for a
    /// transport that owns its socket.
    pub(crate) send_lock: Option<Arc<SharedSendLock>>,
}

#[async_trait::async_trait]
impl PacketTransport for PeerTransport {
    async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            if let Some(pkt) = self.queue.pop() {
                let n = pkt.len().min(buf.len());
                buf[..n].copy_from_slice(&pkt[..n]);
                return Ok(n);
            }
            if self.queue.is_closed() {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "KcpStream: peer session closed",
                ));
            }
            // Arm the notification, then re-check to close the wake race.
            let notified = self.queue.notify.notified();
            if let Some(pkt) = self.queue.pop() {
                let n = pkt.len().min(buf.len());
                buf[..n].copy_from_slice(&pkt[..n]);
                return Ok(n);
            }
            if self.queue.is_closed() {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "KcpStream: peer session closed",
                ));
            }
            notified.await;
        }
    }

    fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        match self.queue.pop() {
            Some(pkt) => {
                let n = pkt.len().min(buf.len());
                buf[..n].copy_from_slice(&pkt[..n]);
                Ok(n)
            }
            None => Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "peer queue empty",
            )),
        }
    }

    async fn recv_vec(&self, buf: &mut Vec<u8>) -> io::Result<usize> {
        loop {
            if let Some(n) = self.queue.pop_into(buf) {
                return Ok(n);
            }
            if self.queue.is_closed() {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "KcpStream: peer session closed",
                ));
            }
            let notified = self.queue.notify.notified();
            if let Some(n) = self.queue.pop_into(buf) {
                return Ok(n);
            }
            if self.queue.is_closed() {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "KcpStream: peer session closed",
                ));
            }
            notified.await;
        }
    }

    fn try_recv_vec(&self, buf: &mut Vec<u8>) -> io::Result<usize> {
        self.queue
            .pop_into(buf)
            .ok_or_else(|| io::Error::new(io::ErrorKind::WouldBlock, "peer queue empty"))
    }

    fn try_recv_batch(&self, pool: &mut [Vec<u8>]) -> io::Result<usize> {
        self.queue.pop_batch(pool)
    }

    fn supports_recv_batch(&self) -> bool {
        true
    }

    async fn send_batch(&self, packets: &[Bytes]) -> io::Result<()> {
        self.send_all(packets).await
    }

    async fn send_batch_to(&self, packets: &[Bytes], _target: SocketAddr) -> io::Result<()> {
        self.send_all(packets).await
    }

    fn try_send_batch_to(&self, packets: &[Bytes], _target: SocketAddr) -> io::Result<usize> {
        let _guard = self.send_lock.as_ref().map(|l| l.lock_peer(self.peer));
        self.socket.try_send_batch_to(packets, self.peer)
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }
}

impl PeerTransport {
    /// Send every datagram, holding the shared-socket lock only around the
    /// non-blocking syscall. On `WouldBlock` the lock is dropped before the
    /// backoff, so one session's full send buffer cannot stall the others'
    /// syscalls, and each blocked sender retries on its own bounded timer.
    async fn send_all(&self, packets: &[Bytes]) -> io::Result<()> {
        let mut offset = 0;
        while offset < packets.len() {
            let sent = {
                let _guard = self.send_lock.as_ref().map(|l| l.lock_peer(self.peer));
                self.socket
                    .try_send_batch_to(&packets[offset..], self.peer)?
            };
            if sent == 0 {
                // The kernel send buffer is full. Do NOT wait on the socket's
                // `writable()` here: tokio's readiness is edge-triggered, and
                // the buffer can drain between the failed send above and the
                // wait — the edge fires before anyone is listening and the
                // future never resolves (observed as a permanent send stall
                // that stopped a whole session's output). A bounded sleep
                // retries the send instead; the kernel drains the buffer on
                // its own, so the next attempt succeeds or surfaces an error.
                // Waiting on this session's inbound notify would race the
                // input loop for the same `notify_one`.
                knet::sleep(TX_RETRY_SLEEP).await;
                continue;
            }
            offset += sent;
        }
        Ok(())
    }
}

impl Drop for PeerTransport {
    fn drop(&mut self) {
        self.queue.mark_closed();
    }
}

/// Optional per-accepted-peer transport wrapper applied by
/// [`crate::KcpListenerBuilder`] (e.g. adding encryption while retaining the
/// listener's single shared-socket reader).
pub(crate) type TransportWrapper =
    Arc<dyn Fn(Arc<dyn PacketTransport>, SocketAddr) -> Arc<dyn PacketTransport> + Send + Sync>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sharded::{acquire_buf, recycle_buf};

    /// Idle peers must not preallocate packet buffers.
    #[test]
    fn peer_queue_spares_are_lazy() {
        let q = PeerQueue::new();
        let buffers = q.buffers.lock();
        assert!(
            buffers.spare.is_empty(),
            "idle peers must not preallocate packet buffers"
        );
    }

    fn datagram(tag: u8) -> Vec<u8> {
        let mut pkt = vec![0u8; MAX_DATAGRAM];
        pkt[0] = tag;
        pkt
    }

    /// `mark_closed` drains the backlog into the buffer pool, and a later
    /// `push` must refuse and recycle rather than re-fill a queue nothing
    /// reads.
    #[test]
    fn mark_closed_recycles_backlog_and_refuses_push() {
        // Drain whatever earlier tests left in the global pool so the
        // assertions below see only buffers this test recycles.
        while acquire_buf().is_some() {}

        let q = PeerQueue::new();
        assert!(q.push(datagram(0xAA), 8));
        assert!(q.push(datagram(0xBB), 8));
        q.mark_closed();
        assert!(q.is_closed());
        assert!(q.pop().is_none(), "mark_closed must drain the backlog");

        // Both queued datagrams went back to the pool.
        let a = acquire_buf().expect("first drained datagram should be pooled");
        let b = acquire_buf().expect("second drained datagram should be pooled");
        assert!(a.capacity() >= MAX_DATAGRAM);
        assert!(b.capacity() >= MAX_DATAGRAM);
        assert!(
            acquire_buf().is_none(),
            "pool should hold exactly the two drained buffers"
        );

        // A push after mark_closed is refused and its buffer recycled.
        assert!(!q.push(datagram(0xCC), 8));
        assert!(q.pop().is_none());
        let c = acquire_buf().expect("refused push must recycle into the pool");
        assert!(c.capacity() >= MAX_DATAGRAM);
    }

    /// `push` and `mark_closed` share the buffer lock, so a close concurrent
    /// with a storm of pushes cannot leave packets in a queue nobody drains.
    #[test]
    fn push_racing_mark_closed_leaves_queue_empty() {
        let q = Arc::new(PeerQueue::new());
        let mut handles = Vec::new();
        for t in 0..4 {
            let q = q.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..2_000 {
                    let mut pkt = datagram((t * 64 + (i % 64)) as u8);
                    pkt[1] = i as u8;
                    q.push(pkt, 4_096);
                }
            }));
        }
        let closer = q.clone();
        handles.push(std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(1));
            closer.mark_closed();
        }));
        for h in handles {
            h.join().unwrap();
        }
        assert!(q.is_closed());
        assert!(
            q.pop().is_none(),
            "a packet must not survive mark_closed + concurrent push"
        );
    }

    /// Buffers undersized for a datagram are not worth pooling.
    #[test]
    fn recycle_drops_undersized_buffers() {
        let small = vec![0u8; 16];
        recycle_buf(small);
        // No assertion beyond "no panic": the pool rejects them internally.
    }
}
