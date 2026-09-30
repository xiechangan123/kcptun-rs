//! UDP listener: one receive task demuxes a shared socket, and each accepted
//! session drives its own KCP state machine.
//!
//! ```text
//! rx task (one per listener, spawned on the caller's runtime)
//!   recvmmsg → group the burst by source address (order preserved)
//!        │
//!        ├─ building peer  → per-session queue (bounded, drop-tail)
//!        └─ live session   → feed_raw_batch(whole peer group) inline:
//!                            one decrypt pass, ONE KCP lock, ONE flush, one
//!                            sendmmsg for the group — no cross-task hop
//!        ▼
//! KcpStream task (the stream's own input loop drains the build queue +
//! flush loop) → datagrams → shared socket
//! ```
//!
//! Grouping the burst before feeding is what keeps the stale-session guard
//! honest: `process_inbound_batch` counts a burst as stale only when EVERY
//! datagram in it mismatched the session (a re-dialed peer), so a single late
//! retransmission mixed into a burst of fresh data must not evict the session.
//! Feeding per datagram would make every datagram a one-packet burst and evict
//! live sessions under loss — the bug this grouping exists to prevent.
//!
//! The library opens no OS threads and no private runtime: the receive task
//! and every session task are `knet::spawn_task`s on whatever runtime the
//! caller built.
//!
//! Outbound datagrams of every session share the one unconnected listen
//! socket. The syscall is safe to issue concurrently, but the kernel send
//! buffer is socket-wide, so sends take a lock that covers only the syscall
//! (see [`crate::transport::PeerTransport`]). A session that finds the buffer
//! full drops the lock before retrying on a bounded timer, so one session's
//! wait cannot stall the others' syscalls.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::config::KcpConfig;
use crate::conn::{kcp_config_setters, resolve_one, KcpStream};
use crate::transport::{PeerQueue, PeerTransport, SharedSendLock, TransportWrapper, MAX_DATAGRAM};
use knet::Notify;

/// How many datagrams one receive wakeup pulls off the socket before yielding
/// back to the caller's runtime. Bounds how long the receive task occupies a
/// runtime worker under a flood. Must stay comfortably above a peer's
/// congestion window: the stale-session guard evaluates whole bursts, so a
/// ceiling near one peer's send window would split its traffic into
/// mostly-fresh bursts and dilute the guard.
const RECV_BATCH: usize = 256;

/// Default bound on a session's inbound queue. A datagram that does not fit is
/// dropped and counted; KCP retransmission recovers it. An unbounded queue
/// would let one fast peer pin the listener's memory.
const SESSION_INBOX_CAP: usize = 2048;

/// How often the receive task reaps dead and idle sessions. With traffic the
/// sweep still runs on this cadence — never per batch, so its O(sessions)
/// clone-and-scan cannot scale with packet rate.
const SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// Max concurrent sessions before new peers are refused.
///
/// A server session is created from a single inbound datagram, so an unbounded
/// map is a remote memory-exhaustion primitive. Raise it through
/// [`WorkerPoolLimits`] to serve more.
const DEFAULT_MAX_SESSIONS: usize = 4096;
/// Consecutive all-stale bursts required before eviction (P0-6 / M-4).
const DEFAULT_STALE_EVICT_THRESHOLD: u32 = 3;
/// Minimum wall-clock span of a stale streak before eviction. Together with
/// the burst-count threshold this keeps eviction off until a peer has been
/// *persistently* talking in a previous generation — one lost-ACK burst, or
/// a short forged burst, must not tear a live session down.
const EVICT_MIN_STALE_SPAN_MS: u64 = 1000;
/// Default per-IP new-session rate (P1-10).
const DEFAULT_PER_IP_SESSION_RATE: u32 = 20;

/// Default idle-session reap threshold. A KCP session with no inbound datagram
/// and no successful write for this long is closed and removed.
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(600);

/// Default cap on how long a peer may sit in the `building` map.
const DEFAULT_BUILDING_TIMEOUT: Duration = Duration::from_secs(10);

// ─── RX buffer pool (lock-free recycling) ─────────────────────────────────

const BUFPOOL_CAP: usize = 4096;

type BufRx = crossbeam_channel::Receiver<Vec<u8>>;
type BufTx = crossbeam_channel::Sender<Vec<u8>>;

fn bufpool() -> &'static (BufTx, BufRx) {
    use crossbeam_channel::bounded;
    use std::sync::OnceLock;
    static POOL: OnceLock<(BufTx, BufRx)> = OnceLock::new();
    POOL.get_or_init(|| {
        let (tx, rx) = bounded(BUFPOOL_CAP);
        (tx, rx)
    })
}

/// Return an empty RX buffer to the pool. Only buffers with full datagram
/// capacity are retained; undersized buffers are freed as normal.
#[inline]
pub(crate) fn recycle_buf(mut buf: Vec<u8>) {
    if buf.capacity() >= MAX_DATAGRAM {
        buf.clear();
        let _ = bufpool().0.try_send(buf);
    }
}

/// Take a pooled buffer, or `None` when the pool is empty. The returned buffer
/// has capacity `≥ MAX_DATAGRAM` and length 0.
#[inline]
pub(crate) fn acquire_buf() -> Option<Vec<u8>> {
    match bufpool().1.try_recv() {
        Ok(buf) if buf.capacity() >= MAX_DATAGRAM => Some(buf),
        _ => None,
    }
}

// ─── KcpListener ─────────────────────────────────────────────────────────

/// Resource limits for [`KcpListener`].
#[derive(Debug, Clone, Copy)]
pub struct WorkerPoolLimits {
    /// Max concurrent sessions (0 = unlimited).
    pub max_sessions_per_worker: usize,
    /// Drop-tail cap on each session's inbound queue (0 = `SESSION_INBOX_CAP`).
    pub worker_channel_cap: usize,
    /// Max datagrams the receive task pulls per wakeup (0 = `RECV_BATCH`).
    pub max_drain_packets: usize,
    /// A peer stuck in `building` longer than this is forgotten.
    /// `Duration::ZERO` = no timeout.
    pub building_timeout: Duration,
    /// A session with no inbound datagram and no successful write for this long
    /// is closed and removed. `Duration::ZERO` = never reap on idleness.
    ///
    /// KCP has no keepalive of its own: an idle session emits nothing, so
    /// `is_dead()` (retransmission budget exhausted) never trips for it, and
    /// without this timeout an abandoned peer is pinned until the process
    /// exits.
    pub idle_timeout: Duration,
    /// Consecutive all-stale bursts required before a session is evicted
    /// (P0-6 / M-4). `1` reproduces the old single-burst behaviour; the
    /// default `3` survives an ACK-blackhole RTO of segment 0.
    pub stale_evict_threshold: u32,
    /// Max **new sessions per second** from one IP (P1-10). `0` disables.
    /// Only admission is limited — established sessions keep full throughput.
    pub per_ip_session_rate: u32,
    /// Max concurrent sessions (published + building) admitted per source IP
    /// (`0` = unlimited). Opt-in defence-in-depth: the rate limit above does
    /// not bound how many sessions one host can *hold*, so a slow-and-steady
    /// client can still pin the whole global budget by varying source ports.
    pub max_sessions_per_ip: usize,
}

impl Default for WorkerPoolLimits {
    fn default() -> Self {
        Self {
            max_sessions_per_worker: DEFAULT_MAX_SESSIONS,
            worker_channel_cap: SESSION_INBOX_CAP,
            max_drain_packets: 0,
            building_timeout: DEFAULT_BUILDING_TIMEOUT,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            stale_evict_threshold: DEFAULT_STALE_EVICT_THRESHOLD,
            per_ip_session_rate: DEFAULT_PER_IP_SESSION_RATE,
            max_sessions_per_ip: 0,
        }
    }
}

/// Live snapshot of [`KcpListener`] resource accounting.
#[derive(Debug, Default, Clone, Copy)]
pub struct WorkerPoolStats {
    /// Sessions currently in the map.
    pub sessions: usize,
    /// Sessions currently under construction (`building` table).
    pub building: usize,
    /// Datagrams dropped because a session's inbound queue was full.
    pub channel_drops: u64,
    /// New sessions refused because `max_sessions_per_worker` was reached.
    pub session_drops: u64,
    /// `KcpStream::build` failures.
    pub build_failures: u64,
    /// Datagrams from an unknown peer that failed the pre-admission integrity
    /// check, so no session was created (spoofed or stray traffic).
    pub unauthenticated_drops: u64,
    /// Datagrams dropped because the source address could not be parsed out of
    /// the receive batch (they cannot be routed to a session).
    pub bad_addr_drops: u64,
    /// Sessions closed and removed by the idle reaper.
    pub idle_reaps: u64,
}

/// Atomic counters behind [`KcpListener::stats`].
#[derive(Default)]
struct ListenerStats {
    channel_drops: AtomicU64,
    session_drops: AtomicU64,
    build_failures: AtomicU64,
    unauthenticated_drops: AtomicU64,
    bad_addr_drops: AtomicU64,
    idle_reaps: AtomicU64,
}

struct PendingAccept {
    conn: KcpStream,
    peer: SocketAddr,
}

/// `(queue, started, build generation)` for a peer whose session is under
/// construction.
type BuildingEntry = (Arc<PeerQueue>, Instant, u64);

/// One accepted peer. The build queue is not held here: the session's
/// transport owns it (and drains it during the building window), so the map
/// entry needs only the connection handle.
struct Session {
    conn: KcpStream,
}

/// KCP listener over one shared UDP socket.
///
/// A single receive task demultiplexes inbound datagrams by source address
/// into per-session queues. Each accepted [`KcpStream`] runs its own input and
/// flush tasks on the caller's runtime, so the listener opens no threads of
/// its own.
pub struct KcpListener {
    socket: Arc<knet::DatagramSocket>,
    sessions: Arc<Mutex<HashMap<SocketAddr, Session>>>,
    building: Arc<Mutex<HashMap<SocketAddr, BuildingEntry>>>,
    pending: Arc<Mutex<VecDeque<PendingAccept>>>,
    accept_notify: Arc<Notify>,
    closed: Arc<AtomicBool>,
    /// Cancels the receive task's socket read the moment [`close`](Self::close)
    /// runs, so shutdown does not wait out the sweep interval.
    stop: knet::CancellationToken,
    last_error: Arc<Mutex<Option<io::Error>>>,
    stats: Arc<ListenerStats>,
    _rx: knet::JoinHandle<()>,
}

impl Drop for KcpListener {
    fn drop(&mut self) {
        // `close()` only stops accept. Drop is what ends the receive task, so
        // a discarded listener does not keep reading the socket.
        self.close();
        self.stop.cancel();
    }
}

impl KcpListener {
    /// Bind a UDP socket on `addr` and return a builder.
    pub fn bind(addr: impl ToSocketAddrs) -> KcpListenerBuilder {
        bind_listener(addr)
    }

    /// Use an already-bound datagram socket, preserving caller socket options.
    pub fn from_socket(socket: Arc<knet::DatagramSocket>) -> KcpListenerBuilder {
        from_socket_listener(socket)
    }

    /// Remove a peer from the session map after its accepted connection ends.
    /// A later datagram from the same address creates a fresh connection and is
    /// surfaced by [`accept`](Self::accept).
    ///
    /// The removed session is closed: the map holds the last non-owning clone,
    /// so dropping it would leave the flush loop running until the process
    /// exits. A not-yet-accepted build for the same peer is dropped too, so
    /// [`accept`](Self::accept) cannot hand out a connection this call just
    /// tore down.
    pub fn remove_peer(&self, peer: SocketAddr) -> bool {
        let stale: Vec<PendingAccept> = {
            let mut pending = self.pending.lock();
            let mut kept = VecDeque::new();
            let mut stale = Vec::new();
            for p in pending.drain(..) {
                if p.peer == peer {
                    stale.push(p);
                } else {
                    kept.push_back(p);
                }
            }
            *pending = kept;
            stale
        };
        for mut p in stale {
            p.conn.attach_owner();
            p.conn.close();
        }
        let removed = self.sessions.lock().remove(&peer);
        if let Some(session) = removed {
            session.conn.close();
            true
        } else {
            false
        }
    }

    /// Current number of known peer sessions.
    pub fn session_count(&self) -> usize {
        self.sessions.lock().len()
    }

    /// Current number of peers whose session is still being built.
    pub fn building_count(&self) -> usize {
        self.building.lock().len()
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Accept the next client connection.
    pub async fn accept(&self) -> io::Result<(KcpStream, SocketAddr)> {
        loop {
            if self.closed.load(Ordering::Acquire) {
                self.discard_pending();
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "KcpListener closed",
                ));
            }
            if let Some(mut v) = self.pending.lock().pop_front() {
                v.conn.attach_owner();
                return Ok((v.conn, v.peer));
            }
            if self.closed.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "KcpListener closed",
                ));
            }
            let notified = self.accept_notify.notified();
            if self.closed.load(Ordering::Acquire) {
                self.discard_pending();
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "KcpListener closed",
                ));
            }
            if let Some(mut v) = self.pending.lock().pop_front() {
                v.conn.attach_owner();
                return Ok((v.conn, v.peer));
            }
            notified.await;
        }
    }

    /// Accept within `timeout`.
    pub async fn accept_timeout(&self, timeout: Duration) -> io::Result<(KcpStream, SocketAddr)> {
        knet::timeout(timeout, self.accept())
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "accept timed out"))?
    }

    /// Non-blocking accept.
    pub fn try_accept(&self) -> io::Result<Option<(KcpStream, SocketAddr)>> {
        if self.closed.load(Ordering::Acquire) {
            self.discard_pending();
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "KcpListener closed",
            ));
        }
        if let Some(mut v) = self.pending.lock().pop_front() {
            v.conn.attach_owner();
            return Ok(Some((v.conn, v.peer)));
        }
        if self.closed.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "KcpListener closed",
            ));
        }
        Ok(None)
    }

    /// Surface and clear the last transport error.
    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        Ok(self.last_error.lock().take())
    }

    /// Stop accepting new connections. A connection already returned by
    /// [`accept`](Self::accept) keeps running: the receive task stays up,
    /// because it still needs inbound datagrams. A session that hits a full
    /// send buffer retries on its own bounded timer and does not depend on any
    /// listener task.
    ///
    /// A connection that was built but not yet accepted is closed here, and so
    /// is one whose build finishes after this call. Both tasks end when the
    /// listener is dropped.
    pub fn close(&self) {
        if self
            .closed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.discard_pending();
            self.accept_notify.notify_waiters();
        }
    }

    /// Close every connection waiting to be accepted. Called once `closed` is
    /// set, so a build that finished in the meantime is not handed out.
    fn discard_pending(&self) {
        // The lock is released before the session map is touched. The build
        // path takes the map first and this queue second, so holding both here
        // would deadlock against it.
        let queued: Vec<PendingAccept> = self.pending.lock().drain(..).collect();
        for mut pending in queued {
            // The backlog holds the owner clone; closing it tears the session
            // down instead of leaving the flush loop running.
            pending.conn.attach_owner();
            pending.conn.close();
            // Drop the map entry only when it is *this* connection. A stale
            // backlog row for a re-dialed peer must not evict the live
            // replacement that already took its place.
            let mut sessions = self.sessions.lock();
            if sessions
                .get(&pending.peer)
                .map(|s| s.conn.is_closed())
                .unwrap_or(false)
            {
                sessions.remove(&pending.peer);
            }
        }
    }

    /// Live resource-accounting snapshot.
    pub fn stats(&self) -> WorkerPoolStats {
        WorkerPoolStats {
            sessions: self.sessions.lock().len(),
            building: self.building.lock().len(),
            channel_drops: self.stats.channel_drops.load(Ordering::Relaxed),
            session_drops: self.stats.session_drops.load(Ordering::Relaxed),
            build_failures: self.stats.build_failures.load(Ordering::Relaxed),
            unauthenticated_drops: self.stats.unauthenticated_drops.load(Ordering::Relaxed),
            bad_addr_drops: self.stats.bad_addr_drops.load(Ordering::Relaxed),
            idle_reaps: self.stats.idle_reaps.load(Ordering::Relaxed),
        }
    }
}

// ─── Builder ────────────────────────────────────────────────────────────

/// Builder for [`KcpListener`].
pub struct KcpListenerBuilder {
    addr: Option<SocketAddr>,
    socket: Option<Arc<knet::DatagramSocket>>,
    config: KcpConfig,
    resolve_err: Option<io::Error>,
    transport_wrapper: Option<TransportWrapper>,
    limits: Option<WorkerPoolLimits>,
    /// Test hook: sleep this long after `KcpStream::build()` returns and
    /// before the session is published, so a `close()` can land on a finished
    /// but not-yet-queued build. `Duration::ZERO` disables it.
    testing_build_delay: Duration,
}

impl KcpListenerBuilder {
    kcp_config_setters!();

    /// Wrap each accepted peer transport before constructing its `KcpStream`.
    pub fn transport_wrapper<F>(mut self, wrapper: F) -> Self
    where
        F: Fn(
                Arc<dyn crate::transport::PacketTransport>,
                SocketAddr,
            ) -> Arc<dyn crate::transport::PacketTransport>
            + Send
            + Sync
            + 'static,
    {
        self.transport_wrapper = Some(Arc::new(wrapper));
        self
    }

    /// Override resource limits.
    pub fn limits(mut self, limits: WorkerPoolLimits) -> Self {
        self.limits = Some(limits);
        self
    }

    /// Test hook: hold a finished build before publishing it, so `close()` can
    /// race an in-flight build deterministically. Not for production use.
    #[doc(hidden)]
    pub fn testing_build_delay(mut self, delay: Duration) -> Self {
        self.testing_build_delay = delay;
        self
    }

    /// Bind the listen socket, spawn the receive task, and return the listener.
    ///
    /// The receive task and every accepted session's tasks run on the caller's
    /// runtime. The listener creates none of its own.
    pub async fn build(self) -> io::Result<KcpListener> {
        if let Some(e) = self.resolve_err {
            return Err(e);
        }

        let socket = match self.socket {
            Some(s) => s,
            None => {
                let addr = self.addr.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "KcpListener: bind address required",
                    )
                })?;
                Arc::new(knet::DatagramSocket::Udp(knet::UdpSocket::bind(addr)?))
            }
        };

        let limits = self.limits.unwrap_or_default();
        let sessions = Arc::new(Mutex::new(HashMap::new()));
        let building = Arc::new(Mutex::new(HashMap::new()));
        let pending = Arc::new(Mutex::new(VecDeque::new()));
        let accept_notify = Arc::new(Notify::new());
        let closed = Arc::new(AtomicBool::new(false));
        let stop = knet::CancellationToken::new();
        let last_error = Arc::new(Mutex::new(None));
        let stats = Arc::new(ListenerStats::default());
        let send_lock = SharedSendLock::new();

        let rx = spawn_rx(RxArgs {
            socket: socket.clone(),
            sessions: sessions.clone(),
            building: building.clone(),
            build_gen: AtomicU64::new(0),
            pending: pending.clone(),
            accept_notify: accept_notify.clone(),
            closed: closed.clone(),
            stop: stop.clone(),
            last_error: last_error.clone(),
            stats: stats.clone(),
            send_lock,
            config: self.config,
            transport_wrapper: self.transport_wrapper,
            limits,
            ip_limiter: Arc::new(Mutex::new(HashMap::new())),
            testing_build_delay: self.testing_build_delay,
        });

        Ok(KcpListener {
            socket,
            sessions,
            building,
            pending,
            accept_notify,
            closed,
            stop,
            last_error,
            stats,
            _rx: rx,
        })
    }
}

/// `KcpListener::bind(addr).await` — awaitable without an explicit `.build()`.
impl std::future::IntoFuture for KcpListenerBuilder {
    type Output = io::Result<KcpListener>;
    type IntoFuture = std::pin::Pin<Box<dyn std::future::Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.build())
    }
}

/// Bind a UDP socket on `addr` and return a builder for the listener.
pub fn bind_listener(addr: impl ToSocketAddrs) -> KcpListenerBuilder {
    match resolve_one(addr) {
        Ok(a) => KcpListenerBuilder {
            addr: Some(a),
            socket: None,
            config: KcpConfig::default(),
            resolve_err: None,
            transport_wrapper: None,
            limits: None,
            testing_build_delay: Duration::ZERO,
        },
        Err(e) => KcpListenerBuilder {
            addr: None,
            socket: None,
            config: KcpConfig::default(),
            resolve_err: Some(e),
            transport_wrapper: None,
            limits: None,
            testing_build_delay: Duration::ZERO,
        },
    }
}

/// Build a listener on an already-bound datagram socket.
pub fn from_socket_listener(socket: Arc<knet::DatagramSocket>) -> KcpListenerBuilder {
    KcpListenerBuilder {
        addr: None,
        socket: Some(socket),
        config: KcpConfig::default(),
        resolve_err: None,
        transport_wrapper: None,
        limits: None,
        testing_build_delay: Duration::ZERO,
    }
}

// ─── Receive task ───────────────────────────────────────────────────────

struct RxArgs {
    socket: Arc<knet::DatagramSocket>,
    sessions: Arc<Mutex<HashMap<SocketAddr, Session>>>,
    /// Peers whose session is being built. Datagrams that arrive in that window
    /// go to the queue the build owns, instead of opening a second session.
    /// The `u64` is the build's generation: a build that outlives its
    /// `building_timeout` is forgotten, and only the build that still owns the
    /// slot may publish the session (so a slow build cannot overwrite a
    /// replacement that already took its place).
    building: Arc<Mutex<HashMap<SocketAddr, BuildingEntry>>>,
    /// Source of [`Self::building`] generations.
    build_gen: AtomicU64,
    pending: Arc<Mutex<VecDeque<PendingAccept>>>,
    accept_notify: Arc<Notify>,
    closed: Arc<AtomicBool>,
    stop: knet::CancellationToken,
    last_error: Arc<Mutex<Option<io::Error>>>,
    stats: Arc<ListenerStats>,
    send_lock: Arc<SharedSendLock>,
    config: KcpConfig,
    transport_wrapper: Option<TransportWrapper>,
    limits: WorkerPoolLimits,
    /// Per-IP new-session rate limiter (P1-10).
    ip_limiter: Arc<Mutex<HashMap<std::net::IpAddr, (Instant, u32)>>>,
    /// Test hook: hold a finished build before publishing. See
    /// [`KcpListenerBuilder::testing_build_delay`].
    testing_build_delay: Duration,
}

/// P1-10: admit at most `rate` new sessions per second from one IP.
/// Returns `false` when the peer is over budget. Only *new* sessions are
/// limited — established ones keep full throughput.
fn allow_new_session(
    limiter: &Mutex<HashMap<std::net::IpAddr, (Instant, u32)>>,
    ip: std::net::IpAddr,
    rate: u32,
) -> bool {
    if rate == 0 {
        return true;
    }
    let mut map = limiter.lock();
    // Opportunistic sweep so a spoofed-source flood cannot grow the map
    // without bound.
    if map.len() > 4096 {
        let now = Instant::now();
        map.retain(|_, (win, _)| now.duration_since(*win) < Duration::from_secs(1));
    }
    let now = Instant::now();
    let entry = map.entry(ip).or_insert((now, 0));
    if now.duration_since(entry.0) >= Duration::from_secs(1) {
        *entry = (now, 0);
    }
    if entry.1 >= rate {
        return false;
    }
    entry.1 += 1;
    true
}

fn spawn_rx(args: RxArgs) -> knet::JoinHandle<()> {
    knet::spawn_task(async move {
        let batch_cap = if args.limits.max_drain_packets > 0 {
            args.limits.max_drain_packets
        } else {
            RECV_BATCH
        };
        let inbox_cap = if args.limits.worker_channel_cap > 0 {
            args.limits.worker_channel_cap
        } else {
            SESSION_INBOX_CAP
        };
        let mut slot = vec![0u8; MAX_DATAGRAM];
        let mut watched: Vec<(SocketAddr, u64)> = Vec::new();
        // Reused across bursts: the datagram buffers themselves move into
        // queues / KCP, so only the index vectors are cleared.
        let mut burst: Vec<(SocketAddr, Vec<u8>)> = Vec::with_capacity(64);
        let mut groups: Vec<(SocketAddr, Vec<Vec<u8>>)> = Vec::new();
        let mut group_index: HashMap<SocketAddr, usize> = HashMap::new();
        let mut last_sweep = Instant::now();
        // recvmmsg scratch: one syscall fills up to RECV_MMSG_MAX slots.
        // Slots move into `burst` and are replaced from the buffer pool.
        const RECV_MMSG_MAX: usize = 64;
        let mut recv_bufs: Vec<Vec<u8>> = Vec::with_capacity(RECV_MMSG_MAX);
        let mut recv_peers: Vec<SocketAddr> = Vec::with_capacity(RECV_MMSG_MAX);

        loop {
            // `close()` does not end this task. An accepted session still needs
            // its peer's datagrams, and `deliver_group` refuses new peers once
            // `closed` is set. The task ends when the listener is dropped,
            // which cancels `stop`.
            //
            // No per-wakeup `yield_now` / `evict_stale` here: live sessions
            // are fed inline in `process_burst`, so their stale counters are
            // already up to date when that returns. Eviction then runs once
            // per burst on the peers just watched.

            slot.resize(MAX_DATAGRAM, 0);
            // The timeout bounds a live listener so the idle sweep runs on a
            // quiet socket. `stop` ends the task when the listener is dropped.
            let first = knet::timeout(
                SWEEP_INTERVAL,
                knet::race(
                    args.stop.cancelled(),
                    std::pin::pin!(args.socket.recv_from(&mut slot)),
                ),
            )
            .await;
            let (n, peer) = match first {
                Ok(knet::RaceOutcome::First(())) => break,
                Ok(knet::RaceOutcome::Second(Ok((n, peer)))) if n > 0 => (n, peer),
                Ok(knet::RaceOutcome::Second(Ok(_))) => {
                    sweep(&args);
                    last_sweep = Instant::now();
                    continue;
                }
                Ok(knet::RaceOutcome::Second(Err(e))) => {
                    *args.last_error.lock() = Some(e);
                    knet::sleep_ms(10).await;
                    continue;
                }
                Err(_) => {
                    // Quiet for a whole sweep interval.
                    sweep(&args);
                    last_sweep = Instant::now();
                    continue;
                }
            };

            let mut buf = std::mem::take(&mut slot);
            buf.truncate(n);
            burst.push((peer, buf));

            // The rest of the burst is already queued in the kernel. Drain it
            // here so one peer's burst is grouped and fed to KCP as ONE batch —
            // per-datagram feeding makes every datagram a one-packet burst,
            // which both multiplies KCP locks and send syscalls and defeats
            // the whole-burst stale-session guard.
            //
            // Linux drains via `recvmmsg` (one syscall per ≤64 datagrams).
            // `try_recv_batch_from_into` returns `Ok(0)` for TcpRaw (no batch
            // path), so fall back to single `try_recv_from` to keep draining.
            while burst.len() < batch_cap {
                let want = (batch_cap - burst.len()).min(RECV_MMSG_MAX);
                while recv_bufs.len() < want {
                    let mut b = acquire_buf().unwrap_or_else(|| Vec::with_capacity(MAX_DATAGRAM));
                    if b.capacity() < MAX_DATAGRAM {
                        b.reserve(MAX_DATAGRAM - b.capacity());
                    }
                    recv_bufs.push(b);
                }
                let batch_n = match args
                    .socket
                    .try_recv_batch_from_into(&mut recv_bufs[..want], &mut recv_peers)
                {
                    Ok(n) => n,
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => 0,
                    Err(e) => {
                        *args.last_error.lock() = Some(e);
                        knet::sleep_ms(10).await;
                        break;
                    }
                };
                if batch_n > 0 {
                    let parsed = batch_n.min(recv_peers.len());
                    for i in 0..parsed {
                        let buf = std::mem::take(&mut recv_bufs[i]);
                        let peer = recv_peers[i];
                        // `try_recv_batch_from_into` leaves len == payload.
                        burst.push((peer, buf));
                        recv_bufs[i] =
                            acquire_buf().unwrap_or_else(|| Vec::with_capacity(MAX_DATAGRAM));
                    }
                    // Source address missing/unparseable: cannot route.
                    let bad = batch_n - parsed;
                    if bad > 0 {
                        args.stats
                            .bad_addr_drops
                            .fetch_add(bad as u64, Ordering::Relaxed);
                        for item in recv_bufs.iter_mut().take(batch_n).skip(parsed) {
                            item.clear();
                        }
                        // Progress requires a routable peer. A batch that
                        // yielded none would spin forever dropping datagrams.
                        if parsed == 0 {
                            break;
                        }
                    }
                    continue;
                }
                // Batch empty or unsupported (TcpRaw) — single-datagram drain.
                let mut buf = std::mem::take(&mut recv_bufs[0]);
                if buf.capacity() < MAX_DATAGRAM {
                    buf.reserve(MAX_DATAGRAM - buf.capacity());
                }
                buf.resize(MAX_DATAGRAM, 0);
                match args.socket.try_recv_from(&mut buf) {
                    Ok((n, peer)) if n > 0 => {
                        buf.truncate(n);
                        burst.push((peer, buf));
                        recv_bufs[0] =
                            acquire_buf().unwrap_or_else(|| Vec::with_capacity(MAX_DATAGRAM));
                    }
                    _ => {
                        buf.clear();
                        recv_bufs[0] = buf;
                        break;
                    }
                }
            }

            process_burst(
                &args,
                &mut burst,
                &mut groups,
                &mut group_index,
                inbox_cap,
                &mut watched,
            );
            // `feed_raw_batch` updated every watched session's stale counter
            // before returning, so the eviction check needs no yield.
            if !watched.is_empty() {
                evict_stale(&args, &mut watched);
            }

            // The sweep is O(sessions) and must not scale with packet rate:
            // run it on its interval, not once per drained batch.
            if last_sweep.elapsed() >= SWEEP_INTERVAL {
                sweep(&args);
                last_sweep = Instant::now();
            }
        }
    })
}

/// Group one drained burst by source address, preserving first-seen order, and
/// hand each peer's datagrams to [`deliver_group`] as a unit. Per-peer grouping
/// is what restores main's `feed_raw_batch` amortization — one KCP lock, one
/// flush and one sendmmsg per peer per burst — and what keeps the whole-burst
/// stale-session guard meaningful.
fn process_burst(
    args: &RxArgs,
    burst: &mut Vec<(SocketAddr, Vec<u8>)>,
    groups: &mut Vec<(SocketAddr, Vec<Vec<u8>>)>,
    group_index: &mut HashMap<SocketAddr, usize>,
    inbox_cap: usize,
    watched: &mut Vec<(SocketAddr, u64)>,
) {
    for (peer, buf) in burst.drain(..) {
        match group_index.get(&peer) {
            Some(&i) => groups[i].1.push(buf),
            None => {
                group_index.insert(peer, groups.len());
                groups.push((peer, vec![buf]));
            }
        }
    }
    group_index.clear();
    for (peer, datagrams) in groups.drain(..) {
        deliver_group(args, peer, datagrams, inbox_cap, watched);
    }
}

/// Push one peer's burst at its existing session, or start building one.
fn deliver_group(
    args: &RxArgs,
    peer: SocketAddr,
    datagrams: Vec<Vec<u8>>,
    inbox_cap: usize,
    watched: &mut Vec<(SocketAddr, u64)>,
) {
    // A peer whose session is being built is claimed even when its queue is
    // full. Falling through would open a second session for the same address
    // and split one handshake across two state machines.
    if let Some((queue, _, _)) = args.building.lock().get(&peer).cloned() {
        for buf in datagrams {
            if !queue.push(buf, inbox_cap) {
                args.stats.channel_drops.fetch_add(1, Ordering::Relaxed);
            }
        }
        return;
    }

    // One `sessions` lock for lookup + optional reap. `close()` runs outside
    // the guard: holding the map across it stalls every other peer's deliver.
    enum Established {
        Live(KcpStream),
        Reaped(KcpStream),
        None,
    }
    let established = {
        let mut sessions = args.sessions.lock();
        match sessions.get(&peer) {
            Some(s) if !s.conn.is_closed() && !s.conn.is_dead() => {
                Established::Live(s.conn.clone())
            }
            Some(_) => Established::Reaped(sessions.remove(&peer).unwrap().conn),
            None => Established::None,
        }
    };
    match established {
        Established::Live(conn) => {
            // Feed the whole group straight into KCP on this task and send the
            // ACKs it produces before returning to the socket. Queueing it for
            // the session's own input task costs a scheduling hop, and on a
            // coarse timer wheel that hop measured 6–13ms — the whole of the
            // fast-mode round trip. The count is taken first:
            // `feed_raw_batch` updates it synchronously, and `evict_stale`
            // compares against it once this burst has been delivered. The
            // batch is the stale-session guard's unit: a group counts as a
            // previous generation only when every datagram in it mismatched.
            let before = conn.stale_burst_count();
            conn.feed_raw_batch(datagrams);
            watched.push((peer, before));
            return;
        }
        Established::Reaped(conn) => {
            conn.close();
            // Fall through: the group now counts as traffic from an unknown
            // peer and may open a fresh session below.
        }
        Established::None => {}
    }

    // No session yet. A closed listener takes no new peers; datagrams for one
    // already being built still go to its queue above.
    if args.closed.load(Ordering::Acquire) {
        for buf in datagrams {
            recycle_buf(buf);
        }
        return;
    }

    // Admission is decided before any state is allocated. In-flight builds
    // count against the cap: with `--crypt none`/`null` the integrity gate
    // below is a no-op, so a spoofed source-address flood would otherwise
    // pile up unbounded `building` entries (each a spawn task + PeerQueue)
    // while every published session is still under the limit.
    //
    // Lock order is `building` then `sessions`, matching the publish path.
    {
        let building = args.building.lock();
        let sessions = args.sessions.lock();
        if args.limits.max_sessions_per_worker > 0
            && sessions.len() + building.len() >= args.limits.max_sessions_per_worker
        {
            drop(sessions);
            drop(building);
            args.stats.session_drops.fetch_add(1, Ordering::Relaxed);
            for buf in datagrams {
                recycle_buf(buf);
            }
            return;
        }
    }

    // P1-10: per-IP new-session rate limit (defence in depth). Without crypto
    // a single host can burn the whole `max_sessions_per_worker` quota.
    if !allow_new_session(&args.ip_limiter, peer.ip(), args.limits.per_ip_session_rate) {
        args.stats.session_drops.fetch_add(1, Ordering::Relaxed);
        log::debug!("listener: per-IP session rate limit hit for {}", peer.ip());
        for buf in datagrams {
            recycle_buf(buf);
        }
        return;
    }

    // Opt-in per-IP cap on pinned session state. The rate limit above does
    // not bound how many sessions one address can *hold*, so a slow-and-steady
    // client can still pin the whole `max_sessions_per_worker` budget by
    // varying source ports. Counted at admission only.
    if args.limits.max_sessions_per_ip > 0 {
        let building_for_ip = args
            .building
            .lock()
            .iter()
            .filter(|(k, _)| k.ip() == peer.ip())
            .count();
        let sessions_for_ip = args
            .sessions
            .lock()
            .iter()
            .filter(|(k, _)| k.ip() == peer.ip())
            .count();
        if building_for_ip + sessions_for_ip >= args.limits.max_sessions_per_ip {
            args.stats.session_drops.fetch_add(1, Ordering::Relaxed);
            log::debug!(
                "listener: per-IP concurrent session cap hit for {} ({}+{})",
                peer.ip(),
                building_for_ip,
                sessions_for_ip
            );
            for buf in datagrams {
                recycle_buf(buf);
            }
            return;
        }
    }

    // Integrity-gate a *copy* of the first datagram: one that fails the
    // transport's CRC32 / AEAD check buys no state at all. The copy is
    // discarded — the originals stay ciphertext and are what the queue
    // receives, so the session input loop decrypts exactly once (decrypting
    // here *and* there would drop the handshake burst).
    let mut gate = vec![datagrams[0].clone()];
    let queue = Arc::new(PeerQueue::new());
    let transport = Arc::new(PeerTransport {
        queue: queue.clone(),
        socket: args.socket.clone(),
        peer,
        send_lock: Some(args.send_lock.clone()),
    });
    let transport: Arc<dyn crate::transport::PacketTransport> = match &args.transport_wrapper {
        Some(wrapper) => wrapper(transport, peer),
        None => transport,
    };
    decrypt_batch_in_place(transport.as_ref(), &mut gate);
    if gate.is_empty() {
        args.stats
            .unauthenticated_drops
            .fetch_add(1, Ordering::Relaxed);
        for buf in datagrams {
            recycle_buf(buf);
        }
        return;
    }

    // Build on its own task. `deliver_group` runs inside the receive task, and
    // nesting a runtime there panics; the datagrams wait in the queue, which
    // is registered before the spawn so the next datagram finds it.
    let gen = args.build_gen.fetch_add(1, Ordering::Relaxed);
    {
        // Second admission check: the integrity gate above can take long
        // enough for concurrent builds to finish, and the cap must hold
        // across the whole admission path, not just at its entry.
        let mut b = args.building.lock();
        let sessions = args.sessions.lock();
        if args.limits.max_sessions_per_worker > 0
            && sessions.len() + b.len() >= args.limits.max_sessions_per_worker
        {
            drop(sessions);
            drop(b);
            args.stats.session_drops.fetch_add(1, Ordering::Relaxed);
            for buf in datagrams {
                recycle_buf(buf);
            }
            return;
        }
        b.insert(peer, (queue.clone(), Instant::now(), gen));
    }
    for buf in datagrams {
        if !queue.push(buf, inbox_cap) {
            args.stats.channel_drops.fetch_add(1, Ordering::Relaxed);
        }
    }
    let sessions = args.sessions.clone();
    let building = args.building.clone();
    let pending = args.pending.clone();
    let accept_notify = args.accept_notify.clone();
    let closed = args.closed.clone();
    let stats = args.stats.clone();
    let config = args.config.clone();
    let testing_build_delay = args.testing_build_delay;
    let max_sessions = args.limits.max_sessions_per_worker;
    let queue = queue.clone();
    knet::spawn_task(async move {
        let conn = match KcpStream::with_transport(transport, peer)
            .connected(false)
            .adopt_conv(true)
            .config(config)
            .build()
            .await
        {
            Ok(conn) => {
                // Test hook: hold a *finished* build so `close()` can land
                // before it is published. No-op in production.
                if !testing_build_delay.is_zero() {
                    knet::sleep_ms(testing_build_delay.as_millis() as u64).await;
                }
                conn
            }
            Err(_) => {
                stats.build_failures.fetch_add(1, Ordering::Relaxed);
                let mut b = building.lock();
                if matches!(b.get(&peer), Some((_, _, g)) if *g == gen) {
                    b.remove(&peer);
                }
                return;
            }
        };
        // Publish only if this build still owns the `building` slot. A build
        // that outlived `building_timeout` has been forgotten (and its queue
        // closed) and may have been replaced by a fresh session — inserting
        // unconditionally would overwrite that session and leak it. Hold the
        // building lock across the insert so the reaper cannot drop the slot
        // in between; `deliver` checks `building` first and both entries point
        // at the same queue, so a datagram that lands in the gap still reaches
        // this session.
        {
            let mut b = building.lock();
            if !matches!(b.get(&peer), Some((_, _, g)) if *g == gen) {
                drop(b);
                conn.close();
                return;
            }
            b.remove(&peer);
            // Final cap check before publish. Several builds that passed
            // admission together can all finish at once; without this the
            // published map would overshoot `max_sessions_per_worker`.
            let mut live = sessions.lock();
            if max_sessions > 0 && live.len() >= max_sessions {
                drop(live);
                drop(b);
                log::warn!(
                    "kcp-rs: dropping built session for {peer}: max_sessions_per_worker ({max_sessions}) reached before publish"
                );
                conn.close();
                // The build's queue still holds the handshake datagrams and
                // its input loop may be parked on it; release the slot's
                // memory instead of leaving a dead build's backlog pinned.
                queue.mark_closed();
                stats.session_drops.fetch_add(1, Ordering::Relaxed);
                return;
            }
            live.insert(peer, Session { conn: conn.clone() });
            drop(live);
            // close() drains `pending` and then sets nothing else, so a publish
            // that loses the race removes the session it just inserted. The
            // accept queue is taken after `sessions`: `discard_pending` drops
            // `pending` before touching the map, so the two locks are never
            // held together in opposite orders.
            let mut waiting = pending.lock();
            if closed.load(Ordering::Acquire) {
                drop(waiting);
                drop(b);
                if let Some(session) = sessions.lock().remove(&peer) {
                    session.conn.close();
                }
                return;
            }
            waiting.push_back(PendingAccept { conn, peer });
        }
        accept_notify.notify_one();
    });
}

/// Evict sessions whose drained burst belonged entirely to a previous
/// generation. Called right after [`process_burst`], which feeds live sessions
/// inline and therefore updates their stale counters before returning.
///
/// The unit is the per-peer group fed by [`deliver_group`]:
/// `process_inbound_batch` bumps `stale_bursts` once, and only when EVERY
/// datagram of the group mismatched the session. A single stale datagram mixed
/// into fresh traffic therefore never reaches this function's eviction —
/// evicting on it would kill live sessions whose ACKs are being lost (the
/// peer's `una` freezes at 0 and its RTO retransmits segment 0, which is
/// indistinguishable from a re-dial until more traffic arrives).
///
/// P0-6 / M-4: even an *entire* all-stale burst is not enough. Under an ACK
/// black hole the peer's `snd_una` never advances, its RTO retransmits sn=0
/// with `una=0`, and if that retransmit is the only unacked datagram the
/// whole burst looks stale. Requiring N consecutive all-stale bursts (any
/// live burst resets the streak) is what separates that from a real re-dial.
fn evict_stale(args: &RxArgs, watched: &mut Vec<(SocketAddr, u64)>) {
    let observed = std::mem::take(watched);
    let mut seen: HashMap<SocketAddr, u64> = HashMap::new();
    for (peer, before) in observed {
        let entry = seen.entry(peer).or_insert(u64::MAX);
        *entry = (*entry).min(before);
    }
    let threshold = args.limits.stale_evict_threshold.max(1);
    for (peer, before) in seen {
        let removed = {
            let mut sessions = args.sessions.lock();
            let Some(session) = sessions.get(&peer) else {
                continue;
            };
            if session.conn.stale_burst_count() <= before {
                continue;
            }
            // A single all-stale burst can be an ACK-blackhole RTO of sn=0.
            // Only a sustained streak — spanning both N bursts and a
            // wall-clock window — means the peer really re-dialed. The time
            // span stops a forged burst from satisfying the count threshold
            // in zero time.
            let now_ms = knet::mono_ms();
            let first = session.conn.first_stale_ms();
            if session.conn.consecutive_stale_burst_count() < threshold as u64
                || first == 0
                || now_ms.saturating_sub(first) < EVICT_MIN_STALE_SPAN_MS
            {
                continue;
            }
            sessions.remove(&peer)
        };
        if let Some(session) = removed {
            log::warn!(
                "listener: evicting stale session for {peer}: the peer re-dialed on the same address"
            );
            session.conn.close();
        }
    }
}

/// Decrypt a burst in place and drop every datagram that fails the transport's
/// integrity check, compacting the survivors to the front.
fn decrypt_batch_in_place(
    transport: &dyn crate::transport::PacketTransport,
    datagrams: &mut Vec<Vec<u8>>,
) {
    let mut write = 0usize;
    for read in 0..datagrams.len() {
        let n = datagrams[read].len();
        let pn = transport.decrypt_packet_in_place(&mut datagrams[read], n);
        if pn > 0 {
            datagrams[read].truncate(pn);
            if write != read {
                datagrams.swap(write, read);
            }
            write += 1;
        }
    }
    for d in datagrams.drain(write..) {
        recycle_buf(d);
    }
}

/// Drop sessions the listener should no longer keep alive.
///
/// A session is reaped when KCP declares the link dead, when it is already
/// closed, or when `limits.idle_timeout` has passed with no inbound datagram
/// and no successful write. Every reaped session is closed, not just dropped:
/// the map holds a non-owning clone, so a bare `remove` leaves the flush loop
/// running.
fn sweep(args: &RxArgs) {
    // Scan the whole map. The lock is only held to clone the session handles;
    // `is_dead()` runs after it is dropped. Capping the scan (the old 4096)
    // starved anything past that prefix: `HashMap` iteration order is bucket
    // order, so the same entries came first on every pass until one of them
    // was removed, and a closed session behind them was never reaped.
    let candidates: Vec<(SocketAddr, KcpStream)> = {
        let sessions = args.sessions.lock();
        sessions.iter().map(|(p, s)| (*p, s.conn.clone())).collect()
    };
    if args.limits.building_timeout > Duration::ZERO {
        // Close the queue of a build that never finished. Otherwise it stays
        // open with whatever handshake datagrams it held, and nothing drains it.
        let abandoned: Vec<Arc<crate::transport::PeerQueue>> = {
            let mut building = args.building.lock();
            let expired: Vec<SocketAddr> = building
                .iter()
                .filter(|(_, (_, started, _))| started.elapsed() >= args.limits.building_timeout)
                .map(|(peer, _)| *peer)
                .collect();
            expired
                .iter()
                .filter_map(|peer| building.remove(peer).map(|(queue, _, _)| queue))
                .collect()
        };
        for queue in abandoned {
            queue.mark_closed();
        }
    }
    if candidates.is_empty() {
        return;
    }
    let now = knet::mono_ms();
    let idle_ms = args.limits.idle_timeout.as_millis() as u64;
    let mut dead: Vec<SocketAddr> = Vec::new();
    let mut idle = 0u64;
    for (peer, conn) in candidates {
        if conn.is_dead() || conn.is_closed() {
            dead.push(peer);
        } else if idle_ms > 0 && now.saturating_sub(conn.last_activity_ms()) >= idle_ms {
            dead.push(peer);
            idle += 1;
        }
    }
    if dead.is_empty() {
        return;
    }
    let evicted: Vec<Session> = {
        let mut sessions = args.sessions.lock();
        dead.iter().filter_map(|p| sessions.remove(p)).collect()
    };
    for session in evicted {
        session.conn.close();
    }
    if idle > 0 {
        args.stats.idle_reaps.fetch_add(idle, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::IpAddr;

    // The per-IP concurrent cap is enforced in `deliver_group` and covered
    // end-to-end by
    // `tests/kcpstream_listener.rs::per_ip_session_limit_bounds_admission_from_one_address`.

    /// P1-10: a single IP must not open more than `rate` new sessions per
    /// second; other IPs are unaffected.
    #[test]
    fn per_ip_session_rate_limits_one_host() {
        let limiter = Mutex::new(HashMap::new());
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        let other: IpAddr = "10.0.0.2".parse().unwrap();
        let rate = 5;

        for i in 0..rate {
            assert!(
                allow_new_session(&limiter, ip, rate),
                "call {i} should be allowed"
            );
        }
        assert!(
            !allow_new_session(&limiter, ip, rate),
            "over-budget admission must be refused"
        );
        // A different IP is unaffected.
        assert!(allow_new_session(&limiter, other, rate));
        // rate=0 disables the limit.
        assert!(allow_new_session(&limiter, ip, 0));
    }
}
