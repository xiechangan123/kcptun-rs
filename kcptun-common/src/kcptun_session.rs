//! Shared kcptun session above the encrypted KCP transport.

use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::{Bytes, BytesMut};
use kcrypt_rs::wire::OffloadProfile;
use parking_lot::Mutex;

use crate::RateLimiter;

/// Configuration for one complete kcptun session.
#[derive(Clone, Debug)]
pub struct KcptunConfig {
    pub kcp: kcp_rs::KcpConfig,
    pub smux: smux_rs::Config,
    pub nocomp: bool,
    pub rate_limit: u32,
    pub offload_profile: OffloadProfile,
    /// How long outbound data may stay unacknowledged before the session is
    /// declared desynchronised (seconds; 0 disables the check).
    ///
    /// Only applies once the peer's KCP restart signal has been observed; with
    /// no such evidence a frozen `snd_una` is indistinguishable from plain loss
    /// on our outbound path, and closing there kills a session whose only fault
    /// is the network.
    pub ack_stall_secs: u64,
}

/// Shared client/server session composition: KCP transport + Snappy + SMUX.
///
/// This type owns the common KCP, Snappy, and SMUX scheduling so transport
/// variants and binaries do not duplicate those loops.
pub struct KcptunSession {
    kcp: Arc<kcp_rs::KcpStream>,
    smux: Arc<smux_rs::Session>,
    flush_notify: Arc<knet::Notify>,
    dead: Arc<AtomicBool>,
    created_ms: u64,
    /// Live state of the session's three pumps (inbound timestamp, outbound
    /// timestamps, parked/blocked flags). A silent session has to distinguish
    /// "the peer sent nothing" from "our own pump is parked" — identical on the
    /// wire, opposite conclusions.
    out_state: Arc<OutState>,
    _handles: Vec<knet::JoinHandle<()>>,
}

/// Live state of this session's outbound and inbound pumps.
///
/// Sampled by the watchdog so a close states *why* the session went quiet:
/// a peer that stopped sending, or a local pump that is parked (which is not a
/// network problem at all and must not be mistaken for one).
struct OutState {
    /// Last successful KCP read (inbound datagram). Distinct from
    /// `kcp.last_activity_ms`, which is also stamped on writes — after a
    /// server restart the writer keeps stamping while the peer is gone.
    last_inbound_ms: AtomicU64,
    /// Monotonic ms of the last successful `kcp.write_all`.
    last_out_ms: AtomicU64,
    /// Monotonic ms the writer entered a blocking `kcp.write_all` (0 = free).
    write_blocked_since: AtomicU64,
    /// Monotonic ms the reader parked waiting for SMUX receive capacity.
    read_parked_since: AtomicU64,
}

impl OutState {
    fn new() -> Self {
        Self {
            last_inbound_ms: AtomicU64::new(knet::mono_ms()),
            last_out_ms: AtomicU64::new(knet::mono_ms()),
            write_blocked_since: AtomicU64::new(0),
            read_parked_since: AtomicU64::new(0),
        }
    }

    /// Enter/leave the blocking write. Returns how long the previous block lasted.
    fn begin_write(&self) {
        self.write_blocked_since
            .store(knet::mono_ms().max(1), Ordering::Release);
    }

    fn end_write(&self) {
        self.write_blocked_since.store(0, Ordering::Release);
        self.last_out_ms.store(knet::mono_ms(), Ordering::Release);
    }

    fn blocked_ms(&self, now: u64) -> u64 {
        let since = self.write_blocked_since.load(Ordering::Acquire);
        if since == 0 {
            0
        } else {
            now.saturating_sub(since)
        }
    }

    fn parked_ms(&self, now: u64) -> u64 {
        let since = self.read_parked_since.load(Ordering::Acquire);
        if since == 0 {
            0
        } else {
            now.saturating_sub(since)
        }
    }
}

impl KcptunSession {
    /// Build a client session over an existing UDP or raw-TCP datagram socket.
    pub async fn connect(
        socket: Arc<knet::DatagramSocket>,
        remote: std::net::SocketAddr,
        key: &[u8],
        crypt: &str,
        config: &KcptunConfig,
    ) -> Result<Self> {
        let kcp = crate::kcp_transport::kcp_stream_with_socket_rate_limited(
            socket,
            remote,
            key,
            crypt,
            config.kcp.clone(),
            true,
            config.offload_profile,
            config.rate_limit,
        )
        .await?;
        Self::new(
            kcp,
            Arc::new(smux_rs::Session::new_client(&config.smux)?),
            config.nocomp,
            0,
            true,
            config.ack_stall_secs,
        )
    }

    /// Build a server session over one accepted raw-TCP datagram socket.
    ///
    /// UDP servers assemble `kcp_rs::KcpListener` + `CryptoTransport` for
    /// shared-socket peer demultiplexing (see `kcptun-server`). A raw-TCP
    /// socket is already private to one peer, so it can directly adopt the
    /// conversation ID from its first valid packet.
    pub async fn serve_transport(
        socket: Arc<knet::DatagramSocket>,
        peer: std::net::SocketAddr,
        key: &[u8],
        crypt: &str,
        config: &KcptunConfig,
    ) -> Result<Self> {
        let kcp = crate::kcp_transport::server_kcp_stream_with_socket_rate_limited(
            socket,
            peer,
            key,
            crypt,
            config.kcp.clone(),
            config.offload_profile,
            config.rate_limit,
        )
        .await?;
        let smux = Arc::new(smux_rs::Session::new_server(&config.smux)?);
        smux.enable_accept();
        Self::new(kcp, smux, config.nocomp, 0, false, config.ack_stall_secs)
    }

    /// Start a client-side session over an established KCP connection.
    pub fn client(kcp: kcp_rs::KcpStream, config: &KcptunConfig) -> Result<Self> {
        Self::new(
            kcp,
            Arc::new(smux_rs::Session::new_client(&config.smux)?),
            config.nocomp,
            config.rate_limit,
            true,
            config.ack_stall_secs,
        )
    }

    /// Start a server-side session over an established KCP connection.
    pub fn server(kcp: kcp_rs::KcpStream, config: &KcptunConfig) -> Result<Self> {
        let smux = Arc::new(smux_rs::Session::new_server(&config.smux)?);
        smux.enable_accept();
        Self::new(
            kcp,
            smux,
            config.nocomp,
            config.rate_limit,
            false,
            config.ack_stall_secs,
        )
    }

    /// Start a server session whose packet transport already applies the
    /// configured on-wire rate limit (the shared-UDP listener path).
    pub fn server_with_limited_transport(
        kcp: kcp_rs::KcpStream,
        config: &KcptunConfig,
    ) -> Result<Self> {
        let smux = Arc::new(smux_rs::Session::new_server(&config.smux)?);
        smux.enable_accept();
        Self::new(kcp, smux, config.nocomp, 0, false, config.ack_stall_secs)
    }

    fn new(
        kcp: kcp_rs::KcpStream,
        smux: Arc<smux_rs::Session>,
        nocomp: bool,
        rate_limit: u32,
        active_open: bool,
        ack_stall_secs: u64,
    ) -> Result<Self> {
        let kcp = Arc::new(kcp);
        let flush_notify = Arc::new(knet::Notify::new());
        let dead = Arc::new(AtomicBool::new(false));
        // `None` when compression is disabled: keeps the "configured" state in
        // one handle instead of a parallel flag the loop has to re-check.
        let compressor =
            (!nocomp).then(|| Arc::new(Mutex::new(snap::write::FrameEncoder::new(Vec::new()))));
        let limiter = Arc::new(RateLimiter::new(rate_limit));
        let out_state = Arc::new(OutState::new());
        let ack_stall_ms = ack_stall_secs.saturating_mul(1000);
        let handles = vec![
            knet::spawn_task(read_loop(
                kcp.clone(),
                smux.clone(),
                flush_notify.clone(),
                dead.clone(),
                nocomp,
                out_state.clone(),
            )),
            knet::spawn_task(write_loop(
                kcp.clone(),
                smux.clone(),
                compressor,
                flush_notify.clone(),
                dead.clone(),
                limiter,
                out_state.clone(),
            )),
            // Independent watchdog: write_loop can block inside kcp.write_all
            // when the peer is gone and the send window fills with unacked
            // data, so it never reaches its keepalive/death checks. This task
            // keeps ticking and force-closes the session (server-restart
            // recovery must not wait for dead_link or a stuck writer).
            knet::spawn_task(watchdog_loop(
                kcp.clone(),
                smux.clone(),
                dead.clone(),
                out_state.clone(),
                ack_stall_ms,
            )),
        ];
        let session = Self {
            kcp,
            smux,
            flush_notify,
            dead,
            created_ms: knet::mono_ms(),
            out_state,
            _handles: handles,
        };
        kcp_rs::DEFAULT_SNMP.session_opened(active_open);
        Ok(session)
    }

    /// Open and queue a client-side SMUX stream.
    pub fn open_stream(&self) -> Result<Arc<smux_rs::Stream>, smux_rs::SessionError> {
        let stream = self.smux.open_stream()?;
        self.smux.queue_syn(stream.id());
        stream.set_flush_notify(self.flush_notify.clone());
        self.flush_notify.notify_one();
        Ok(stream)
    }

    /// Accept the next server-side SMUX stream.
    pub async fn accept(&self) -> Result<Arc<smux_rs::Stream>, smux_rs::SessionError> {
        loop {
            if self.smux.is_closed() {
                return Err(smux_rs::SessionError::SessionClosed);
            }
            if let Some(id) = self.smux.pop_accepted_stream() {
                if let Some(stream) = self.smux.streams().lock().get(&id).cloned() {
                    stream.set_flush_notify(self.flush_notify.clone());
                    return Ok(stream);
                }
                continue;
            }
            self.smux.accept_notify().notified().await;
        }
    }

    /// Remove a stream from the session map.
    pub fn remove_stream(&self, id: u32) {
        self.smux.remove_stream(id);
    }

    /// Streams still tracked by this session (in flight or lingering).
    ///
    /// A replaced session is kept alive until this reaches zero, so a transfer
    /// that is still running when its slot is retired finishes normally.
    pub fn active_stream_count(&self) -> usize {
        self.smux.stream_count()
    }

    /// Wake the shared SMUX writer.
    pub fn flush_notify(&self) -> Arc<knet::Notify> {
        self.flush_notify.clone()
    }

    /// Latest KCP transport activity, using the monotonic clock.
    pub fn last_activity_ms(&self) -> u64 {
        self.kcp.last_activity_ms()
    }

    /// Monotonic timestamp in milliseconds when this session was created.
    ///
    /// Used by `--autoexpire` to compute an absolute expiry deadline from
    /// creation time (matching Go kcptun), independent of keepalive activity.
    pub fn created_ms(&self) -> u64 {
        self.created_ms
    }

    /// Whether the KCP or SMUX session has failed or timed out.
    ///
    /// Inbound silence longer than the keepalive timeout also counts as
    /// death: after a server restart the old socket may keep accepting
    /// writes (macOS often swallows ICMP) while no peer data ever returns.
    /// A restarted client would not keep such a session.
    pub fn is_dead(&self) -> bool {
        self.dead.load(Ordering::Acquire)
            || self.kcp.is_dead()
            || self.kcp.is_closed()
            || self.smux.is_closed()
            || self.smux.is_keepalive_timeout()
            || self.inbound_idle_expired()
    }

    /// True when no inbound KCP datagram has been seen for the SMUX
    /// keepalive timeout, and our own receive window is not the reason for the
    /// silence (see [`smux_rs::Session::is_keepalive_timeout`], which carries
    /// the same guard as Go's smux).
    fn inbound_idle_expired(&self) -> bool {
        let timeout = self.smux.keepalive_timeout_secs();
        timeout > 0
            && self.inbound_idle_ms() >= timeout.saturating_mul(1000)
            && self.smux.has_receive_capacity()
    }

    /// Milliseconds since the last inbound KCP datagram.
    pub fn inbound_idle_ms(&self) -> u64 {
        knet::mono_ms().saturating_sub(self.out_state.last_inbound_ms.load(Ordering::Acquire))
    }

    /// Milliseconds since this session last got a packet onto the wire.
    ///
    /// A session whose outbound age is large while its peer is silent is a
    /// local stall (a parked writer), not necessarily a dead path — the two
    /// look identical from the far end, which is why this is reported next to
    /// the watchdog verdict.
    pub fn out_idle_ms(&self) -> u64 {
        knet::mono_ms().saturating_sub(self.out_state.last_out_ms.load(Ordering::Acquire))
    }

    /// Milliseconds the outbound pump has been parked inside `kcp.write_all`
    /// (0 = not parked).
    pub fn write_blocked_ms(&self) -> u64 {
        self.out_state.blocked_ms(knet::mono_ms())
    }

    /// Close KCP, SMUX, and all streams.
    pub fn close(&self) {
        self.dead.store(true, Ordering::Release);
        self.smux.close();
        self.kcp.close();
        self.flush_notify.notify_waiters();
    }
}

impl Drop for KcptunSession {
    fn drop(&mut self) {
        self.close();
        kcp_rs::DEFAULT_SNMP.session_closed();
    }
}

async fn read_loop(
    kcp: Arc<kcp_rs::KcpStream>,
    smux: Arc<smux_rs::Session>,
    flush: Arc<knet::Notify>,
    dead: Arc<AtomicBool>,
    nocomp: bool,
    out_state: Arc<OutState>,
) {
    let mut buf = vec![0u8; 64 * 1024];
    let mut decoder = (!nocomp).then(crate::SnappyStreamDecoder::new);
    while !dead.load(Ordering::Acquire) && !smux.is_closed() && !kcp.is_closed() {
        // Respect the SMUX receive window: while it is exhausted, stop
        // pulling from KCP so its receive window fills and the peer feels
        // backpressure, instead of buffering unread data without bound
        // (`max_receive_buffer` used to be pure bookkeeping). The write loop
        // reclaims tokens every cycle as the application reads, so this
        // parks for at most one tick.
        if !smux.has_receive_capacity() {
            // Diagnostics only: a reader parked here is invisible on the wire
            // (no ACK progress, no payload) and is a local stall, not loss.
            let _ = out_state.read_parked_since.compare_exchange(
                0,
                knet::mono_ms().max(1),
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            knet::sleep_ms(10).await;
            continue;
        }
        {
            let parked = out_state.read_parked_since.swap(0, Ordering::AcqRel);
            if parked != 0 {
                let elapsed = knet::mono_ms().saturating_sub(parked);
                if elapsed >= READ_PARK_WARN_MS {
                    log::warn!(
                        "session read loop was parked {elapsed}ms on SMUX receive capacity (bucket={}), no KCP data pulled",
                        smux.token_bucket_value()
                    );
                }
            }
        }
        let n = match kcp.read(&mut buf).await {
            Ok(0) => {
                // Silent close paths are indistinguishable from a dead peer in
                // the logs; state the reason and the transport counters.
                log::warn!(
                    "session read loop: KCP EOF (closed={}, dead={}, snd_una={}, wait_send={}, rmt_wnd={}, out_idle_ms={}){}",
                    kcp.is_closed(),
                    kcp.is_dead(),
                    kcp.snd_una(),
                    kcp.wait_send(),
                    kcp.rmt_wnd(),
                    knet::mono_ms().saturating_sub(
                        out_state.last_out_ms.load(Ordering::Acquire)
                    ),
                    kcp.take_error()
                        .ok()
                        .flatten()
                        .map(|e| format!(" last_error={e}"))
                        .unwrap_or_default()
                );
                break;
            }
            Ok(n) => n,
            Err(e) if kcp.is_closed() => {
                log::warn!(
                    "session read loop: read failed on a closed KCP: {e} (dead={}, snd_una={}, wait_send={})",
                    kcp.is_dead(),
                    kcp.snd_una(),
                    kcp.wait_send()
                );
                break;
            }
            Err(e) => {
                log::debug!("session read loop: transient read error: {e}");
                continue;
            }
        };
        out_state
            .last_inbound_ms
            .store(knet::mono_ms(), Ordering::Release);
        let result = if let Some(decoder) = decoder.as_mut() {
            decoder.feed(&buf[..n]).and_then(|data| {
                if data.is_empty() {
                    Ok(())
                } else {
                    smux.process_data(&data)
                        .map(|_| ())
                        .map_err(std::io::Error::other)
                }
            })
        } else {
            smux.process_data(&buf[..n])
                .map(|_| ())
                .map_err(std::io::Error::other)
        };
        if let Err(error) = result {
            log::warn!("SMUX process_data error: {error}");
        }
        flush.notify_one();
    }
    dead.store(true, Ordering::Release);
    smux.close();
    kcp.close();
}

/// How long outbound data may stay unacknowledged before the session is
/// declared dead, provided the peer is still sending us traffic.
///
/// A restarted peer brings up a fresh KCP state: it drops our segments as
/// out-of-window without ACKing them, so `snd_una` never advances even though
/// its own ACKs/probes keep arriving and the SMUX keepalive stays satisfied.
/// `dead_link` only notices after its full retransmission budget (~20
/// retransmits with RTO backoff) and the idle timeout needs 30 s of complete
/// silence, so neither recovers a stale session in time.
/// Ack-stall window used when no peer restart has been observed.
///
/// A frozen `snd_una` with a live peer has two shapes: the peer's KCP was reset
/// (seen as a restart signal, and never recoverable) or our outbound data is
/// being dropped (recoverable by KCP retransmission). Only the first deserves a
/// fast close; the second gets this much longer window, which still bounds the
/// unrecoverable "peer silent for us but still talking" case well inside KCP's
/// own `dead_link` budget.
const ACK_STALL_NO_RESTART_MS: u64 = 60_000;

/// Ack-stall window to apply for this sample (0 = the check is disabled).
///
/// The fast window requires evidence that the peer's KCP was restarted; without
/// it a frozen `snd_una` is attributed to loss on our outbound path and only the
/// long grace applies.
fn stall_window_ms(ack_stall_ms: u64, peer_restarted: bool) -> u64 {
    if ack_stall_ms == 0 {
        return 0; // check disabled
    }
    if peer_restarted {
        ack_stall_ms
    } else {
        ack_stall_ms.max(ACK_STALL_NO_RESTART_MS)
    }
}

/// A stall only counts while the peer is still reachable. During a plain
/// network outage both directions go quiet at once; there KCP's own
/// retransmission is the correct recovery and the session must be left alone.
const ACK_STALL_INBOUND_FRESH_MS: u64 = 5_000;

/// Whether inbound silence is treated as peer death.
///
/// Silence is measured at the **SMUX frame** level: no frame decoded for the
/// keepalive timeout, the same rule Go's smux applies. It is also the useful
/// one: a peer can keep its KCP acknowledgements flowing while sending no frames
/// at all — its session write loop blocked on a full send window, so neither
/// data nor keepalive NOPs leave it — and such a session delivers nothing to the
/// streams on it, so it must not be kept alive by bare ACKs. `rx_age_ms` is
/// logged next to the verdict for diagnosis (datagrams flowing while frames are
/// stale means the peer's writer is stuck), not as a reason to keep the session.
///
/// The receive-capacity guard stays: while our own window is drained the peer's
/// writer is blocked on us, so quiet is expected.
fn silence_is_fatal(timeout_secs: u64, inbound_age_ms: u64, has_receive_capacity: bool) -> bool {
    timeout_secs > 0 && inbound_age_ms >= timeout_secs.saturating_mul(1000) && has_receive_capacity
}

/// Report (once) when the outbound pump has been parked this long inside
/// `kcp.write_all`. The paired peer sees pure silence, so without this line a
/// local stall is indistinguishable from packet loss.
const WRITE_BLOCK_WARN_MS: u64 = 3_000;

/// Report (once) when the inbound pump has been parked this long waiting for
/// SMUX receive capacity — again, indistinguishable from peer silence on the
/// wire.
const READ_PARK_WARN_MS: u64 = 5_000;

/// Default ack-stall window used by `bench/` and the binaries' CLI defaults.
pub const ACK_STALL_DEFAULT_SECS: u64 = 10;

/// Tracks whether the peer keeps acknowledging outbound data.
///
/// The clock starts when data is in flight and nothing new gets acknowledged,
/// and keeps running across inbound gaps (peer traffic is bursty). It only
/// reports a stall once the peer has also been heard from recently and is
/// advertising room for more data — a peer whose receive window is closed is
/// applying backpressure, not losing our stream state.
struct AckStallDetector {
    last_una: u32,
    stalled_since: Option<u64>,
}

impl AckStallDetector {
    fn new(una: u32) -> Self {
        Self {
            last_una: una,
            stalled_since: None,
        }
    }

    /// How long the current stall has been running (diagnostics).
    fn stalled_ms(&self, now_ms: u64) -> Option<u64> {
        self.stalled_since.map(|since| now_ms.saturating_sub(since))
    }

    /// Feed one sample (roughly every 500 ms); true means the session should
    /// be declared dead.
    fn sample(
        &mut self,
        now_ms: u64,
        waiting: bool,
        una: u32,
        inbound_age_ms: u64,
        peer_window_open: bool,
        threshold_ms: u64,
    ) -> bool {
        if !waiting || !peer_window_open || una != self.last_una {
            self.stalled_since = None;
        } else if self.stalled_since.is_none() {
            self.stalled_since = Some(now_ms);
        }
        self.last_una = una;
        self.stalled_since.is_some_and(|since| {
            now_ms.saturating_sub(since) >= threshold_ms
                && inbound_age_ms < ACK_STALL_INBOUND_FRESH_MS
        })
    }
}

#[cfg(test)]
mod silence_tests {
    use super::silence_is_fatal;

    /// Nothing (no frames, no payload) for the whole window: a dead session.
    #[test]
    fn silence_past_the_timeout_is_fatal() {
        assert!(silence_is_fatal(90, 90_000, true));
        assert!(silence_is_fatal(90, 120_000, true));
    }

    /// Recent frames: alive, including just inside the edge.
    #[test]
    fn recent_frames_keep_the_session() {
        assert!(!silence_is_fatal(90, 400, true));
        assert!(!silence_is_fatal(90, 89_000, true));
        assert!(!silence_is_fatal(90, 0, true));
    }

    /// Our own receive window is drained: the peer's writer is blocked on us, so
    /// its quiet is self-inflicted. Never fatal.
    #[test]
    fn drained_receive_window_keeps_the_session() {
        assert!(!silence_is_fatal(90, 120_000, false));
    }

    /// `--keepalivetimeout 0` disables the check entirely.
    #[test]
    fn disabled_timeout_keeps_the_session() {
        assert!(!silence_is_fatal(0, 600_000, true));
    }
}

#[cfg(test)]
mod ack_stall_tests {
    use super::{
        stall_window_ms, AckStallDetector, ACK_STALL_DEFAULT_SECS, ACK_STALL_INBOUND_FRESH_MS,
        ACK_STALL_NO_RESTART_MS,
    };

    /// The window the tests exercise (the binaries default to this value).
    const ACK_STALL_MS: u64 = ACK_STALL_DEFAULT_SECS * 1000;

    /// Samples arrive every 500 ms in production; the peer is reachable.
    const FRESH: u64 = 200;

    #[test]
    fn fires_after_the_stall_window() {
        let mut det = AckStallDetector::new(7);
        let mut now = 0;
        while now < ACK_STALL_MS {
            assert!(
                !det.sample(now, true, 7, FRESH, true, ACK_STALL_MS),
                "fired early at {now}ms"
            );
            now += 500;
        }
        assert!(det.sample(ACK_STALL_MS, true, 7, FRESH, true, ACK_STALL_MS));
    }

    #[test]
    fn outage_between_peer_and_us_does_not_fire() {
        // Data in flight but the peer stopped talking to us as well: that is a
        // link outage, which KCP retransmission is meant to ride out. The
        // inbound age grows with the outage, so the reachability check fails
        // even after the stall window has elapsed.
        let mut det = AckStallDetector::new(7);
        assert!(!det.sample(0, true, 7, 100, true, ACK_STALL_MS));
        assert!(!det.sample(5_000, true, 7, 5_100, true, ACK_STALL_MS));
        assert!(!det.sample(ACK_STALL_MS, true, 7, 10_100, true, ACK_STALL_MS));
        assert!(!det.sample(20_000, true, 7, 20_100, true, ACK_STALL_MS));
    }

    #[test]
    fn closed_peer_window_is_flow_control_not_desync() {
        // A peer that advertises no room is deliberately not taking data: the
        // frozen snd_una is backpressure (a slow consumer on the far side), so
        // the session must survive even though the peer keeps talking to us.
        let mut det = AckStallDetector::new(7);
        let mut now = 0;
        while now <= ACK_STALL_MS * 3 {
            assert!(
                !det.sample(now, true, 7, FRESH, false, ACK_STALL_MS),
                "fired on flow control at {now}ms"
            );
            now += 500;
        }
    }

    #[test]
    fn acknowledgement_progress_restarts_the_clock() {
        let mut det = AckStallDetector::new(7);
        for now in (0..6_000).step_by(500) {
            assert!(
                !det.sample(now, true, 7, FRESH, true, ACK_STALL_MS),
                "fired early at {now}ms"
            );
        }
        // Peer acknowledged more data: the window starts over from here.
        assert!(!det.sample(6_000, true, 8, FRESH, true, ACK_STALL_MS));
        let mut now = 6_500;
        while now < 16_500 {
            assert!(
                !det.sample(now, true, 8, FRESH, true, ACK_STALL_MS),
                "fired early at {now}ms"
            );
            now += 500;
        }
        assert!(det.sample(16_500, true, 8, FRESH, true, ACK_STALL_MS));
    }

    #[test]
    fn nothing_in_flight_never_fires() {
        let mut det = AckStallDetector::new(7);
        assert!(!det.sample(0, false, 7, FRESH, true, ACK_STALL_MS));
        assert!(!det.sample(600_000, false, 7, FRESH, true, ACK_STALL_MS));
    }

    /// The fast window applies only with restart evidence; without it the long
    /// grace is used, so loss-induced stalls do not tear the session down.
    #[test]
    fn stall_window_requires_restart_evidence_for_the_fast_path() {
        assert_eq!(stall_window_ms(10_000, true), 10_000);
        assert_eq!(stall_window_ms(10_000, false), ACK_STALL_NO_RESTART_MS);
        // A configured value above the grace is respected either way.
        assert_eq!(stall_window_ms(90_000, false), 90_000);
        assert_eq!(stall_window_ms(90_000, true), 90_000);
        // 0 keeps the check disabled.
        assert_eq!(stall_window_ms(0, true), 0);
        assert_eq!(stall_window_ms(0, false), 0);
    }

    #[test]
    fn fresh_inbound_bound_is_shorter_than_the_stall_window() {
        // The reachability probe must be able to discriminate a live peer
        // before the stall window elapses, otherwise the rule is dead code.
        assert!(ACK_STALL_INBOUND_FRESH_MS < ACK_STALL_MS);
    }
}

/// Force-close the session when the peer has gone silent, independently of
/// the write path (which can block on a full send window after a server
/// restart), and when it keeps talking but has stopped acknowledging our data.
///
/// Deliberately ignores stream-level progress: a stream that has written but
/// not yet been answered is indistinguishable from a healthy request against
/// a slow backend, so treating it as death tears down working sessions.
/// Link death is judged only by transport state: KCP dead/closed, SMUX
/// keepalive timeout, inbound silence, and stalled ACK progress.
async fn watchdog_loop(
    kcp: Arc<kcp_rs::KcpStream>,
    smux: Arc<smux_rs::Session>,
    dead: Arc<AtomicBool>,
    out_state: Arc<OutState>,
    ack_stall_ms: u64,
) {
    let mut ack_stall = AckStallDetector::new(kcp.snd_una());
    let mut write_stall_logged = false;
    let mut heartbeat = 0u32;
    while !dead.load(Ordering::Acquire) && !smux.is_closed() && !kcp.is_closed() {
        knet::sleep_ms(500).await;
        // Periodic full state dump (debug): the only way to tell a stalled
        // *sender* (frozen snd_nxt with data queued) from a stalled *receiver*
        // (sender fine, peer's rcv_nxt frozen) on the same silent wire.
        if log::log_enabled!(log::Level::Debug) {
            heartbeat = heartbeat.wrapping_add(1);
            if heartbeat.is_multiple_of(4) {
                log::debug!(
                    "session state: snd_una={} snd_nxt={} rcv_nxt={} wait_send={} rmt_wnd={} dead={} closed={} | smux(bucket={} streams={}) | out_age_ms={} write_blocked_ms={} read_parked_ms={} | inbound_age_ms={} rx_age_ms={}",
                    kcp.snd_una(),
                    kcp.snd_nxt(),
                    kcp.rcv_nxt(),
                    kcp.wait_send(),
                    kcp.rmt_wnd(),
                    kcp.is_dead(),
                    kcp.is_closed(),
                    smux.token_bucket_value(),
                    smux.stream_count(),
                    knet::mono_ms().saturating_sub(out_state.last_out_ms.load(Ordering::Acquire)),
                    out_state.blocked_ms(knet::mono_ms()),
                    out_state.parked_ms(knet::mono_ms()),
                    knet::mono_ms().saturating_sub(out_state.last_inbound_ms.load(Ordering::Acquire)),
                    kcp.rx_age_ms(),
                );
            }
        }
        let now = knet::mono_ms();
        let timeout = smux.keepalive_timeout_secs();
        let inbound_age = now.saturating_sub(out_state.last_inbound_ms.load(Ordering::Acquire));
        // Datagram age is diagnostic only: it says whether the peer's transport
        // is still talking to us while its frames are missing.
        let rx_age = kcp.rx_age_ms();

        // Same guard as `smux.is_keepalive_timeout()`: while our receive window
        // is drained the peer's writer is blocked on us, so silence is expected.
        let timed_out = silence_is_fatal(timeout, inbound_age, smux.has_receive_capacity());

        let una = kcp.snd_una();
        let waiting = kcp.wait_send() > 0;
        // A closed peer window means it is out of buffer space and is choosing
        // not to take more data, so an unacknowledged backlog is flow control.
        let peer_window_open = kcp.rmt_wnd() > 0;
        // A peer that restarted its KCP never accepts our in-flight sequence
        // range again, so a frozen `snd_una` with a live peer is unrecoverable
        // and may be acted on at `ack_stall_ms`. Without that evidence the same
        // picture is produced by plain loss on our outbound path (measured on
        // the live path: 3-34s downlink dropouts), which KCP retransmission
        // recovers from, so it gets the long grace instead.
        let peer_restarted = kcp.peer_restart_count() > 0;
        let stall_window = stall_window_ms(ack_stall_ms, peer_restarted);
        let ack_stalled = stall_window > 0
            && ack_stall.sample(
                now,
                waiting,
                una,
                inbound_age,
                peer_window_open,
                stall_window,
            );
        if log::log_enabled!(log::Level::Debug) {
            // Only while acknowledgements are actually behind — a bulk transfer
            // keeps data in flight constantly and must not log every tick.
            if let Some(stalled) = ack_stall.stalled_ms(now) {
                log::debug!(
                    "session watchdog: no ack progress for {stalled}ms (snd_una={una}, wait_send={}, inbound_age={inbound_age}ms)",
                    kcp.wait_send()
                );
            }
        }

        let kcp_dead = kcp.is_dead();
        let smux_timeout = smux.is_keepalive_timeout();
        // A locally parked pump produces the same wire picture as a dead peer
        // (silence), so both have to be visible before drawing conclusions.
        let write_blocked_ms = out_state.blocked_ms(now);
        let read_parked_ms = out_state.parked_ms(now);
        let out_age_ms = now.saturating_sub(out_state.last_out_ms.load(Ordering::Acquire));
        if write_blocked_ms >= WRITE_BLOCK_WARN_MS && !write_stall_logged {
            // One line per blocking episode: a long stall would otherwise log
            // every 500ms tick and bury the restart of the flow.
            write_stall_logged = true;
            log::warn!(
                "session write loop blocked {write_blocked_ms}ms in kcp.write_all (wait_send={}, rmt_wnd={}, snd_una={una}, inbound_age={inbound_age}ms)",
                kcp.wait_send(),
                kcp.rmt_wnd()
            );
        } else if write_blocked_ms == 0 {
            write_stall_logged = false;
        }
        if kcp_dead || smux_timeout || timed_out || ack_stalled {
            log::warn!(
                "session watchdog: closing (kcp_dead={kcp_dead}, smux_timeout={smux_timeout}, inbound_idle={timed_out}, ack_stalled={ack_stalled}, peer_restart_seen={peer_restarted}, snd_una={una}, wait_send={}, rmt_wnd={}, bucket={}, out_age_ms={out_age_ms}, write_blocked_ms={write_blocked_ms}, read_parked_ms={read_parked_ms}, inbound_age_ms={inbound_age}, rx_age_ms={rx_age})",
                kcp.wait_send(),
                kcp.rmt_wnd(),
                smux.token_bucket_value()
            );
            dead.store(true, Ordering::Release);
            smux.close();
            kcp.close();
            break;
        }
    }
}

/// How long the session write loop waits for the shared blocking pool before
/// compressing inline instead.
///
/// The pool is shared by every CPU offload (Snappy here, crypto in
/// `KcpTransport`), so when all its workers are occupied — or, before the
/// tcpraw-accept fix, stuck on pending accepts — an unbounded wait parked the
/// whole session write loop: no data, no keepalive NOPs, until the peer's
/// silence watchdog killed the session. 200 ms sits orders of magnitude above
/// a normal ≤64 KiB Snappy frame encode (sub-millisecond), while still
/// bounding the damage of a starved pool to a slow batch instead of a mute.
const COMPRESS_CPU_BLOCK_WAIT: Duration = Duration::from_millis(200);

/// One Snappy frame-encode into the session encoder. Returns an empty buffer
/// on encoder failure (the batch is dropped, matching the pre-offload inline
/// behavior).
fn snappy_encode_frame(
    compressor: &Mutex<snap::write::FrameEncoder<Vec<u8>>>,
    plain: Bytes,
) -> Bytes {
    let mut encoder = compressor.lock();
    if encoder.write_all(&plain).is_err() || encoder.flush().is_err() {
        return Bytes::new();
    }
    std::mem::take::<Vec<u8>>(encoder.get_mut()).into()
}

/// Warn at most once per 30 s when compress offload overflows to inline.
fn warn_compress_offload_overflow() {
    static LAST_WARN_MS: AtomicU64 = AtomicU64::new(0);
    let now = knet::mono_ms();
    let last = LAST_WARN_MS.load(Ordering::Relaxed);
    if now.saturating_sub(last) >= 30_000
        && LAST_WARN_MS
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        log::warn!(
            "session write loop: Snappy offload exceeded {COMPRESS_CPU_BLOCK_WAIT:?}, compressing inline (blocking pool starved?)"
        );
    }
}

async fn write_loop(
    kcp: Arc<kcp_rs::KcpStream>,
    smux: Arc<smux_rs::Session>,
    compressor: Option<Arc<Mutex<snap::write::FrameEncoder<Vec<u8>>>>>,
    flush: Arc<knet::Notify>,
    dead: Arc<AtomicBool>,
    limiter: Arc<RateLimiter>,
    out_state: Arc<OutState>,
) {
    // KCP owns its own event-driven flush loop. Stream writes notify this task
    // directly, so this is only an idle health-check cadence. A 2ms timer per
    // session dominates short concurrent transfers.
    const IDLE_WAKE_MS: u64 = 10;
    let mut out = BytesMut::with_capacity(64 * 1024);
    let mut health = 0u32;
    while !dead.load(Ordering::Acquire) && !smux.is_closed() && !kcp.is_closed() {
        // Fast path: a wake is already pending (a stream write or the previous
        // iteration's `notify_one` preserved a permit) — await it directly so
        // idle iterations only pay the timer-wheel cost, not every iteration.
        if flush.has_pending() {
            flush.notified().await;
        } else {
            let _ = knet::timeout(Duration::from_millis(IDLE_WAKE_MS), flush.notified()).await;
        }
        if health == 0 {
            health = 50;
            if kcp.is_dead() || smux.is_keepalive_timeout() {
                break;
            }
            if smux.check_keepalive() {
                smux.keepalive_frame().encode(&mut out);
                smux.mark_keepalive_sent();
            }
        } else {
            health -= 1;
        }
        let has_pending_stream_data = smux
            .streams()
            .lock()
            .values()
            .any(|stream| stream.pending_send() > 0);
        let allow_fin = !has_pending_stream_data && kcp.wait_send() == 0;
        let fin_ids =
            smux.prepare_outbound_into_controlled(&mut out, 64 * 1024, smux.version(), allow_fin);

        // Reap stale streams via Session::remove_stream so receive-window
        // tokens for unread buffered bytes are recycled. A bare map remove +
        // close leaked those tokens; under YouTube-scale concurrent streams
        // (browser cancel / 0-recv pipes with unread SMUX data) the session
        // token bucket went permanently negative, read_loop parked, KCP
        // advertised rmt_wnd=0, and the whole session went mute.
        {
            let linger = Duration::from_secs(30);
            let streams = smux.streams();
            let stale: Vec<u32> = {
                let stream_map = streams.lock();
                stream_map
                    .iter()
                    .filter(|(_, stream)| {
                        (stream.is_local_closed()
                            && stream.is_remote_closed()
                            && stream.is_fin_sent())
                            || (stream.is_local_closed()
                                && stream.pending_send() == 0
                                && stream
                                    .local_closed_elapsed()
                                    .is_some_and(|elapsed| elapsed >= linger))
                    })
                    .map(|(id, _)| *id)
                    .collect()
            };
            drop(streams);
            for id in stale {
                smux.remove_stream(id);
            }
        }

        let packet: Option<Bytes> = if out.is_empty() {
            None
        } else if let Some(compressor) = compressor.as_ref() {
            let plain = out.split().freeze();
            let plain_len = plain.len();
            Some(if kcrypt_rs::should_cpu_block_compress(plain_len) {
                // Offload with a bounded wait. The job gets a refcounted clone
                // of `plain`; on timeout the original is still owned here, so
                // the fallback compresses inline with zero data loss (the
                // queued job's later result is simply dropped).
                let job_compressor = compressor.clone();
                let job_plain = plain.clone();
                match knet::timeout(
                    COMPRESS_CPU_BLOCK_WAIT,
                    knet::cpu_block(move || snappy_encode_frame(&job_compressor, job_plain)),
                )
                .await
                {
                    Ok(encoded) => encoded,
                    Err(_) => {
                        warn_compress_offload_overflow();
                        snappy_encode_frame(compressor, plain)
                    }
                }
            } else {
                snappy_encode_frame(compressor, plain)
            })
        } else {
            Some(out.split().freeze())
        };
        if let Some(packet) = packet.filter(|p| !p.is_empty()) {
            // Compatibility fallback for callers that provide a pre-built
            // KcpStream. Production client/server transports rate-limit the
            // encrypted/FEC batch below KCP and pass a disabled limiter here.
            loop {
                let wait = limiter.acquire(packet.len());
                if wait.is_zero() {
                    break;
                }
                knet::sleep(wait).await;
            }
            out_state.begin_write();
            let write_result = kcp.write_all(&packet).await;
            out_state.end_write();
            if let Err(e) = write_result {
                log::warn!(
                    "session write loop: kcp.write_all failed: {e} (closed={}, dead={}, snd_una={}, wait_send={}, rmt_wnd={})",
                    kcp.is_closed(),
                    kcp.is_dead(),
                    kcp.snd_una(),
                    kcp.wait_send(),
                    kcp.rmt_wnd()
                );
                break;
            }
            smux.mark_fins_sent(&fin_ids);

            // `prepare_outbound_into_controlled` deliberately caps each KCP
            // write to 64 KiB. If a stream still has queued data, preserve a
            // notify permit for the next iteration instead of imposing the
            // idle 2ms poll delay between chunks (a 128 KiB TCP write commonly
            // needs two iterations). Backpressure remains bounded by
            // `write_all`, which waits when the KCP window is full.
            if smux
                .streams()
                .lock()
                .values()
                .any(|stream| stream.pending_send() > 0)
            {
                flush.notify_one();
            }
        }
    }
    dead.store(true, Ordering::Release);
    smux.close();
    kcp.close();
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use super::*;
    use knet::{AsyncReadExt, AsyncWriteExt};
    use std::net::SocketAddr;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shared_client_server_session_roundtrip() {
        let a = knet::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let b = knet::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let addr_a = a.local_addr().unwrap();
        let addr_b = b.local_addr().unwrap();
        drop(a);
        drop(b);
        let socket_a = Arc::new(knet::DatagramSocket::Udp(
            knet::UdpSocket::connect(addr_a, addr_b).unwrap(),
        ));
        let socket_b = Arc::new(knet::DatagramSocket::Udp(
            knet::UdpSocket::connect(addr_b, addr_a).unwrap(),
        ));
        let config = kcp_rs::KcpConfig {
            conv: 0x51_55_58,
            mode: kcp_rs::KcpMode::Fast3,
            datashard: 3,
            parityshard: 1,
            ..Default::default()
        };
        let session_config = KcptunConfig {
            kcp: config,
            smux: smux_rs::Config {
                keepalive_interval: 0,
                keepalive_timeout: 0,
                ..smux_rs::DEFAULT_CONFIG.clone()
            },
            nocomp: false,
            rate_limit: 0,
            offload_profile: OffloadProfile::Tokio,
            ack_stall_secs: 10,
        };
        let key = b"0123456789abcdef0123456789abcdef";
        let client = KcptunSession::connect(socket_a, addr_b, key, "aes", &session_config)
            .await
            .unwrap();
        let server = Arc::new(
            KcptunSession::serve_transport(socket_b, addr_a, key, "aes", &session_config)
                .await
                .unwrap(),
        );

        let server_task = {
            let server = server.clone();
            knet::spawn_task(async move {
                let stream = server.accept().await.unwrap();
                let mut stream = smux_rs::SmuxIo::new(stream, server.flush_notify());
                let mut input = [0u8; 14];
                stream.read_exact(&mut input).await.unwrap();
                assert_eq!(&input, b"common-session");
                stream.write_all(b"roundtrip-ok").await.unwrap();
            })
        };
        let stream = client.open_stream().unwrap();
        let mut stream = smux_rs::SmuxIo::new(stream, client.flush_notify());
        stream.write_all(b"common-session").await.unwrap();
        let mut output = [0u8; 12];
        knet::timeout(Duration::from_secs(5), stream.read_exact(&mut output))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&output, b"roundtrip-ok");
        server_task.await.unwrap();
        client.close();
        server.close();
    }
}

/// Goroutine-specific latency regression for the complete KCP + SMUX session
/// path. It deliberately opens a fresh SMUX stream per message, mirroring the
/// proxy's TCP-accept path while excluding the outer TCP sockets. If this path
/// regresses to the writer loop's 10ms idle wake, the assertion catches it
/// before a full tunnel benchmark does.
#[cfg(test)]
mod goroutine_tests {
    use super::*;
    use knet::{AsyncReadExt, AsyncWriteExt};
    use std::net::SocketAddr;

    #[test]
    fn fresh_stream_echoes_do_not_wait_for_idle_flush_tick() {
        const ROUNDS: usize = 32;
        knet::block_on(async {
            let a = knet::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
            let b = knet::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
            let addr_a = a.local_addr().unwrap();
            let addr_b = b.local_addr().unwrap();
            drop(a);
            drop(b);
            let socket_a = Arc::new(knet::DatagramSocket::Udp(
                knet::UdpSocket::connect(addr_a, addr_b).unwrap(),
            ));
            let socket_b = Arc::new(knet::DatagramSocket::Udp(
                knet::UdpSocket::connect(addr_b, addr_a).unwrap(),
            ));
            let session_config = KcptunConfig {
                kcp: kcp_rs::KcpConfig {
                    conv: 0x51_55_58,
                    mode: kcp_rs::KcpMode::Fast3,
                    datashard: 0,
                    parityshard: 0,
                    ..Default::default()
                },
                smux: smux_rs::Config {
                    keepalive_interval: 0,
                    keepalive_timeout: 0,
                    ..smux_rs::DEFAULT_CONFIG.clone()
                },
                nocomp: true,
                rate_limit: 0,
                offload_profile: OffloadProfile::Tokio,
                ack_stall_secs: 10,
            };
            let key = b"0123456789abcdef0123456789abcdef";
            let client = KcptunSession::connect(socket_a, addr_b, key, "null", &session_config)
                .await
                .unwrap();
            let server = Arc::new(
                KcptunSession::serve_transport(socket_b, addr_a, key, "null", &session_config)
                    .await
                    .unwrap(),
            );
            let server_task = {
                let server = server.clone();
                knet::spawn_task(async move {
                    for _ in 0..ROUNDS {
                        let stream = server.accept().await.unwrap();
                        let mut stream = smux_rs::SmuxIo::new(stream, server.flush_notify());
                        let mut byte = [0u8; 1];
                        stream.read_exact(&mut byte).await.unwrap();
                        stream.write_all(&byte).await.unwrap();
                    }
                })
            };

            let mut worst = Duration::ZERO;
            let mut samples: Vec<Duration> = Vec::with_capacity(ROUNDS);
            for value in 0..ROUNDS as u8 {
                let started = std::time::Instant::now();
                let stream = client.open_stream().unwrap();
                let mut stream = smux_rs::SmuxIo::new(stream, client.flush_notify());
                stream.write_all(&[value]).await.unwrap();
                let mut echoed = [0u8; 1];
                // Bounded read: an unbounded read_exact turned any lost-wake /
                // starvation bug into a >60 s test hang instead of a fast panic.
                knet::timeout(Duration::from_secs(3), stream.read_exact(&mut echoed))
                    .await
                    .expect("echo read timed out (stream write stalled)")
                    .unwrap();
                assert_eq!(echoed, [value]);
                samples.push(started.elapsed());
                worst = worst.max(started.elapsed());
            }
            server_task.await.unwrap();
            client.close();
            server.close();
            // Regression guard for "writer waits for the idle flush tick":
            // the MEDIAN must stay in the sub-millisecond notify path. The
            // worst case on a loaded desktop can absorb one 10 ms idle tick
            // through scheduler jitter alone (IDLE_WAKE_MS), so a hard worst
            // bound only guards gross stalls, not the tick itself.
            samples.sort();
            let median = samples[samples.len() / 2];
            assert!(
                median < Duration::from_millis(5),
                "fresh stream echo median waited for idle flush tick: {median:?} (worst {worst:?})"
            );
            assert!(
                worst < Duration::from_millis(50),
                "fresh stream echo worst-case stall: {worst:?}"
            );
        });
    }

    /// Exercise the production forwarding shape end to end: a fresh local TCP
    /// connection is piped to a fresh SMUX stream, the server side opens a
    /// fresh target TCP connection and pipes it back to a TCP echo listener.
    /// The lighter stream-only test above cannot catch a wake lost between the
    /// two `copy_bidirectional` state machines.
    #[test]
    fn tcp_smux_tcp_proxy_does_not_wait_for_idle_ticks() {
        const ROUNDS: usize = 16;
        knet::block_on(async {
            let a = knet::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
            let b = knet::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
            let addr_a = a.local_addr().unwrap();
            let addr_b = b.local_addr().unwrap();
            drop(a);
            drop(b);
            let socket_a = Arc::new(knet::DatagramSocket::Udp(
                knet::UdpSocket::connect(addr_a, addr_b).unwrap(),
            ));
            let socket_b = Arc::new(knet::DatagramSocket::Udp(
                knet::UdpSocket::connect(addr_b, addr_a).unwrap(),
            ));
            let session_config = KcptunConfig {
                kcp: kcp_rs::KcpConfig {
                    conv: 0x51_55_58,
                    mode: kcp_rs::KcpMode::Fast3,
                    datashard: 0,
                    parityshard: 0,
                    ..Default::default()
                },
                smux: smux_rs::Config {
                    keepalive_interval: 0,
                    keepalive_timeout: 0,
                    ..smux_rs::DEFAULT_CONFIG.clone()
                },
                nocomp: true,
                rate_limit: 0,
                offload_profile: OffloadProfile::Tokio,
                ack_stall_secs: 10,
            };
            let key = b"0123456789abcdef0123456789abcdef";
            let client = Arc::new(
                KcptunSession::connect(socket_a, addr_b, key, "null", &session_config)
                    .await
                    .unwrap(),
            );
            let server = Arc::new(
                KcptunSession::serve_transport(socket_b, addr_a, key, "null", &session_config)
                    .await
                    .unwrap(),
            );

            let target_listener = knet::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                .await
                .unwrap();
            let target_addr = target_listener.local_addr().unwrap();
            let target_task = knet::spawn_task(async move {
                let mut handlers = Vec::with_capacity(ROUNDS);
                for _ in 0..ROUNDS {
                    let (mut target, _) = target_listener.accept().await.unwrap();
                    handlers.push(knet::spawn_task(async move {
                        let mut byte = [0u8; 1];
                        target.read_exact(&mut byte).await.unwrap();
                        target.write_all(&byte).await.unwrap();
                    }));
                }
                for handler in handlers {
                    handler.await.unwrap();
                }
            });

            let server_task = {
                let server = server.clone();
                knet::spawn_task(async move {
                    let mut handlers = Vec::with_capacity(ROUNDS);
                    for _ in 0..ROUNDS {
                        let stream = server.accept().await.unwrap();
                        let notify = server.flush_notify();
                        handlers.push(knet::spawn_task(async move {
                            let mut target = knet::TcpStream::connect(target_addr.to_string())
                                .await
                                .unwrap();
                            let mut smux = smux_rs::SmuxIo::new(stream, notify);
                            crate::pipe(&mut target, &mut smux, 0).await.unwrap();
                        }));
                    }
                    for handler in handlers {
                        handler.await.unwrap();
                    }
                })
            };

            let local_listener = knet::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                .await
                .unwrap();
            let local_addr = local_listener.local_addr().unwrap();
            let client_task = {
                let client = client.clone();
                knet::spawn_task(async move {
                    let mut handlers = Vec::with_capacity(ROUNDS);
                    for _ in 0..ROUNDS {
                        let (mut local, _) = local_listener.accept().await.unwrap();
                        let stream = client.open_stream().unwrap();
                        let notify = client.flush_notify();
                        handlers.push(knet::spawn_task(async move {
                            let mut smux = smux_rs::SmuxIo::new(stream, notify);
                            crate::pipe(&mut local, &mut smux, 0).await.unwrap();
                        }));
                    }
                    for handler in handlers {
                        handler.await.unwrap();
                    }
                })
            };

            let mut worst = Duration::ZERO;
            let mut samples: Vec<Duration> = Vec::with_capacity(ROUNDS);
            for value in 0..ROUNDS as u8 {
                let started = std::time::Instant::now();
                let mut local = knet::TcpStream::connect(local_addr.to_string())
                    .await
                    .unwrap();
                local.write_all(&[value]).await.unwrap();
                let mut echoed = [0u8; 1];
                local.read_exact(&mut echoed).await.unwrap();
                assert_eq!(echoed, [value]);
                samples.push(started.elapsed());
                worst = worst.max(started.elapsed());
            }
            client_task.await.unwrap();
            server_task.await.unwrap();
            target_task.await.unwrap();
            client.close();
            server.close();
            samples.sort();
            let median = samples[samples.len() / 2];
            assert!(
                median < Duration::from_millis(5),
                "full TCP/SMUX/TCP forwarding median waited for an idle tick: {median:?} (worst {worst:?})"
            );
            assert!(
                worst < Duration::from_millis(50),
                "full TCP/SMUX/TCP forwarding worst-case stall: {worst:?}"
            );
        });
    }
}
