//! Client DialOptions, session building, stream handler, and expiry logic.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use log::{debug, info, warn};

use crate::socket;

pub(crate) type SessionRef = Arc<kcptun_common::KcptunSession>;
pub(crate) type SessionPool = Arc<parking_lot::Mutex<Vec<SessionRef>>>;

/// CLI-derived options retained for initial dial and reconnect.
#[derive(Clone, Debug)]
pub(crate) struct ClientDialOptions {
    pub(crate) crypt: String,
    pub(crate) mode: String,
    pub(crate) mtu: u32,
    pub(crate) sndwnd: u32,
    pub(crate) rcvwnd: u32,
    pub(crate) datashard: u32,
    pub(crate) parityshard: u32,
    pub(crate) acknodelay: bool,
    pub(crate) nodelay: u32,
    pub(crate) interval: u32,
    pub(crate) resend: u32,
    pub(crate) nc: u32,
    pub(crate) smuxver: u8,
    pub(crate) smuxbuf: usize,
    pub(crate) streambuf: usize,
    pub(crate) framesize: usize,
    pub(crate) keepalive: u64,
    pub(crate) keepalivetimeout: u64,
    pub(crate) ackstalltimeout: u64,
    pub(crate) nocomp: bool,
    pub(crate) ratelimit: u32,
}

/// Build a [`KcptunSession`] for the given remote address and options.
pub(crate) async fn build_session(
    remote: SocketAddr,
    key: &[u8; 32],
    cfg: &ClientDialOptions,
    socket: Arc<knet::DatagramSocket>,
) -> Result<kcptun_common::KcptunSession> {
    let params = kcptun_common::KcpCliParams {
        mode: cfg.mode.clone(),
        mtu: cfg.mtu,
        sndwnd: cfg.sndwnd,
        rcvwnd: cfg.rcvwnd,
        datashard: cfg.datashard,
        parityshard: cfg.parityshard,
        acknodelay: cfg.acknodelay,
        nodelay: cfg.nodelay,
        interval: cfg.interval,
        resend: cfg.resend,
        nc: cfg.nc,
        conv: crate::DEFAULT_CONV,
        token: 0,
    };
    let smux = smux_rs::Config {
        version: cfg.smuxver,
        max_receive_buffer: cfg.smuxbuf,
        max_stream_buffer: cfg.streambuf,
        max_frame_size: cfg.framesize,
        keepalive_interval: cfg.keepalive,
        // Go's BuildSmuxConfig leaves the timeout at 30s; it is configurable
        // here because on a lossy path the 30s inbound-silence rule turns a
        // transient dropout into a session teardown that kills every stream on
        // it (measured: 20MB downloads cut to 0-3MB, one session death each).
        keepalive_timeout: cfg.keepalivetimeout,
    };
    let config = kcptun_common::KcptunConfig {
        kcp: params.to_kcp_config(),
        smux,
        nocomp: cfg.nocomp,
        rate_limit: cfg.ratelimit,
        offload_profile: kcrypt_rs::OffloadProfile::Tokio,
        ack_stall_secs: cfg.ackstalltimeout,
    };
    kcptun_common::KcptunSession::connect(socket, remote, key, &cfg.crypt, &config).await
}

/// Check if a session has exceeded its auto-expire deadline.
///
/// Uses the session **creation time** (matching Go kcptun), NOT last
/// activity.  Go sets `expiryDate = creation + autoexpire` and reconnects
/// when `now > expiryDate`.  This is independent of keepalive — a session
/// is replaced after `autoexpire` seconds regardless of traffic.
pub(crate) fn is_session_expired(
    session: &kcptun_common::KcptunSession,
    autoexpire_secs: u64,
) -> bool {
    is_creation_expired_at(session.created_ms(), autoexpire_secs, knet::mono_ms())
}

/// Check if a session has exceeded its scavenger deadline.
///
/// Go scavenger deadline = `creation + autoexpire + scavengeTTL`.
/// If the session is still alive at this point, force-close it.
pub(crate) fn is_session_scavenge_expired(
    session: &kcptun_common::KcptunSession,
    autoexpire_secs: u64,
    scavengettl_secs: u64,
) -> bool {
    is_creation_scavenge_expired_at(
        session.created_ms(),
        autoexpire_secs,
        scavengettl_secs,
        knet::mono_ms(),
    )
}

/// Check if `created_ms + autoexpire` has passed at `now_ms`.
pub(crate) fn is_creation_expired_at(created_ms: u64, autoexpire_secs: u64, now_ms: u64) -> bool {
    now_ms > created_ms + autoexpire_secs * 1000
}

/// Check if `created_ms + autoexpire + scavengettl` has passed at `now_ms`.
pub(crate) fn is_creation_scavenge_expired_at(
    created_ms: u64,
    autoexpire_secs: u64,
    scavengettl_secs: u64,
    now_ms: u64,
) -> bool {
    now_ms > created_ms + (autoexpire_secs + scavengettl_secs) * 1000
}

/// Whether a replaced ("retired") session may be closed now.
///
/// Go hands a replaced session to the scavenger, which closes it once
/// `creation + autoexpire + scavengeTTL` has passed — dropping whatever was
/// still in flight on it. Here the deadline is the same, but the session must
/// also have **finished serving**: while it still has streams, it keeps
/// running so an in-progress transfer (a video download, say) completes even
/// though its pool slot was already replaced. The session's own liveness
/// watchdog still applies, so a retired session whose peer dies is torn down
/// as usual instead of lingering.
pub(crate) fn should_retire_session(
    session: &kcptun_common::KcptunSession,
    autoexpire_secs: u64,
    scavengettl_secs: u64,
    now_ms: u64,
) -> bool {
    should_close_retired(
        session.created_ms(),
        autoexpire_secs,
        scavengettl_secs,
        now_ms,
        session.is_dead(),
        session.active_stream_count(),
    )
}

/// Pure form of [`should_retire_session`].
fn should_close_retired(
    created_ms: u64,
    autoexpire_secs: u64,
    scavengettl_secs: u64,
    now_ms: u64,
    dead: bool,
    active_streams: usize,
) -> bool {
    if dead {
        return true;
    }
    active_streams == 0
        && is_creation_scavenge_expired_at(created_ms, autoexpire_secs, scavengettl_secs, now_ms)
}

/// Handle a single client connection: pipe between local TCP and SMUX stream
/// with optional QPP. Compression is handled at the KCP/SMUX session level
/// (matching Go kcptun architecture).
#[allow(clippy::too_many_arguments)]
#[cfg_attr(not(feature = "qpp"), allow(unused_variables))]
pub(crate) async fn handle_client(
    local: knet::TcpStream,
    smux_stream: Arc<smux_rs::stream::Stream>,
    qpp_enabled: bool,
    qpp_key: Vec<u8>,
    qpp_count: u16,
    quiet: bool,
    flush_notify: Arc<knet::Notify>,
    closewait: u64,
) -> Result<()> {
    smux_stream.set_flush_notify(flush_notify.clone());
    let smux_async = smux_rs::SmuxIo::new(smux_stream.clone(), flush_notify);

    // Go starts closewait when either copy direction completes; the reverse
    // direction remains active during that grace period.
    let pipe_result = if qpp_enabled {
        #[cfg(feature = "qpp")]
        {
            let qpp_port = kcptun_common::QPPPort::new(smux_async, &qpp_key, qpp_count);
            let mut local_pin = local;
            let mut qpp_pin = qpp_port;
            kcptun_common::pipe(&mut local_pin, &mut qpp_pin, closewait).await
        }
        #[cfg(not(feature = "qpp"))]
        {
            unreachable!("qpp_enabled should be false when qpp feature disabled")
        }
    } else {
        let mut local_pin = local;
        let mut smux_pin = smux_async;
        kcptun_common::pipe(&mut local_pin, &mut smux_pin, closewait).await
    };

    // Local half-close only. Do NOT mark_fin_sent here — that blocked the flush
    // loop from ever encoding a real FIN (BUGREPORT_PROXY_MEMORY_GROWTH).
    // Flush marks fin_sent after FIN is queued; linger reaps if peer never FINs.
    //
    // IMPORTANT: Do NOT call clear_buffers() here. The flush loop may still have
    // data in the stream's send_buf that has been written by the pipe but not yet
    // drained and sent through KCP. Clearing buffers would discard unsent data,
    // causing silent data loss (observed in stress tests as truncated/zero responses).
    // The flush loop is responsible for draining send_buf, sending FIN after drain,
    // and the linger reaper will eventually clear buffers after is_fin_sent + timeout.
    smux_stream.mark_local_closed();
    // Do not clear buffers — let flush drain and linger reap.

    match pipe_result {
        Ok((a, b)) => {
            #[cfg(feature = "qpp")]
            let qpp_suffix = if qpp_enabled { " (QPP)" } else { "" };
            #[cfg(not(feature = "qpp"))]
            let qpp_suffix = "";
            if !quiet {
                info!("pipe completed: {} sent, {} recv{}", a, b, qpp_suffix);
            }
        }
        Err(e) => {
            warn!("pipe error: {}", e);
        }
    }

    Ok(())
}

/// Reconnect a dead session at the given index, returning true on success.
///
/// The replaced session is *retired*, not killed: it moves out of the pool and
/// onto the scavenger list, and keeps serving whatever it still has in flight
/// — the scavenger closes it once its grace period has passed **and** it has
/// no streams left (see [`should_retire_session`]). Tearing it down here would
/// abort a download that happens to be running when an `autoexpire` slot is
/// replaced, which surfaces to users as a stalled video or page.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn reconnect_session(
    conns: &SessionPool,
    tracked_sessions: Option<&SessionPool>,
    idx: usize,
    remote_addr: &str,
    key: &[u8; 32],
    session_cfg: &ClientDialOptions,
    tcp: bool,
    sockbuf: u32,
    dscp: u32,
) -> bool {
    let remote = match kcptun_common::random_remote_addr(remote_addr) {
        Ok(remote) => {
            debug!(
                "reconnect idx={}: remote_addr=\"{}\" -> selected {}:{}",
                idx,
                remote_addr,
                remote.ip(),
                remote.port()
            );
            remote
        }
        Err(e) => {
            log::error!("reconnect remote address resolution failed: {:#}", e);
            return false;
        }
    };
    info!("connection {} is dead, reconnecting to {}...", idx, remote);
    let socket = match socket::create_client_socket(remote, tcp, sockbuf, dscp) {
        Ok(s) => s,
        Err(e) => {
            log::error!("reconnect socket creation failed: {:#}", e);
            return false;
        }
    };
    match build_session(remote, key, session_cfg, socket).await {
        Ok(new_conn) => {
            let new_conn = Arc::new(new_conn);
            // Swap under the pool lock; the retired session is handled outside
            // it (closing under the lock can re-enter via Drop/task wakeups).
            let old = {
                let mut guard = conns.lock();
                std::mem::replace(&mut guard[idx], new_conn.clone())
            };
            match tracked_sessions {
                // Hand the retired session to the scavenger: it stays on the
                // list and keeps serving its streams until the grace period
                // has passed and they are done.
                Some(tracked_sessions) => {
                    let mut tracked = tracked_sessions.lock();
                    tracked.retain(|s| !Arc::ptr_eq(s, &new_conn) && !Arc::ptr_eq(s, &old));
                    tracked.push(old);
                    tracked.push(new_conn);
                }
                // No scavenger (`--autoexpire 0`): a replacement only happens
                // for a session that is already dead, so tear it down now
                // instead of leaking its tasks.
                None => {
                    knet::spawn_task(async move {
                        old.close();
                    });
                }
            }
            info!("connection {} reconnected", idx);
            true
        }
        Err(e) => {
            log::error!("reconnect connection {} failed: {:#}", idx, e);
            false
        }
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_not_expired_when_within_autoexpire() {
        // created at 1000ms, autoexpire=3600s, now=1001ms → not expired
        assert!(!is_creation_expired_at(1_000, 3600, 1_001));
    }

    #[test]
    fn test_expired_after_autoexpire() {
        // created at 1000ms, autoexpire=0s, now=1001ms → expired
        assert!(is_creation_expired_at(1_000, 0, 1_001));
    }

    #[test]
    fn test_scavenge_not_expired_within_grace() {
        // created at 1000ms, autoexpire=0, scavengettl=3600s, now=1001ms
        assert!(!is_creation_scavenge_expired_at(1_000, 0, 3600, 1_001));
    }

    #[test]
    fn test_scavenge_expired_after_grace() {
        // created at 1000ms, autoexpire=0, scavengettl=0, now=1001ms
        assert!(is_creation_scavenge_expired_at(1_000, 0, 0, 1_001));
    }

    /// A retired session is only closed once it is dead, or once its grace
    /// period has passed *and* it has finished serving.
    #[test]
    fn test_retired_session_waits_for_streams() {
        const CREATED: u64 = 0;
        const AUTOEXPIRE: u64 = 300;
        const TTL: u64 = 600;
        let past_ttl = (AUTOEXPIRE + TTL + 1) * 1000;

        // Still inside the grace period: keep it even when idle.
        assert!(!should_close_retired(
            CREATED,
            AUTOEXPIRE,
            TTL,
            past_ttl - 1000,
            false,
            0
        ));

        // Past the grace period and idle: retire.
        assert!(should_close_retired(
            CREATED, AUTOEXPIRE, TTL, past_ttl, false, 0
        ));

        // Past the grace period but still serving: keep it so the transfer in
        // flight completes (this is the fix — closing here aborts downloads
        // whenever an autoexpire slot is replaced).
        assert!(!should_close_retired(
            CREATED, AUTOEXPIRE, TTL, past_ttl, false, 1
        ));
        assert!(!should_close_retired(
            CREATED,
            AUTOEXPIRE,
            TTL,
            past_ttl * 10,
            false,
            3
        ));

        // A dead session is torn down regardless of its streams.
        assert!(should_close_retired(
            CREATED, AUTOEXPIRE, TTL, past_ttl, true, 5
        ));
        assert!(should_close_retired(CREATED, AUTOEXPIRE, TTL, 0, true, 0));
    }
}
