//! Socket buffer sizing that survives a kernel that clamps it.
//!
//! A KCP stack sizes its socket buffers to the window it can put in flight
//! (hundreds of KB). The unprivileged `SO_RCVBUF` / `SO_SNDBUF` request is
//! silently capped at `net.core.{r,w}mem_max`, and inside a container that
//! sysctl is normally owned by the host: this project's VPS grants 266,240
//! bytes of accounting (~208 KB of 1200-byte payload, ~173 datagrams) against
//! a 600 KB send window. The mismatch does not fail loudly — the extra
//! datagrams are dropped at the socket queue, and on OpenVZ even
//! `Udp.RcvbufErrors` stays at zero while `Udp.InErrors` moves, so the loss is
//! invisible to every counter an operator would look at.
//!
//! `SO_*BUFFFORCE` is the same option without the sysctl clamp. It needs
//! CAP_NET_ADMIN, which container root normally holds. Escalation here is best
//! effort: without the capability the plain value stands and the caller can
//! report what the kernel actually granted.

use socket2::Socket;

/// Linux `SO_*BUFFFORCE` — `SO_*BUF` without the `net.core.{r,w}mem_max` cap.
#[cfg(target_os = "linux")]
const SO_SNDBUFFORCE: libc::c_int = libc::SO_SNDBUFFORCE;
#[cfg(target_os = "linux")]
const SO_RCVBUFFORCE: libc::c_int = libc::SO_RCVBUFFORCE;

/// What the kernel granted, as `getsockopt` reports it.
///
/// These are the kernel's *accounting* values, which is twice the requested
/// bytes on Linux when the request fits under the cap (and which is what the
/// queue is actually measured against), so they are not directly comparable to
/// the requested number without that factor in mind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SocketBuffers {
    /// Bytes the caller asked for.
    pub requested: usize,
    pub recv: usize,
    pub send: usize,
}

impl SocketBuffers {
    /// Both directions got at least what was requested.
    #[inline]
    pub fn granted(&self) -> bool {
        self.recv >= self.requested && self.send >= self.requested
    }
}

/// Apply `bytes` to both directions, escalating to the `*FORCE` variants when
/// the plain request came back clamped, and return what the kernel holds.
///
/// Errors on the plain path are recorded rather than returned: a socket that
/// keeps the system default still works, it just queues less, and callers want
/// the effective numbers rather than a failure.
pub fn set_socket_buffers(socket: &Socket, bytes: usize) -> SocketBuffers {
    let _ = socket.set_recv_buffer_size(bytes);
    let _ = socket.set_send_buffer_size(bytes);

    let bufs = SocketBuffers {
        requested: bytes,
        recv: socket.recv_buffer_size().unwrap_or(0),
        send: socket.send_buffer_size().unwrap_or(0),
    };

    #[cfg(target_os = "linux")]
    let bufs = escalate(socket, bufs);

    bufs
}

/// Retry a clamped direction with the matching `*FORCE` option.
#[cfg(target_os = "linux")]
fn escalate(socket: &Socket, mut bufs: SocketBuffers) -> SocketBuffers {
    if bufs.recv < bufs.requested && force_buffer(socket, SO_RCVBUFFORCE, bufs.requested) {
        bufs.recv = socket.recv_buffer_size().unwrap_or(bufs.recv);
    }
    if bufs.send < bufs.requested && force_buffer(socket, SO_SNDBUFFORCE, bufs.requested) {
        bufs.send = socket.send_buffer_size().unwrap_or(bufs.send);
    }
    bufs
}

/// `setsockopt(SOL_SOCKET, opt, bytes)` bypassing the sysctl cap.
/// Returns `false` (leaving the caller's value untouched) on EPERM or any
/// other error.
#[cfg(target_os = "linux")]
fn force_buffer(socket: &Socket, opt: libc::c_int, bytes: usize) -> bool {
    use std::os::unix::io::AsRawFd;

    let val = bytes.min(libc::c_int::MAX as usize) as libc::c_int;
    // SAFETY: `val` is a live c_int and its size matches the length passed.
    unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            opt,
            &val as *const libc::c_int as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        ) == 0
    }
}

/// One-line summary for startup logs: what was asked for versus what the
/// kernel holds, with the accounting factor spelled out when they differ.
pub fn describe(bufs: &SocketBuffers) -> String {
    if bufs.granted() {
        format!(
            "requested={} effective recv={} send={}",
            bufs.requested, bufs.recv, bufs.send
        )
    } else {
        format!(
            "requested={} effective recv={} send={} (clamped by net.core.{{r,w}}mem_max)",
            bufs.requested, bufs.recv, bufs.send
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn udp_socket() -> Socket {
        Socket::new(socket2::Domain::IPV4, socket2::Type::DGRAM, None).unwrap()
    }

    #[test]
    fn requested_size_is_honoured_or_reported() {
        let socket = udp_socket();
        // 64 KiB is under every platform's floor for an unprivileged request,
        // so this asserts the plain path works and the read-back is real.
        let bufs = set_socket_buffers(&socket, 64 * 1024);
        assert_eq!(bufs.requested, 64 * 1024);
        assert!(bufs.recv >= 64 * 1024, "recv={}", bufs.recv);
        assert!(bufs.send >= 64 * 1024, "send={}", bufs.send);
        assert!(bufs.granted());
    }

    #[test]
    fn describe_mentions_the_clamp_only_when_clamped() {
        let granted = SocketBuffers {
            requested: 4096,
            recv: 8192,
            send: 8192,
        };
        assert!(!describe(&granted).contains("clamped"));

        let clamped = SocketBuffers {
            requested: 4 * 1024 * 1024,
            recv: 266_240,
            send: 266_240,
        };
        assert!(describe(&clamped).contains("clamped"));
        assert!(!clamped.granted());
    }
}
