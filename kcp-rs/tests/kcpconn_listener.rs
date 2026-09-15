//! End-to-end tests for the server **listen** / client **connect** path:
//! [`KcpListener`] multi-peer accept + [`KcpConn::connect`] dial.
//!
//! NOTE: gated on `async-tokio`/`async-smol`, which no longer exist as kcp-rs
//! features — this whole target currently compiles to nothing. Reviving it
//! needs the `kio` → `knet` rename plus API drift fixes.
//!
//! ```text
//! cargo test -p kcp-rs --features async-tokio --test kcpconn_listener
//! cargo test -p kcp-rs --features async-smol  --test kcpconn_listener
//! ```
//!
//! A client dials a real listener over localhost UDP, the listener accepts a
//! per-peer `KcpConn`, and payloads must round-trip byte-for-byte.

#![cfg(any(feature = "async-tokio", feature = "async-smol"))]

use std::net::SocketAddr;
use std::time::Duration;

use kcp_rs::listener::KcpListenerLimits;
use kcp_rs::{KcpConn, KcpListener, KcpMode};
use kio::{AsyncReadExt, AsyncWriteExt};

const CONV: u32 = 0x00C0_FFEE;

/// Deterministic, non-trivial payload pattern.
fn make_payload(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| ((i * 131 + seed as usize) % 251) as u8)
        .collect()
}

/// FNV-1a 64 — independent checksum cross-check on top of byte equality.
fn fnv1a(data: &[u8]) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01B3);
    }
    h
}

/// Read exactly `buf.len()` bytes, polling with a timeout.
async fn read_exact_timeout(conn: &mut KcpConn, buf: &mut [u8], limit: Duration) {
    let deadline = std::time::Instant::now() + limit;
    let mut filled = 0usize;
    while filled < buf.len() {
        if std::time::Instant::now() > deadline {
            panic!("timeout waiting for data, got {}/{}", filled, buf.len());
        }
        match kio::timeout(Duration::from_millis(200), conn.read(&mut buf[filled..])).await {
            Ok(Ok(0)) => panic!("unexpected EOF at {}", filled),
            Ok(Ok(n)) => filled += n,
            Ok(Err(e)) => panic!("read error: {}", e),
            Err(_) => continue,
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

/// Client dials the listener, server accepts, 256 KiB echoes back byte-exact.
#[test]
fn listener_accept_echo_roundtrip() {
    kio::block_on(async {
        let listener = KcpListener::bind("127.0.0.1:0")
            .conv(CONV)
            .mode(KcpMode::Fast3)
            .sndwnd(512)
            .rcvwnd(512)
            .build()
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();

        // Dial path: KcpConn::connect (fresh ephemeral UDP socket).
        let mut client = KcpConn::connect(addr)
            .conv(CONV)
            .mode(KcpMode::Fast3)
            .sndwnd(512)
            .rcvwnd(512)
            .build()
            .await
            .unwrap();

        // The client's first flush creates the server-side session.
        let payload = make_payload(256 * 1024, 11);
        client.write_all(&payload).await.unwrap();
        client.flush().await.unwrap();

        let (mut server, peer) = listener.accept().await.unwrap();
        assert_eq!(peer, client.local_addr().unwrap(), "accepted peer addr");

        // client → server
        let mut got = vec![0u8; payload.len()];
        read_exact_timeout(&mut server, &mut got, Duration::from_secs(30)).await;
        assert_eq!(got, payload, "server received wrong bytes");
        assert_eq!(fnv1a(&got), fnv1a(&payload), "server checksum mismatch");

        // server → client (echo)
        server.write_all(&got).await.unwrap();
        server.flush().await.unwrap();
        let mut back = vec![0u8; payload.len()];
        read_exact_timeout(&mut client, &mut back, Duration::from_secs(30)).await;
        assert_eq!(back, payload, "client received wrong bytes");
        assert_eq!(fnv1a(&back), fnv1a(&payload), "client checksum mismatch");

        drop(client);
        drop(server);
        listener.close();
    });
}

/// One listener, two clients: each accepted `KcpConn` sees exactly its own
/// peer's bytes (peer demux).
#[test]
fn listener_multiple_peers_demux() {
    kio::block_on(async {
        let listener = KcpListener::bind("127.0.0.1:0")
            .conv(CONV)
            .mode(KcpMode::Fast3)
            .sndwnd(512)
            .rcvwnd(512)
            .build()
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();

        let mut c1 = KcpConn::connect(addr)
            .conv(CONV)
            .mode(KcpMode::Fast3)
            .sndwnd(512)
            .rcvwnd(512)
            .build()
            .await
            .unwrap();
        let mut c2 = KcpConn::connect(addr)
            .conv(CONV)
            .mode(KcpMode::Fast3)
            .sndwnd(512)
            .rcvwnd(512)
            .build()
            .await
            .unwrap();

        let p1_data = make_payload(64 * 1024, 1);
        let p2_data = make_payload(64 * 1024, 2);
        c1.write_all(&p1_data).await.unwrap();
        c2.write_all(&p2_data).await.unwrap();
        c1.flush().await.unwrap();
        c2.flush().await.unwrap();

        let addr_c1 = c1.local_addr().unwrap();
        let addr_c2 = c2.local_addr().unwrap();
        let (mut s_a, p_a) = listener.accept().await.unwrap();
        let (mut s_b, p_b) = listener.accept().await.unwrap();

        // Each accepted conn is the peer that dialed it — data must not leak.
        let (expected_a, expected_b) = if p_a == addr_c1 {
            (p1_data.clone(), p2_data.clone())
        } else {
            assert_eq!(p_a, addr_c2, "unexpected peer address");
            (p2_data.clone(), p1_data.clone())
        };
        assert_eq!(p_b, if p_a == addr_c1 { addr_c2 } else { addr_c1 });

        let mut got_a = vec![0u8; expected_a.len()];
        let mut got_b = vec![0u8; expected_b.len()];
        read_exact_timeout(&mut s_a, &mut got_a, Duration::from_secs(30)).await;
        read_exact_timeout(&mut s_b, &mut got_b, Duration::from_secs(30)).await;
        assert_eq!(got_a, expected_a, "peer A received another peer's bytes");
        assert_eq!(got_b, expected_b, "peer B received another peer's bytes");

        drop(c1);
        drop(c2);
        drop(s_a);
        drop(s_b);
        listener.close();
    });
}

/// The configured drain limit counts the packet that woke the reader. With a
/// one-packet quantum, two peers still make progress in separate wakeups.
#[test]
fn listener_drain_limit_counts_first_packet() {
    kio::block_on(async {
        let listener = KcpListener::bind("127.0.0.1:0")
            .conv(CONV)
            .limits(KcpListenerLimits {
                max_drain_packets: 1,
                ..KcpListenerLimits::default()
            })
            .build()
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let mut c1 = KcpConn::connect(addr).conv(CONV).build().await.unwrap();
        let mut c2 = KcpConn::connect(addr).conv(CONV).build().await.unwrap();
        c1.write_all(b"a").await.unwrap();
        c2.write_all(b"b").await.unwrap();

        let (mut s1, _) = listener
            .accept_timeout(Duration::from_secs(2))
            .await
            .unwrap();
        let (mut s2, _) = listener
            .accept_timeout(Duration::from_secs(2))
            .await
            .unwrap();
        let mut a = [0u8; 1];
        let mut b = [0u8; 1];
        read_exact_timeout(&mut s1, &mut a, Duration::from_secs(2)).await;
        read_exact_timeout(&mut s2, &mut b, Duration::from_secs(2)).await;
        assert!(matches!(a[0], b'a' | b'b'));
        assert!(matches!(b[0], b'a' | b'b'));
        assert_ne!(a, b, "each peer must retain its own routed packet");
        c1.close();
        c2.close();
        s1.close();
        s2.close();
        listener.close();
    });
}

/// After a connection is fully closed on both sides, the listener keeps
/// accepting and serving fresh clients.
///
/// (A true "same socket re-dials" reconnect is not viable at the KCP layer:
/// the continuing client SN stream would not match the fresh server session's
/// `rcv_nxt = 0`. Real reconnects start a new client session with SN from 0.)
#[test]
fn listener_serves_new_client_after_previous_closed() {
    kio::block_on(async {
        let listener = KcpListener::bind("127.0.0.1:0")
            .conv(CONV)
            .mode(KcpMode::Fast3)
            .sndwnd(512)
            .rcvwnd(512)
            .build()
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();

        // Connection 1: fully served, then closed on both sides.
        let mut c1 = KcpConn::connect(addr)
            .conv(CONV)
            .mode(KcpMode::Fast3)
            .build()
            .await
            .unwrap();
        c1.write_all(b"one").await.unwrap();
        c1.flush().await.unwrap();
        let (mut s1, _p1) = listener.accept().await.unwrap();
        let mut g1 = vec![0u8; 3];
        read_exact_timeout(&mut s1, &mut g1, Duration::from_secs(10)).await;
        assert_eq!(&g1, b"one");
        drop(s1);
        drop(c1);

        // Connection 2: fresh client session (SN starts at 0) is served.
        let mut c2 = KcpConn::connect(addr)
            .conv(CONV)
            .mode(KcpMode::Fast3)
            .build()
            .await
            .unwrap();
        c2.write_all(b"two").await.unwrap();
        c2.flush().await.unwrap();
        let (mut s2, p2) = listener.accept().await.unwrap();
        assert_eq!(p2, c2.local_addr().unwrap());
        let mut g2 = vec![0u8; 3];
        read_exact_timeout(&mut s2, &mut g2, Duration::from_secs(10)).await;
        assert_eq!(&g2, b"two");

        drop(s2);
        drop(c2);
        listener.close();
    });
}

/// `connect_timeout` succeeds when a live, conv-compatible listener responds to
/// the forced `WASK` probe with `WINS` (first-packet reachability check).
#[test]
fn connect_timeout_live_listener_succeeds() {
    kio::block_on(async {
        let listener = KcpListener::bind("127.0.0.1:0")
            .conv(CONV)
            .mode(KcpMode::Fast3)
            .build()
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();

        let client = KcpConn::connect(addr)
            .conv(CONV)
            .mode(KcpMode::Fast3)
            .connect_timeout(Duration::from_secs(5))
            .build()
            .await
            .expect("live listener should answer the probe within the timeout");

        // The probe-triggered session should be accepted by the listener.
        let (_server, _peer) = listener.accept().await.unwrap();
        client.close();
        listener.close();
    });
}

/// `connect_timeout` fails with `TimedOut` (after roughly the full timeout)
/// when the peer socket is alive but never answers — the probe is
/// retransmitted until the deadline, so there is no fast failure.
#[test]
fn connect_timeout_unresponsive_peer_times_out() {
    kio::block_on(async {
        // A bound-but-silent peer: packets are accepted by the kernel, nothing
        // ever replies (and no ICMP port-unreachable is generated).
        let silent = kio::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let addr = silent.local_addr().unwrap();

        let start = std::time::Instant::now();
        let err = match KcpConn::connect(addr)
            .conv(CONV)
            .connect_timeout(Duration::from_millis(300))
            .build()
            .await
        {
            Ok(_) => panic!("connect to an unresponsive peer should time out"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
        assert!(
            start.elapsed() >= Duration::from_millis(280),
            "should wait roughly the full timeout before failing"
        );
        drop(silent);
    });
}

/// A closed port answers with an ICMP port-unreachable, which closes the
/// connection: `connect_timeout` fails *before* its deadline instead of
/// waiting it out. This is the signal a client redial relies on after a
/// server restart.
#[test]
fn connect_to_closed_port_fails_before_timeout() {
    kio::block_on(async {
        // Grab an ephemeral port then release it: nothing listens there.
        let probe = kio::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let dead = probe.local_addr().unwrap();
        drop(probe);

        let start = std::time::Instant::now();
        let err = match KcpConn::connect(dead)
            .conv(CONV)
            .connect_timeout(Duration::from_secs(2))
            .build()
            .await
        {
            Ok(_) => panic!("connect to a closed port should fail"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), std::io::ErrorKind::NotConnected);
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "closed port must fail fast, not wait for the connect deadline"
        );
    });
}

/// `KcpListener::bind(addr).await` works without an explicit `.build()`.
#[test]
fn listener_bind_into_future() {
    kio::block_on(async {
        let listener = KcpListener::bind("127.0.0.1:0")
            .conv(CONV)
            .await
            .expect("bind via IntoFuture");
        assert!(listener.local_addr().unwrap().port() != 0);
        listener.close();
    });
}

/// `KcpConn::connect(addr).await` works without an explicit `.build()`.
#[test]
fn kcpconn_connect_into_future() {
    kio::block_on(async {
        let listener = KcpListener::bind("127.0.0.1:0").conv(CONV).await.unwrap();
        let addr = listener.local_addr().unwrap();

        let mut client = KcpConn::connect(addr).conv(CONV).await.unwrap();
        client.write_all(b"hi").await.unwrap();
        let (_server, _peer) = listener.accept().await.unwrap();
        client.close();
        listener.close();
    });
}

/// `accept_timeout` fails with `TimedOut` when no client connects in time.
#[test]
fn listener_accept_timeout() {
    kio::block_on(async {
        let listener = KcpListener::bind("127.0.0.1:0").conv(CONV).await.unwrap();
        let err = match listener.accept_timeout(Duration::from_millis(100)).await {
            Ok(_) => panic!("expected accept timeout"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
        listener.close();
    });
}

/// `try_accept` returns `None` when nothing is pending, then `Some` once a
/// client's first datagram registers a peer session (non-blocking poll).
#[test]
fn listener_try_accept() {
    kio::block_on(async {
        let listener = KcpListener::bind("127.0.0.1:0").conv(CONV).await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Nothing connected yet → None.
        assert!(listener.try_accept().unwrap().is_none());

        // Dial + write → the demux reader registers a peer session.
        let mut client = KcpConn::connect(addr).conv(CONV).await.unwrap();
        client.write_all(b"hi").await.unwrap();

        // Poll until the accepted conn is pending (reader is async).
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if let Some((_server, peer)) = listener.try_accept().unwrap() {
                assert_eq!(peer, client.local_addr().unwrap());
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!("timed out waiting for try_accept");
            }
            kio::sleep_ms(10).await;
        }
        client.close();
        listener.close();
    });
}

/// `take_error` starts empty.
#[test]
fn listener_take_error_initial_none() {
    kio::block_on(async {
        let listener = KcpListener::bind("127.0.0.1:0").conv(CONV).await.unwrap();
        assert!(listener.take_error().unwrap().is_none());
        listener.close();
    });
}
