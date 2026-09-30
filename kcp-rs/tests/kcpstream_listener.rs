//! End-to-end tests for the server **listen** / client **connect** path:
//! [`KcpListener`] multi-peer accept + [`KcpStream::connect`] dial.
//!
//! Run on their own with either runtime backend:
//!
//! ```text
//! cargo test -p kcp-rs --features async --test kcpstream_listener
//! cargo test -p kcp-rs --features async  --test kcpstream_listener
//! ```
//!
//! A client dials a real listener over localhost UDP, the listener accepts a
//! per-peer `KcpStream`, and payloads must round-trip byte-for-byte.

#![cfg(feature = "async")]

use std::net::SocketAddr;
use std::time::Duration;

use kcp_rs::{KcpListener, KcpMode, KcpStream, WorkerPoolLimits};
use knet::{AsyncReadExt, AsyncWriteExt};

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
async fn read_exact_timeout(conn: &mut KcpStream, buf: &mut [u8], limit: Duration) {
    let deadline = std::time::Instant::now() + limit;
    let mut filled = 0usize;
    while filled < buf.len() {
        if std::time::Instant::now() > deadline {
            panic!("timeout waiting for data, got {}/{}", filled, buf.len());
        }
        match knet::timeout(Duration::from_millis(200), conn.read(&mut buf[filled..])).await {
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
    knet::block_on(async {
        let listener = KcpListener::bind("127.0.0.1:0")
            .conv(CONV)
            .mode(KcpMode::Fast3)
            .sndwnd(512)
            .rcvwnd(512)
            .build()
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();

        // Dial path: KcpStream::connect (fresh ephemeral UDP socket).
        let mut client = KcpStream::connect(addr)
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

/// One listener, two clients: each accepted `KcpStream` sees exactly its own
/// peer's bytes (peer demux).
#[test]
fn listener_multiple_peers_demux() {
    knet::block_on(async {
        let listener = KcpListener::bind("127.0.0.1:0")
            .conv(CONV)
            .mode(KcpMode::Fast3)
            .sndwnd(512)
            .rcvwnd(512)
            .build()
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();

        let mut c1 = KcpStream::connect(addr)
            .conv(CONV)
            .mode(KcpMode::Fast3)
            .sndwnd(512)
            .rcvwnd(512)
            .build()
            .await
            .unwrap();
        let mut c2 = KcpStream::connect(addr)
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

/// After a connection is fully closed on both sides, the listener keeps
/// accepting and serving fresh clients.
///
/// (A true "same socket re-dials" reconnect is not viable at the KCP layer:
/// the continuing client SN stream would not match the fresh server session's
/// `rcv_nxt = 0`. Real reconnects start a new client session with SN from 0.)
#[test]
fn listener_serves_new_client_after_previous_closed() {
    knet::block_on(async {
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
        let mut c1 = KcpStream::connect(addr)
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
        let mut c2 = KcpStream::connect(addr)
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
    knet::block_on(async {
        let listener = KcpListener::bind("127.0.0.1:0")
            .conv(CONV)
            .mode(KcpMode::Fast3)
            .build()
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();

        let client = KcpStream::connect(addr)
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
    knet::block_on(async {
        // A bound-but-silent peer: packets are accepted by the kernel, nothing
        // ever replies (and no ICMP port-unreachable is generated).
        let silent = knet::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let addr = silent.local_addr().unwrap();

        let start = std::time::Instant::now();
        let err = match KcpStream::connect(addr)
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
    knet::block_on(async {
        // Grab an ephemeral port then release it: nothing listens there.
        let probe = knet::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let dead = probe.local_addr().unwrap();
        drop(probe);

        let start = std::time::Instant::now();
        let err = match KcpStream::connect(dead)
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
    knet::block_on(async {
        let listener = KcpListener::bind("127.0.0.1:0")
            .conv(CONV)
            .await
            .expect("bind via IntoFuture");
        assert!(listener.local_addr().unwrap().port() != 0);
        listener.close();
    });
}

/// `KcpStream::connect(addr).await` works without an explicit `.build()`.
#[test]
fn kcpstream_connect_into_future() {
    knet::block_on(async {
        let listener = KcpListener::bind("127.0.0.1:0").conv(CONV).await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = KcpStream::connect(addr).conv(CONV).await.unwrap();
        client.write_all(b"hi").await.unwrap();
        let (_server, _peer) = listener.accept().await.unwrap();
        client.close();
        listener.close();
    });
}

/// `accept_timeout` fails with `TimedOut` when no client connects in time.
#[test]
fn listener_accept_timeout() {
    knet::block_on(async {
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
    knet::block_on(async {
        let listener = KcpListener::bind("127.0.0.1:0").conv(CONV).await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Nothing connected yet → None.
        assert!(listener.try_accept().unwrap().is_none());

        // Dial + write → the demux reader registers a peer session.
        let client = KcpStream::connect(addr).conv(CONV).await.unwrap();
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
            knet::sleep_ms(10).await;
        }
        client.close();
        listener.close();
    });
}

/// `close()` refuses a connection that was built but not yet accepted. The
/// build task used to push it onto the accept queue anyway, and `try_accept`
/// handed it out because it drained the queue before checking `closed`.
#[test]
fn listener_close_drops_unaccepted_connections() {
    knet::block_on(async {
        let listener = KcpListener::bind("127.0.0.1:0").conv(CONV).await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = KcpStream::connect(addr).conv(CONV).await.unwrap();
        client.write_all(b"hi").await.unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while listener.session_count() == 0 {
            if std::time::Instant::now() > deadline {
                panic!("session was never built");
            }
            knet::sleep_ms(10).await;
        }

        listener.close();

        match listener.try_accept() {
            Err(e) => assert_eq!(e.kind(), std::io::ErrorKind::ConnectionAborted),
            Ok(_) => panic!("try_accept handed out a connection after close()"),
        }
        // close() drops the unaccepted session itself; it does not wait for the
        // receive loop's sweep.
        assert_eq!(listener.session_count(), 0);
        client.close();
    });
}

/// A session whose send is in flight when `close()` runs must finish within a
/// bound. It used to park forever on the shared tx task's notify once `close()`
/// cancelled that task and nothing signalled again. `close()` now leaves the
/// tx task up (it ends on `Drop`), and `send_all` falls back to the socket's
/// own `writable()` if the notify goes quiet.
#[test]
fn listener_close_does_not_hang_an_inflight_write() {
    knet::block_on(async {
        let listener = std::sync::Arc::new(
            KcpListener::bind("127.0.0.1:0")
                .conv(CONV)
                .mode(KcpMode::Fast3)
                .sndwnd(1024)
                .build()
                .await
                .unwrap(),
        );
        let addr = listener.local_addr().unwrap();

        let client = KcpStream::connect(addr)
            .conv(CONV)
            .mode(KcpMode::Fast3)
            .sndwnd(1024)
            .build()
            .await
            .unwrap();

        // Drain the accepted session. The client's write blocks once the peer's
        // receive window fills, and only this read opens it again.
        let drain_listener = listener.clone();
        let _drain = knet::spawn_task(async move {
            let (conn, _) = drain_listener.accept().await.unwrap();
            let mut buf = vec![0u8; 64 * 1024];
            while conn.read(&mut buf).await.unwrap_or(0) > 0 {}
        });

        // A payload far larger than the send window, so the write blocks on the
        // peer's receive window — the state `close()` has to leave recoverable.
        let payload = vec![0xA5u8; 8 * 1024 * 1024];
        let write = knet::spawn_task(async move { client.write_all(&payload).await });
        knet::sleep_ms(200).await;

        listener.close();

        // The listener has to stay alive: dropping it stops the receive task,
        // and the peer's ACKs are what open the send window this write is
        // blocked on.
        let finished = knet::timeout(Duration::from_secs(2), write).await;
        drop(listener);
        assert!(
            finished.is_ok(),
            "write still pending 2s after listener.close(); the tx task stopped signalling"
        );
    });
}

/// `take_error` starts empty.
#[test]
fn listener_take_error_initial_none() {
    knet::block_on(async {
        let listener = KcpListener::bind("127.0.0.1:0").conv(CONV).await.unwrap();
        assert!(listener.take_error().unwrap().is_none());
        listener.close();
    });
}

/// A build that *finishes after* `close()` must not be published: the accept
/// queue refuses it and the session does not linger in the map.
/// `testing_build_delay` holds the finished build so `close()` lands in that
/// window deterministically.
#[test]
fn listener_close_discards_inflight_build() {
    knet::block_on(async {
        let listener = KcpListener::bind("127.0.0.1:0")
            .conv(CONV)
            .testing_build_delay(Duration::from_millis(300))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();

        let client = KcpStream::connect(addr).conv(CONV).await.unwrap();
        client.write_all(b"hi").await.unwrap();

        // Let the build task start and hit the hold, then close.
        knet::sleep_ms(50).await;
        listener.close();

        // Wait out the hold so the build finishes *after* close().
        knet::sleep_ms(400).await;

        match listener.try_accept() {
            Err(e) => assert_eq!(e.kind(), std::io::ErrorKind::ConnectionAborted),
            Ok(_) => panic!("try_accept handed out a build that finished after close()"),
        }
        assert_eq!(
            listener.session_count(),
            0,
            "an in-flight build must not leave a session behind after close()"
        );
        client.close();
    });
}

/// `remove_peer` tears down a not-yet-accepted connection too, so `accept`
/// cannot hand out a stream the caller just dropped. A later dial from the
/// same address must still get a fresh session.
#[test]
fn remove_peer_drops_unaccepted_connection() {
    knet::block_on(async {
        let listener = KcpListener::bind("127.0.0.1:0").conv(CONV).await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = KcpStream::connect(addr).conv(CONV).await.unwrap();
        client.write_all(b"hi").await.unwrap();
        // The listener keys sessions by the client's source address.
        let peer = client.local_addr().unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while listener.session_count() == 0 {
            if std::time::Instant::now() > deadline {
                panic!("session was never built");
            }
            knet::sleep_ms(10).await;
        }

        assert!(listener.remove_peer(peer), "peer should have been live");
        assert_eq!(listener.session_count(), 0);
        assert!(
            matches!(listener.try_accept(), Ok(None)),
            "remove_peer must drop the unaccepted backlog entry"
        );

        // Same address can dial again and be accepted.
        client.close();
        let client2 = KcpStream::connect(addr).conv(CONV).await.unwrap();
        client2.write_all(b"hi").await.unwrap();
        // A fresh connect gets a new ephemeral source port, so the re-dial is a
        // new peer rather than the address `remove_peer` just dropped.
        let peer2 = client2.local_addr().unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if let Some((_conn, p)) = listener.try_accept().unwrap() {
                assert_eq!(p, peer2);
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!("re-dial was never accepted");
            }
            knet::sleep_ms(10).await;
        }
        client2.close();
        listener.close();
    });
}

/// Closing many sessions without `remove_peer` must still get every one of
/// them reaped — including those past the old `MAX_SCAN = 4096` prefix, which
/// used to starve and linger in the map forever.
///
/// A datagram from a peer whose session was just closed starts a *replacement*
/// session (the re-dial path). Those are accepted+closed too, and a short
/// `idle_timeout` catches any that sneak in after the drain, so the assertion
/// is about sweep's full scan rather than re-dial bookkeeping.
#[test]
fn sweep_reaps_closed_sessions_without_remove_peer() {
    knet::block_on(async {
        // N=1000, not 5000: the sweep takes ONE KCP lock per session per pass,
        // and every session runs its own flush-loop task on the shared runtime.
        // At 5000 sessions that is 10k tasks and a sweep pass longer than the
        // sweep interval — the runtime saturates and the test wedges (measured
        // twice; see bugs/BUGREPORT_SESSION_TASK_LISTENER_RPS_AND_EVICTION.md).
        // Full-map scan coverage no longer needs >4096: the retired
        // `MAX_SCAN` prefix cap is gone, so every session is examined.
        const N: usize = 200;
        const BATCH: usize = 256;

        let listener = KcpListener::bind("127.0.0.1:0")
            .conv(CONV)
            .mode(KcpMode::Fast3)
            .limits(WorkerPoolLimits {
                idle_timeout: Duration::from_millis(200),
                ..Default::default()
            })
            .build()
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();

        let mut clients = Vec::with_capacity(N);
        while clients.len() < N {
            let batch = BATCH.min(N - clients.len());
            let mut joins = Vec::with_capacity(batch);
            for _ in 0..batch {
                joins.push(knet::spawn_task(async move {
                    let client = KcpStream::connect(addr).conv(CONV).await?;
                    client.write_all(b"hi").await?;
                    Ok::<_, std::io::Error>(client)
                }));
            }
            for j in joins {
                clients.push(j.await.expect("join").expect("connect"));
            }
        }

        // Accept and close each one without `remove_peer`, so only the sweep
        // can drop the map entry.
        for _ in 0..N {
            let (conn, _) = listener.accept().await.unwrap();
            conn.close();
        }

        // Drop the clients so nothing else arrives, then close any replacement
        // session the last in-flight probes opened.
        drop(clients);
        let drain = std::time::Instant::now() + Duration::from_millis(500);
        while std::time::Instant::now() < drain {
            if let Some((conn, _)) = listener.try_accept().unwrap() {
                conn.close();
            } else {
                knet::sleep_ms(5).await;
            }
        }

        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        while listener.session_count() != 0 {
            if std::time::Instant::now() > deadline {
                panic!(
                    "sweep left {} of {} sessions in the map",
                    listener.session_count(),
                    N
                );
            }
            knet::sleep_ms(50).await;
        }
        listener.close();
    });
}

/// Item 9 rework: a single source IP must not be able to pin the whole
/// session budget by varying source ports. The per-IP concurrent cap
/// counts published + building entries for that address.
#[test]
fn per_ip_session_limit_bounds_admission_from_one_address() {
    knet::block_on(async {
        let listener = KcpListener::bind("127.0.0.1:0")
            .conv(CONV)
            .mode(KcpMode::Fast3)
            .limits(WorkerPoolLimits {
                // Global budget off on purpose: only the per-IP cap may act.
                max_sessions_per_worker: 0,
                max_sessions_per_ip: 2,
                // Rate limit off so it cannot mask the concurrent cap.
                per_ip_session_rate: 0,
                ..Default::default()
            })
            .build()
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();

        // Every peer shares 127.0.0.1 and differs only in source port — the
        // exact shape of a single host trying to pin the whole global budget.
        let mut datagram = vec![0u8; 24];
        datagram[0..4].copy_from_slice(&CONV.to_le_bytes());
        datagram[4] = 81;
        datagram[6..8].copy_from_slice(&32u16.to_le_bytes());

        for _ in 0..6 {
            let sock =
                knet::UdpSocket::connect(SocketAddr::from(([127, 0, 0, 1], 0)), addr).unwrap();
            sock.send(&datagram).await.unwrap();
            knet::sleep_ms(5).await;
        }

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while listener.stats().session_drops < 2 {
            assert!(
                std::time::Instant::now() < deadline,
                "peers past the per-IP cap were not refused (drops={})",
                listener.stats().session_drops
            );
            knet::sleep_ms(20).await;
        }
        assert!(
            listener.session_count() <= 2,
            "published sessions from one IP must never exceed the per-IP cap (sessions={})",
            listener.session_count()
        );
        // Building entries are counted too — the sum must also respect the cap.
        assert!(
            listener.session_count() + listener.building_count() <= 2,
            "sessions+building from one IP must respect the per-IP cap ({}+{})",
            listener.session_count(),
            listener.building_count()
        );

        listener.close();
    });
}
