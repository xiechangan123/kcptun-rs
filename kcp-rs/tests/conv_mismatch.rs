//! A session that receives a peer's traffic for a *different* conversation must
//! report it, so the listener can evict the stale session instead of swallowing
//! a re-dialed peer that reused its source address.
//!
//! Symptom this guards against (measured on the live path): the client re-dials
//! from the same UDP source port ~1-2s after a close, the server still holds the
//! old session for that address, and every datagram of the new conversation is
//! dropped inside the stale KCP — so the client sees a link that is up but
//! answers nothing and closes it 30s later as "keepalive timeout", with no
//! packet loss anywhere on the path.
//!
//! ```text
//! cargo test -p kcp-rs --features async --test conv_mismatch
//! ```

#![cfg(feature = "async")]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use kcp_rs::{KcpMode, KcpStream, PacketTransport};
use knet::AsyncWriteExt;

const CONV_SERVER: u32 = 0x00C0_FFEE;
const CONV_CLIENT: u32 = 0x00C0_FFAA;

#[test]
fn conv_mismatch_is_reported_on_the_session_that_rejected_it() {
    knet::block_on(async {
        let a_tmp = knet::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let b_tmp = knet::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let addr_a = a_tmp.local_addr().unwrap();
        let addr_b = b_tmp.local_addr().unwrap();
        drop(a_tmp);
        drop(b_tmp);

        let sock_a = knet::UdpSocket::connect(addr_a, addr_b).unwrap();
        let sock_b = knet::UdpSocket::connect(addr_b, addr_a).unwrap();

        // The "server-side" session, already bound to its conversation.
        let server = KcpStream::with_transport(
            Arc::new(knet::DatagramSocket::Udp(sock_a)) as Arc<dyn PacketTransport>,
            addr_b,
        )
        .connected(true)
        .conv(CONV_SERVER)
        .mode(KcpMode::Fast3)
        .mtu(1350)
        .sndwnd(512)
        .rcvwnd(512)
        .build()
        .await
        .unwrap();

        // A peer that re-dialed from the same address with a new conversation.
        let mut client = KcpStream::with_transport(
            Arc::new(knet::DatagramSocket::Udp(sock_b)) as Arc<dyn PacketTransport>,
            addr_a,
        )
        .connected(true)
        .conv(CONV_CLIENT)
        .mode(KcpMode::Fast3)
        .mtu(1350)
        .sndwnd(512)
        .rcvwnd(512)
        .build()
        .await
        .unwrap();

        assert_eq!(server.conv_mismatch_count(), 0, "nothing mismatched yet");

        client.write_all(b"new conversation").await.unwrap();
        client.flush().await.unwrap();

        // The stale session must report the rejected datagrams (the listener
        // evicts on this signal) rather than silently absorbing them.
        let deadline = Instant::now() + Duration::from_secs(10);
        while server.conv_mismatch_count() == 0 {
            assert!(
                Instant::now() < deadline,
                "stale session never reported the conversation mismatch — the \
                 listener cannot tell a re-dialed peer from silence"
            );
            knet::sleep_ms(50).await;
        }

        // And it must not have delivered the other conversation's payload.
        let mut buf = [0u8; 64];
        match knet::timeout(Duration::from_millis(500), server.read(&mut buf)).await {
            Err(_) | Ok(Ok(0)) | Ok(Err(_)) => {}
            Ok(Ok(n)) => {
                panic!("payload from a foreign conversation leaked into this session ({n} bytes)")
            }
        }
    });
}

/// Same conv, same address, new generation: after a re-dial the peer's KCP
/// restarts its sequence space at 0 while this session is already past it.
/// KCP would treat those segments as ancient duplicates and answer only ACKs,
/// leaving the peer with a link that never delivers its frames — so the session
/// has to report the restart instead of absorbing it.
#[test]
fn sequence_restart_is_reported_on_the_session_it_replaced() {
    knet::block_on(async {
        let a_tmp = knet::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let b_tmp = knet::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let addr_a = a_tmp.local_addr().unwrap();
        let addr_b = b_tmp.local_addr().unwrap();
        drop(a_tmp);
        drop(b_tmp);

        let sock_a = knet::UdpSocket::connect(addr_a, addr_b).unwrap();

        let server = KcpStream::with_transport(
            Arc::new(knet::DatagramSocket::Udp(sock_a)) as Arc<dyn PacketTransport>,
            addr_b,
        )
        .connected(true)
        .conv(CONV_SERVER)
        .mode(KcpMode::Fast3)
        .mtu(1350)
        .sndwnd(512)
        .rcvwnd(512)
        .build()
        .await
        .unwrap();

        // Same conv on both sides: the conversation ID cannot tell the
        // generations apart, only the sequence number can. The re-dial reuses
        // the same source port, exactly like the live client does.
        let mut first = KcpStream::with_transport(
            Arc::new(knet::DatagramSocket::Udp(
                knet::UdpSocket::connect(addr_b, addr_a).unwrap(),
            )) as Arc<dyn PacketTransport>,
            addr_a,
        )
        .connected(true)
        .conv(CONV_SERVER)
        .mode(KcpMode::Fast3)
        .mtu(1350)
        .sndwnd(512)
        .rcvwnd(512)
        .build()
        .await
        .unwrap();
        // Enough traffic to move the server's `rcv_nxt` well past the floor the
        // restart signal requires (a handful of segments is indistinguishable
        // from a brand-new session racing its own first datagrams).
        let first_payload = vec![b'a'; 64 * 1024];
        first.write_all(&first_payload).await.unwrap();
        first.flush().await.unwrap();

        // Wait until the server's session is past sequence zero.
        let deadline = Instant::now() + Duration::from_secs(10);
        while server.rcv_nxt() == 0 && Instant::now() < deadline {
            knet::sleep_ms(50).await;
        }
        assert_eq!(
            server.peer_restart_count(),
            0,
            "the first generation must not look like a restart"
        );
        assert!(
            server.rcv_nxt() >= 16,
            "precondition: generation one must have moved the session past the restart floor (rcv_nxt={})",
            server.rcv_nxt()
        );

        // The peer re-dials from the same source port with a fresh KCP: its
        // first segment is numbered 0 again.
        drop(first);
        // The re-dial rebinds the same source address; the previous socket is
        // released as its tasks wind down, so give the OS a moment.
        let rebind = {
            let mut sock = Err(std::io::Error::from(std::io::ErrorKind::AddrInUse));
            for _ in 0..40 {
                match knet::UdpSocket::connect(addr_b, addr_a) {
                    Ok(s) => {
                        sock = Ok(s);
                        break;
                    }
                    Err(e) => {
                        sock = Err(e);
                        knet::sleep_ms(50).await;
                    }
                }
            }
            sock
        };
        let mut second = KcpStream::with_transport(
            Arc::new(knet::DatagramSocket::Udp(rebind.unwrap())) as Arc<dyn PacketTransport>,
            addr_a,
        )
        .connected(true)
        .conv(CONV_SERVER)
        .mode(KcpMode::Fast3)
        .mtu(1350)
        .sndwnd(512)
        .rcvwnd(512)
        .build()
        .await
        .unwrap();
        second.write_all(b"generation two").await.unwrap();
        second.flush().await.unwrap();

        let deadline = Instant::now() + Duration::from_secs(10);
        while server.peer_restart_count() == 0 {
            assert!(
                Instant::now() < deadline || server.rcv_nxt() == 0,
                "sequence restart went unnoticed — the listener would keep the \
                 stale session and swallow the new conversation"
            );
            knet::sleep_ms(50).await;
        }
    });
}
