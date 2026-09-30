//! Listener path with a `transport_wrapper` that really encrypts.
//!
//! These tests exist to catch the double-decrypt bug: the listener's admission
//! gate must verify a *copy* of the first datagram and queue the original
//! ciphertext, because the session input loop decrypts again through the same
//! wrapper. Pushing the gate's plaintext into the queue drops the handshake
//! burst (and breaks `connect_timeout`).
//!
//! ```text
//! cargo test -p kcp-rs --features async --test listener_crypto_gate
//! ```

#![cfg(feature = "async")]

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kcp_rs::{KcpListener, KcpMode, KcpStream, PacketTransport};

const CONV: u32 = 0x00C0_FFEE;
const MAGIC: u8 = 0xA5;
const KEY: u8 = 0x5A;

/// Toy "cipher": wire = `[MAGIC][payload ^ KEY]`. `decrypt_packet_in_place`
/// returns 0 when the magic byte is wrong — same contract as `CryptoTransport`
/// (CRC / AEAD failure).
struct XorWrap {
    inner: Arc<dyn PacketTransport>,
}

impl XorWrap {
    fn seal(payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(payload.len() + 1);
        out.push(MAGIC);
        out.extend(payload.iter().map(|b| b ^ KEY));
        out
    }

    fn open(buf: &mut [u8], n: usize) -> usize {
        if n < 1 || buf[0] != MAGIC {
            return 0;
        }
        for b in &mut buf[1..n] {
            *b ^= KEY;
        }
        buf.copy_within(1..n, 0);
        n - 1
    }
}

#[async_trait::async_trait]
impl PacketTransport for XorWrap {
    async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let n = self.inner.recv(buf).await?;
            let plain = Self::open(buf, n);
            if plain > 0 || n == 0 {
                return Ok(plain);
            }
            // bad magic — skip, like CryptoTransport
        }
    }

    fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let n = self.inner.try_recv(buf)?;
            let plain = Self::open(buf, n);
            if plain > 0 || n == 0 {
                return Ok(plain);
            }
        }
    }

    fn decrypt_packet_in_place(&self, buf: &mut [u8], n: usize) -> usize {
        Self::open(buf, n)
    }

    async fn send_batch(&self, packets: &[Bytes]) -> io::Result<()> {
        let sealed: Vec<Bytes> = packets.iter().map(|p| Bytes::from(Self::seal(p))).collect();
        self.inner.send_batch(&sealed).await
    }

    async fn send_batch_to(&self, packets: &[Bytes], target: SocketAddr) -> io::Result<()> {
        let sealed: Vec<Bytes> = packets.iter().map(|p| Bytes::from(Self::seal(p))).collect();
        self.inner.send_batch_to(&sealed, target).await
    }

    fn try_send_batch(&self, packets: &[Bytes]) -> io::Result<usize> {
        let sealed: Vec<Bytes> = packets.iter().map(|p| Bytes::from(Self::seal(p))).collect();
        self.inner.try_send_batch(&sealed)
    }

    fn try_send_batch_to(&self, packets: &[Bytes], target: SocketAddr) -> io::Result<usize> {
        let sealed: Vec<Bytes> = packets.iter().map(|p| Bytes::from(Self::seal(p))).collect();
        self.inner.try_send_batch_to(&sealed, target)
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}

fn wrap(transport: Arc<dyn PacketTransport>) -> Arc<dyn PacketTransport> {
    Arc::new(XorWrap { inner: transport })
}

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

/// The admission gate must not consume the decrypt: the first datagram has to
/// reach KCP as ciphertext so the input loop's single decrypt sees it. A
/// `connect_timeout` round-trip fails immediately if that handshake packet is
/// dropped.
#[test]
fn encrypted_listener_accept_echo() {
    knet::block_on(async {
        let listener = KcpListener::bind("127.0.0.1:0")
            .conv(CONV)
            .mode(KcpMode::Fast3)
            .transport_wrapper(|t, _| wrap(t))
            .build()
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();

        let sock = knet::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let mut client =
            KcpStream::with_transport(wrap(Arc::new(knet::DatagramSocket::Udp(sock))), addr)
                .connected(false)
                .conv(CONV)
                .mode(KcpMode::Fast3)
                .connect_timeout(Duration::from_secs(3))
                .build()
                .await
                .expect("encrypted connect_timeout: handshake datagram was dropped");

        let (mut server, _peer) = knet::timeout(Duration::from_secs(3), listener.accept())
            .await
            .expect("accept timed out")
            .unwrap();

        let payload = b"hello-encrypted-listener";
        client.write_all(payload).await.unwrap();
        let mut got = vec![0u8; payload.len()];
        read_exact_timeout(&mut server, &mut got, Duration::from_secs(3)).await;
        assert_eq!(&got, payload);

        let reply = b"reply-from-server";
        server.write_all(reply).await.unwrap();
        let mut got = vec![0u8; reply.len()];
        read_exact_timeout(&mut client, &mut got, Duration::from_secs(3)).await;
        assert_eq!(&got, reply);

        listener.close();
    });
}

/// A datagram that fails the wrapper's integrity check must not create a
/// session (no `accept`).
#[test]
fn encrypted_listener_rejects_bad_magic() {
    knet::block_on(async {
        let listener = KcpListener::bind("127.0.0.1:0")
            .conv(CONV)
            .transport_wrapper(|t, _| wrap(t))
            .build()
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();

        let sock = knet::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        // Not sealed: no MAGIC prefix → admission gate must refuse.
        let _ = sock.send_to(b"not-a-sealed-datagram", addr).await.unwrap();

        let accepted = knet::timeout(Duration::from_millis(400), listener.accept()).await;
        assert!(
            accepted.is_err() || matches!(accepted, Ok(Err(_))),
            "unauthenticated datagram must not be accepted"
        );
        assert_eq!(listener.session_count(), 0);
        assert!(listener.stats().unauthenticated_drops >= 1);

        listener.close();
    });
}

/// Two sealed clients must demux to two accepted sessions and both echo.
#[test]
fn encrypted_listener_multiple_peers() {
    knet::block_on(async {
        let listener = KcpListener::bind("127.0.0.1:0")
            .conv(CONV)
            .mode(KcpMode::Fast3)
            .transport_wrapper(|t, _| wrap(t))
            .build()
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();

        let sock_a = knet::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let client_a =
            KcpStream::with_transport(wrap(Arc::new(knet::DatagramSocket::Udp(sock_a))), addr)
                .connected(false)
                .conv(CONV)
                .mode(KcpMode::Fast3)
                .connect_timeout(Duration::from_secs(3))
                .build()
                .await
                .unwrap();
        let sock_b = knet::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let client_b =
            KcpStream::with_transport(wrap(Arc::new(knet::DatagramSocket::Udp(sock_b))), addr)
                .connected(false)
                .conv(CONV)
                .mode(KcpMode::Fast3)
                .connect_timeout(Duration::from_secs(3))
                .build()
                .await
                .unwrap();

        let (mut server_a, _) = knet::timeout(Duration::from_secs(3), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let (mut server_b, _) = knet::timeout(Duration::from_secs(3), listener.accept())
            .await
            .unwrap()
            .unwrap();

        client_a.write_all(b"AAA").await.unwrap();
        client_b.write_all(b"BBB").await.unwrap();
        let mut ga = [0u8; 3];
        let mut gb = [0u8; 3];
        read_exact_timeout(&mut server_a, &mut ga, Duration::from_secs(3)).await;
        read_exact_timeout(&mut server_b, &mut gb, Duration::from_secs(3)).await;
        assert_eq!(&ga, b"AAA");
        assert_eq!(&gb, b"BBB");

        listener.close();
    });
}
