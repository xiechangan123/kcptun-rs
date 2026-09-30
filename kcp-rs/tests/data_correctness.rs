//! Standalone data-correctness tests for the KCP state machine.
//!
//! These integration tests use only the crate's **default** features (no
//! async) and can be run on their own:
//!
//! ```text
//! cargo test -p kcp-rs --test data_correctness
//! ```
//!
//! They verify that KCP delivers data **reliably, in order, and byte-for-byte
//! correctly** over an in-memory "flaky" link that drops, duplicates, delays
//! and reorders packets — the exact failure conditions the KCP ARQ was built
//! to survive. A deterministic PRNG (SplitMix64, fixed seeds) keeps every test
//! reproducible.

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use kcp_rs::fec::{fec_kcp_from_recovered, FecDecoder, FecEncoder};
use kcp_rs::{KcpConfig, KCP};

/// Conversation ID shared by both ends of every in-memory link.
const CONV: u32 = 0x00C0_FFEE;
/// Bytes handed to `KCP::send` per loop iteration (well under `KCP_MAX_FRAG`).
const SEND_CHUNK: usize = 16 * 1024;
/// Payload size for each transfer test.
const PAYLOAD_LEN: usize = 128 * 1024;

// ─── Deterministic PRNG (SplitMix64) ─────────────────────────────────────────

struct SplitMix64(u64);

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

// ─── Flaky channel model ─────────────────────────────────────────────────────

/// Packet impairments applied to one direction of an in-memory link.
#[derive(Debug, Clone, Copy)]
struct Impairment {
    /// Percent of packets dropped outright (0–100).
    loss_pct: u64,
    /// Percent of packets duplicated (a delayed second copy is sent).
    dup_pct: u64,
    /// Base one-way delivery delay in ms.
    delay_ms: u64,
    /// Random extra delay in ms; different packets get different jitter, so a
    /// later packet can occasionally overtake an earlier one (reordering).
    jitter_ms: u64,
}

impl Impairment {
    fn none() -> Self {
        Self {
            loss_pct: 0,
            dup_pct: 0,
            delay_ms: 0,
            jitter_ms: 0,
        }
    }
}

/// An in-memory packet queue that applies an [`Impairment`] profile.
struct FlakyChannel {
    imp: Impairment,
    rng: SplitMix64,
    /// `(deliver_at_ms, payload)` — delivered once `now >= deliver_at_ms`.
    pending: Vec<(u64, Vec<u8>)>,
    sent: u64,
    dropped: u64,
    duplicated: u64,
}

impl FlakyChannel {
    fn new(imp: Impairment) -> Self {
        Self {
            imp,
            rng: SplitMix64::new(0x5EED_CAFE),
            pending: Vec::new(),
            sent: 0,
            dropped: 0,
            duplicated: 0,
        }
    }

    /// KCP output callback → queue the raw segment, subject to impairment.
    fn push(&mut self, data: &[u8]) {
        self.sent += 1;
        if self.rng.below(100) < self.imp.loss_pct {
            self.dropped += 1;
            return;
        }
        let now = now_ms() as u64;
        let delay = self.imp.delay_ms + self.rng.below(self.imp.jitter_ms + 1);
        self.pending.push((now + delay, data.to_vec()));
        // Duplicate: a second copy that arrives a little later.
        if self.rng.below(100) < self.imp.dup_pct {
            self.duplicated += 1;
            self.pending.push((now + delay + 2, data.to_vec()));
        }
    }

    /// Pop every packet whose delivery time has arrived (queue order).
    fn deliver(&mut self, now: u64) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let mut i = 0;
        while i < self.pending.len() {
            if self.pending[i].0 <= now {
                out.push(self.pending.remove(i).1);
            } else {
                i += 1;
            }
        }
        out
    }
}

// ─── Transfer driver ─────────────────────────────────────────────────────────

/// Run a full transfer over an in-memory flaky link, returning every byte the
/// receiver reassembled. Panics on timeout or dead-link.
fn run_transfer(payload: &[u8], a2b: Impairment, b2a: Impairment) -> Vec<u8> {
    let chan_a2b = Arc::new(Mutex::new(FlakyChannel::new(a2b)));
    let chan_b2a = Arc::new(Mutex::new(FlakyChannel::new(b2a)));

    let a2b_out = chan_a2b.clone();
    let mut a = KCP::new(CONV, 0, move |data: Bytes| {
        a2b_out.lock().unwrap().push(&data);
    });
    let b2a_out = chan_b2a.clone();
    let mut b = KCP::new(CONV, 0, move |data: Bytes| {
        b2a_out.lock().unwrap().push(&data);
    });

    a.apply(&KcpConfig::default()).unwrap();
    b.apply(&KcpConfig::default()).unwrap();

    let mut sent = 0usize;
    let mut received = Vec::with_capacity(payload.len());
    let deadline = SystemTime::now() + Duration::from_secs(20);

    loop {
        let now = now_ms() as u64;

        // Feed the next chunk into the sender's queue.
        if sent < payload.len() {
            let end = (sent + SEND_CHUNK).min(payload.len());
            a.send(&payload[sent..end]).expect("send should fit window");
            sent = end;
        }

        // Advance both state machines (drives flush + retransmission timers).
        a.update(now as u32);
        b.update(now as u32);
        a.flush();
        b.flush();

        // Deliver in-flight packets (loss / dup / reorder already applied).
        for p in chan_a2b.lock().unwrap().deliver(now) {
            b.input(&p, true).expect("receiver input");
        }
        for p in chan_b2a.lock().unwrap().deliver(now) {
            a.input(&p, true).expect("sender input");
        }

        // Drain everything the receiver has reassembled.
        while let Ok(d) = b.recv_bytes() {
            received.extend_from_slice(&d);
        }

        if sent >= payload.len() && received.len() >= payload.len() {
            break;
        }
        assert!(
            SystemTime::now() < deadline,
            "transfer timed out: sent {sent}/{} recv {}/{}",
            payload.len(),
            received.len(),
            payload.len(),
        );
        assert!(!a.is_dead(), "sender dead-linked");
        assert!(!b.is_dead(), "receiver dead-linked");
        std::thread::sleep(Duration::from_millis(1));
    }

    // Prove the impairment actually fired (guards against a no-op channel), so
    // the tests genuinely exercise loss/duplicate recovery.
    {
        let a2b = chan_a2b.lock().unwrap();
        if a2b.imp.loss_pct > 0 {
            assert!(a2b.dropped > 0, "data channel should have dropped packets");
        }
        if a2b.imp.dup_pct > 0 {
            assert!(
                a2b.duplicated > 0,
                "data channel should have duplicated packets"
            );
        }
        let b2a = chan_b2a.lock().unwrap();
        if b2a.imp.loss_pct > 0 {
            assert!(b2a.dropped > 0, "ack channel should have dropped packets");
        }
    }

    received
}

// ─── Verification helpers ────────────────────────────────────────────────────

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

fn assert_integrity(received: &[u8], payload: &[u8]) {
    assert_eq!(received.len(), payload.len(), "length mismatch");
    assert_eq!(received, payload, "byte-for-byte mismatch");
    assert_eq!(fnv1a(received), fnv1a(payload), "FNV-1a checksum mismatch");
}

fn now_ms() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u32
}

// ─── Tests ───────────────────────────────────────────────────────────────────

/// Perfect link: every byte must round-trip unchanged, in order.
#[test]
fn reliable_delivery_clean_channel() {
    let payload = make_payload(PAYLOAD_LEN, 7);
    let received = run_transfer(&payload, Impairment::none(), Impairment::none());
    assert_integrity(&received, &payload);
}

/// 20% packet loss on the data path (5% on the ACK path): retransmission must
/// recover every lost segment.
#[test]
fn reliable_delivery_with_20pct_loss() {
    let payload = make_payload(PAYLOAD_LEN, 0xA5);
    let data_dir = Impairment {
        loss_pct: 20,
        ..Impairment::none()
    };
    let ack_dir = Impairment {
        loss_pct: 5,
        ..Impairment::none()
    };
    let received = run_transfer(&payload, data_dir, ack_dir);
    assert_integrity(&received, &payload);
}

/// Combined worst case: loss + duplication + jitter-induced reordering + delay
/// in both directions.
#[test]
fn reliable_delivery_loss_reorder_dup_delay() {
    let payload = make_payload(PAYLOAD_LEN, 0x5E);
    let data_dir = Impairment {
        loss_pct: 15,
        dup_pct: 5,
        delay_ms: 3,
        jitter_ms: 8,
    };
    let ack_dir = Impairment {
        loss_pct: 5,
        delay_ms: 2,
        jitter_ms: 4,
        ..Impairment::none()
    };
    let received = run_transfer(&payload, data_dir, ack_dir);
    assert_integrity(&received, &payload);
}

/// Reed-Solomon FEC: drop data shard 0, recover it byte-exactly from the
/// surviving data shards + parity.
#[test]
fn fec_reconstructs_lost_data_packets() {
    let ds = 3usize;
    let ps = 2usize;
    let mut enc = FecEncoder::new(ds, ps, 0).unwrap();
    let mut dec = FecDecoder::new(ds, ps).unwrap();

    // Variable-length payloads so RS pads shorter shards (exercises SIZE trim).
    let payloads: Vec<Vec<u8>> = (0..ds).map(|i| make_payload(37 + i * 7, i as u8)).collect();

    let mut data_frames = Vec::new();
    let mut parity_frames = Vec::new();
    for p in &payloads {
        let (data, parity) = enc.wrap_kcp_packet(p, 1000);
        data_frames.push(data);
        if !parity.is_empty() {
            parity_frames = parity;
        }
    }
    assert_eq!(parity_frames.len(), ps);

    // Shard 0 is "lost" on the wire; only data 1, 2 + both parity arrive.
    for f in &data_frames[1..] {
        assert!(dec.decode(f).is_empty());
    }
    let mut recovered = dec.decode(&parity_frames[0]);
    if recovered.is_empty() {
        recovered = dec.decode(&parity_frames[1]);
    }
    assert_eq!(
        recovered.len(),
        1,
        "expected exactly one recovered data shard"
    );
    let kcp = fec_kcp_from_recovered(&recovered[0]).expect("valid SIZE field");
    assert_eq!(
        kcp,
        payloads[0].as_slice(),
        "recovered data must be byte-exact"
    );
}
