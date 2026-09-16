//! `RepeatSegs` accounting for duplicates.
//!
//! Go's `KCP.Input` keeps `repeat = true` unless `parse_data` rejects the
//! segment, so a duplicate whose `sn` is already below `rcv_nxt` — the normal
//! case for a retransmission that arrives after the original was delivered —
//! is counted. Counting only the in-window branch made the counter blind to
//! exactly the retransmissions it exists to measure, which inverted the
//! conclusion of a real retransmit-storm investigation.

use std::sync::atomic::Ordering;

use kcp_rs::{snmp_enable, DEFAULT_SNMP, KCP};

const CONV: u32 = 0x1122_3344;
const PUSH: u8 = 81;

fn seg(sn: u32, una: u32, ts: u32, payload: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(24 + payload.len());
    b.extend_from_slice(&CONV.to_le_bytes());
    b.push(PUSH);
    b.push(0); // frg
    b.extend_from_slice(&128u16.to_le_bytes()); // wnd
    b.extend_from_slice(&ts.to_le_bytes());
    b.extend_from_slice(&sn.to_le_bytes());
    b.extend_from_slice(&una.to_le_bytes());
    b.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    b.extend_from_slice(payload);
    b
}

#[test]
fn duplicate_below_rcv_nxt_counts_toward_repeat_segs() {
    snmp_enable();
    let mut kcp = KCP::new(CONV, 0, |_pkt| {});
    kcp.set_nodelay(0, 10, 2, 1);
    kcp.set_snd_wnd(128);
    kcp.set_rcv_wnd(128);

    let before = DEFAULT_SNMP.repeat_segs.load(Ordering::Acquire);

    // In-order segment sn=0: delivered, rcv_nxt advances past it.
    kcp.input_no_flush(&seg(0, 1, 1, b"a"), false).unwrap();
    assert_eq!(kcp.peeksize(), Some(1), "sn=0 must be delivered in order");

    // Retransmission of sn=0 arrives after delivery: sn < rcv_nxt. This is the
    // case that must still count as a repeat.
    kcp.input_no_flush(&seg(0, 1, 1, b"a"), false).unwrap();
    let after_dup = DEFAULT_SNMP.repeat_segs.load(Ordering::Acquire);
    assert!(
        after_dup > before,
        "a duplicate below rcv_nxt must count as a repeat: {before} -> {after_dup}"
    );

    // An FEC-reconstructed copy (`regular = false`) must not be counted, as in Go.
    kcp.input_no_flush_typed(&seg(0, 1, 1, b"a"), false, false)
        .unwrap();
    assert_eq!(
        DEFAULT_SNMP.repeat_segs.load(Ordering::Acquire),
        after_dup,
        "recovered (non-regular) duplicates must not be counted"
    );
}
