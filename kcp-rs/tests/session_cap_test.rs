//! Regression: `max_sessions_per_worker` must bound the **sum** of published
//! sessions and in-flight builds.
//!
//! With `--crypt none`/`null` the pre-admission integrity gate is a no-op, so a
//! spoofed source-address flood used to pile up unbounded `building` entries
//! (one spawn task + PeerQueue each) while every published session was still
//! under the limit. P0-1 / H-1.

#![cfg(feature = "async")]

use std::net::UdpSocket;
use std::time::Duration;

use kcp_rs::{KcpListener, KcpMode, WorkerPoolLimits};

/// Small cap so the test stays fast; the invariant is scale-free.
const CAP: usize = 8;
/// Many more distinct source ports than the cap.
const ATTACKERS: usize = 64;
/// Hold finished builds in the `building` table long enough for the flood to
/// pile up. Without the fix the sum reaches `ATTACKERS`.
const BUILD_HOLD_MS: u64 = 400;

#[test]
fn building_and_sessions_never_exceed_max_sessions_per_worker() {
    knet::block_on(async {
        let listener = KcpListener::bind("127.0.0.1:0")
            .conv(0x00C0_FFEE)
            .mode(KcpMode::Fast3)
            .limits(WorkerPoolLimits {
                max_sessions_per_worker: CAP,
                // Keep the reaper out of the way for the sampling window.
                building_timeout: Duration::from_secs(30),
                idle_timeout: Duration::from_secs(30),
                ..Default::default()
            })
            .testing_build_delay(Duration::from_millis(BUILD_HOLD_MS))
            .build()
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();

        // Distinct ephemeral source ports — one "spoofed" peer each.
        let mut socks = Vec::with_capacity(ATTACKERS);
        for _ in 0..ATTACKERS {
            let s = UdpSocket::bind("127.0.0.1:0").expect("bind attacker socket");
            s.set_nonblocking(true).ok();
            socks.push(s);
        }

        let mut peak_sum = 0usize;
        for s in &socks {
            // Any payload is admitted: the integrity gate is a no-op with no
            // crypto configured. Use a 24-byte KCP-shaped header so later
            // parsing does not panic (cmd=2 PSH, len=0).
            let mut pkt = [0u8; 24];
            pkt[4] = 2; // cmd = PSH
            let _ = s.send_to(&pkt, addr);

            let st = listener.stats();
            let sum = st.sessions + st.building;
            if sum > peak_sum {
                peak_sum = sum;
            }
            assert!(
                sum <= CAP,
                "sessions({}) + building({}) exceeded cap {} (peak_sum={})",
                st.sessions,
                st.building,
                CAP,
                peak_sum
            );
        }

        // Keep sampling until the hold expires and everything publishes or is
        // dropped. The invariant must hold the whole way through.
        let deadline = std::time::Instant::now() + Duration::from_millis(BUILD_HOLD_MS + 800);
        while std::time::Instant::now() < deadline {
            let st = listener.stats();
            let sum = st.sessions + st.building;
            if sum > peak_sum {
                peak_sum = sum;
            }
            assert!(
                sum <= CAP,
                "post-flood sessions({}) + building({}) exceeded cap {}",
                st.sessions,
                st.building,
                CAP
            );
            if st.sessions + st.building == 0 && st.session_drops > 0 {
                break;
            }
            knet::sleep_ms(5).await;
        }

        let st = listener.stats();
        assert!(
            st.session_drops > 0,
            "expected some admissions to be refused at the cap, stats={st:?}"
        );
        assert!(
            st.sessions <= CAP,
            "settled sessions {} exceeds cap {}",
            st.sessions,
            CAP
        );
        assert!(
            peak_sum <= CAP,
            "peak sessions+building {peak_sum} exceeded cap {CAP}"
        );

        listener.close();
    });
}
