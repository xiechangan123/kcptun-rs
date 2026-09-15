# Handoff — session liveness / reconnect / autoexpire (2026-09-15)

Audience: the next agent (or human) picking this up. Everything below was
measured or read from source during this session; nothing is assumed.

- **HEAD**: `68008404` on `main`, working tree clean.
- **Commits from this session** (oldest first):
  | commit | what |
  |---|---|
  | `fdfb05be` | fix: client didn't compile; replaced the stream "blackhole" heuristic with ACK-stall detection |
  | `de112783` | fix: transient ICMP no longer kills a session; Go's receive-bucket guard; ACK-stall needs an open peer window |
  | `81d25d15` | fix: replaced sessions are *retired* (served to completion), not killed |
  | `68008404` | test: full-lifecycle retirement test (3 phases, fails on the pre-fix client) |
- **Gates**: `cargo test --workspace` green, `cargo clippy --workspace -- -D warnings` clean, `cargo fmt` applied.

## 1. What the user was hitting, in order of impact

1. **Periodic freezes while browsing (YouTube).** With `--autoexpire 300`, the
   client replaced a pool slot every 5 min and the old session was closed
   immediately, aborting every stream it carried (measured: a 30 MiB download
   stopped at 12.9 MiB). Go hands the replaced session to the scavenger and
   keeps it alive until `creation + autoexpire + scavengeTTL`. Fixed in
   `81d25d15`: retired sessions move onto the scavenger list and are closed
   only once the grace period has passed **and** `active_stream_count() == 0`.
2. **A session killed by a single stray ICMP.** On the real path a high-rate
   transfer surfaces isolated `ConnectionRefused` on the connected UDP socket
   while the peer is still streaming; `b1e2b4af`'s "fatal error" policy closed
   the whole session on the first one, **silently** (no log). Fixed in
   `de112783`: fatal only before the first inbound datagram or after
   `FATAL_ERROR_MIN_SILENCE_MS` (2 s) of inbound silence, and always logged
   (`transport error: … (fatal=…, inbound_silent=…)`).
3. **Self-inflicted session death.** Go's smux skips its keepalive timeout
   while the receive bucket is drained (`recvLoop may block while bucket is 0,
   in this case, session should not be closed`); this port lacked that guard,
   so a slow local consumer tore down its own session. Added in `de112783`
   (`smux::is_keepalive_timeout`, `KcptunSession::inbound_idle_expired`, and
   the watchdog's silence check).

## 2. Measured facts about the user's deployment (reuse these, don't re-derive)

Access: `ssh root@38.145.210.53` works non-interactively (BatchMode, key-based).
Their service: `systemctl status kcptun`, unit `ExecStart=/usr/local/bin/kcptun-server
-l :50200-50250 -t 127.0.0.1:8388 (shadowsocks) --mode fast --dscp 46 --sndwnd 1024
--rcvwnd 1024 --mtu 1350 --datashard 10 --parityshard 3 --tcp --snmpperiod 120
--snmplog /tmp/snmp.log --log /tmp/kcptun.log --sockbuf 10485760 --smuxbuf 10485760`.
Client (their machine, this Mac): `--mode fast --mtu 1350 --sndwnd 256 --rcvwnd 1024
--dscp 46 --datashard 10 --parityshard 3 --autoexpire 300 --conn 8 --sockbuf/--smuxbuf 10485760`.

| fact | value |
|---|---|
| path RTT / ICMP loss | 195 ms (stddev 0.5 ms) / **7 %** (100 pings) |
| real bulk loss (client FEC repairs ÷ shards) | **6.6–7.8 %** — matches ICMP, so the path is not losing 30 % |
| server-reported retransmission (`--mode fast`) | **17–21 %**, of which ~99 % RTO-driven |
| vCPU / RAM / swap | **1 / 512 MB / swap in use**, load 6.9–7.4 during their testing |
| server `--tcp` | **no-op in this build** — `ss` shows UDP 50200-50250 |
| FEC counters | **receive-side** in both Go and Rust (`FECFullShards` = complete shard sets received, `FECParityShards` = parity received). 2888/970 ≈ 2.98 ⇒ config is datashard 10 / parityshard 3 |

**Config levers, A/B measured on that path (5 MiB each, ±15 % run-to-run noise):**

| client setting | speed | retransmission |
|---|---|---|
| `--mode fast3` (interval 10) | 1102 KB/s | 28.6 % |
| `--mode fast` (interval 30, their setting) | 1585 KB/s | 19.6 % |
| `--mode manual --nodelay 0 --interval 100 --resend 2 --nc 1` | **1668 KB/s** | **10.4 %** |
| `--mode fast --acknodelay` | 1224 KB/s | 33.9 % |

Recommended to the user (not applied yet): both ends `--mode fast3` or manual
with `--interval 100`; drop the server's `--tcp`; `--smuxbuf` 10 MiB → 1–4 MiB
on a 512 MB box. `--dscp 46` made no measurable difference (keep or drop).

## 3. OPEN ITEM A (highest value): fast retransmit does not fire on the live path

Symptom: with 7 % real loss, the sender retransmits ~2.5× that, and
`FastRetransSegs = 15` vs `LostSegs = 1,569` — i.e. **almost every loss waits
for an RTO**, and the tight RTO floor (`srtt + max(interval, 4*rttvar)`) then
turns late ACKs into spurious retransmissions. Throughput drops with the
retransmission rate (table above).

Already checked and found **correct** (do not redo these):
`KcpConfig::apply` → `set_mode` → `set_nodelay` wires `fastresend` ✓ (unit test
asserts 2); `KCP::parse_fastack` matches kcp-go line for line ✓; the three
flush branches (fast / early / RTO) match ✓; SNMP counters are wired to the
same values as Go ✓; ACK `ts` echo in `ack_push` ✓; `input_no_flush_typed` is
the only ack-parsing path and the server uses it (`conn/endpoint.rs:855`) ✓.

The unit test `test_fast_retransmit_fires_on_duplicate_acks` **passes** — it
exercises the bare `KCP` layer with a single loss. The divergence is therefore
in something only the live path exercises (bursty multi-loss with a full window,
the batched inbound path, FEC reconstruction interleaving, or the flush cadence).

Suggested next step: build a **KcpStream-level** bulk-loss harness — inject
~7 % loss in both directions (`PacketTransport` impl, see
`kcp-rs/tests/data_correctness.rs::FlakyChannel` and
`kcp-rs/src/conn.rs::PartialBatchTransport` for the two existing patterns),
push ~8 MiB with `--mode fast`-equivalent settings, then assert
`fast_retrans ≈ losses` and `lost_segs ≈ losses`. That reproduction will make
the bug fixable; today it only reproduces on the live path.

## 4. OPEN ITEM B: unbounded writer → memory blowup

A sustained high-rate transfer grows the **sender's** RSS roughly in step with
the forwarded bytes: measured on loopback at ~150 MiB/s, server RSS 1.95 GB at
t+8.8 s → 2.04 GB at t+19.4 s → process gone at t+30.4 s (`poll() = -9`, SIGKILL,
no panic, no crash report, listening socket disappears). On the user's 512 MB
box the same growth means swap death. Root cause: the per-stream send buffer is
unbounded (`Stream::write` pushes into `inner.send`; flow control only applies
in `drain_send_max`), so a fast producer with a slow/limited link buffers
everything. Fix shape: block the writer once `pending_send` exceeds the peer
window / `max_stream_buffer` (Go's smux `Write` does exactly that). Reproduce
locally with any full-rate download (see §6).

## 5. Things that were verified as *not* the problem (don't chase these)

- **Not our commits**: the ~4 GiB wall reproduces on pre-`b1e2b4af` binaries
  (`d29e9a90`), built in a throwaway worktree — it is the memory blowup (§4),
  not an accounting bug introduced here.
- **Not keepalive cadence**: `check_keepalive` uses `last_keepalive_ms`, so both
  peers emit a NOP every interval (Go-compatible).
- **Not the u32 counters**: the `~4 GiB` figure is "bytes forwarded before the
  process is killed", not a counter wrap.
- **Not DSCP**, not the window sizes: within noise (table above).

## 6. How to reproduce the live measurements (no scripts are checked in)

The scratch scripts used live in `/tmp` and were deleted once by the OS — expect
to rewrite them (or keep them under `~/kt-scratch/`).

- **Path quality**: `ping -c 100 -i 0.2 38.145.210.53` (loss/RTT).
- **Isolated tunnel test on the VPS** (never touch the `kcptun` service):
  start transient units —
  `systemd-run --unit=probe-http --collect /usr/bin/python2 -m SimpleHTTPServer 8399`
  (serves from `/`, so fetch `/tmp/probe.bin`; this systemd has no
  `--working-directory`) and
  `systemd-run --unit=probe-kcp --collect /usr/local/bin/kcptun-server -l :50300-50310
  -t 127.0.0.1:8399 --key verify-test --crypt null --mode fast --mtu 1350 --sndwnd 1024
  --rcvwnd 1024 --datashard 10 --parityshard 3 --snmpperiod 5 --snmplog /tmp/snmp_probe.log`
  then run a local client (`--snmplog`, `--snmpperiod 5`) against
  `38.145.210.53:50300-50310`, `curl` the blob, and diff the last complete SNMP
  rows on both sides. **Clean up the units, the blob and the logs afterwards.**
- **Retransmission vs real loss**: client `FECRecovered / (FECFullShards × 10)`
  is the true downlink loss; server `RetransSegs / OutSegs` is what KCP did.
- **Desync / restart recovery**: kill the server, restart it after 0.2 s (short
  enough that no ICMP reaches the client), then probe every 3 s — the ACK-stall
  rule must close the stale session within ~10.5 s.
- **Retirement**: `cargo test --release -p kcptun-server --test session_retirement_test -- --nocapture`
  (all three phases are asserted; it fails against the pre-`81d25d15` client).

## 7. Landmines

- `find_bin` in `kcptun-server/tests/*` prefers an *existing* `target/release`
  binary. `reconnect_test.rs` has a source-vs-binary freshness guard; the other
  test files do **not**. A stale client binary silently tests old code — always
  `cargo build --release -p kcptun-client -p kcptun-server` first.
- `knet::mono_ms()` is milliseconds since first call (process start), so tests
  cannot back-date timestamps; shorten the timeout instead.
- `lsof -ti:PORT | xargs kill -9` also kills the owner of a *connected* socket
  whose foreign port matches — that is why the old reconnect test also killed
  the client. Use `kill` on the child PID where possible.
- The Rust client dials with a **fixed conv** (`kcptun-client/src/main.rs:31`),
  where Go randomizes per dial. Known Go-parity gap; not fixed here.
- `kcp-rs/src/conn.rs` does not depend on `log`… it does now (`log = "0.4"`
  added in `de112783`); `smux-rs` already had it.
- Dead test targets: `kcp-rs/tests/kcpconn_integrity.rs`,
  `kcpconn_listener.rs`, `tcpconn_tcp.rs` are gated behind the removed
  `async-tokio`/`async-smol` features, so they compile to nothing. Reviving them
  needs a `kio` → `knet` rename plus API fixes (in-file NOTE added).

## 8. Test surface added this session

- `kcp-rs`: `AckStallDetector` unit tests (fires after the window / never on
  outage / resets on progress / never without data in flight / never on flow
  control); `note_io_error_*` tests updated for the gated fatal policy.
- `smux-rs`: `keepalive_timeout_ignores_silence_while_receive_window_is_drained`.
- `kcptun-client`: `test_retired_session_waits_for_streams`.
- `kcptun-server/tests/session_retirement_test.rs`: the three-phase lifecycle
  test described above.
