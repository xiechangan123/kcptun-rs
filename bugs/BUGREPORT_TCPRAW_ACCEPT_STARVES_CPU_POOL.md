# BUGREPORT — tcpraw accept starves the shared cpu_block pool (session mute)

Date: 2026-09-20
Affects: kcptun-server with `--tcp` (Linux tcpraw), any session whose write loop
uses Snappy/crypto CPU offload (production default).
Fixed in: `knet-rs/src/net/tcpraw.rs` (dedicated accept thread),
`kcptun-common/src/kcptun_session.rs` (`COMPRESS_CPU_BLOCK_WAIT` bounded offload).
Regression: `knet-rs::net::tcpraw::integration_tests::pending_accepts_do_not_starve_the_cpu_block_pool`
(Linux root; fails on the old code, passes on the new code).

## Symptom

Every few minutes the tunnel went completely mute for ~120 s while the client's
watchdog tore the session down and rebuilt it (`inbound_idle=true, smux_timeout=true`).
Browser page loads through the chain hung until that rebuild; small/sequential
requests often still worked. Server-side fingerprint during the mute (this is
the differentiator from the Bug A/B/C mute family):

```
out_age_ms growing (no server->client frames at all, keepalive NOPs included)
write_blocked_ms=0            (NOT parked in kcp.write_all)
read_parked_ms=0, bucket fine, wait_send=0, rmt_wnd open
read side alive: rcv_nxt advances, target connects succeed, ss-server responds
```

I.e. the server received requests, connected to the target, the target answered
(tcpdump on `lo port 8388` showed response bytes arriving at kcptun-server) —
but **nothing ever left the server**: the session `write_loop` vanished between
iterations.

## Root cause chain

1. `knet::TcpRawListener::accept()` ran a **blocking** `std::net::TcpListener::accept()`
   inside `knet::cpu_block` — i.e. on the shared persistent blocking pool
   (`knet-rs/src/task/tokio.rs`, size = `available_parallelism().clamp(2, 8)`,
   so **2 workers** on the 1-vCPU VPS).
2. `kcptun-server` with `-l :50200-50210 --tcp` spawns **one accept loop per
   port** at startup. Each loop submits a blocking accept job; the first two
   workers park in `inet_csk_accept` until a TCP handshake completes on their
   port, and the remaining nine accept jobs stay queued behind them.
3. Internet scanners completing handshakes on 50200/50201 occasionally freed a
   worker for a moment (explains why large transfers *sometimes* succeeded);
   when no scan arrived, the pool was fully occupied (observed live: both
   `tokio-cpu-*` threads in `inet_csk_accept`, `Recv-Q=2` stuck on :50210,
   scanner CLOSE-WAITs).
4. The session `write_loop` offloads Snappy (`kcptun_session.rs`, batches
   >= 16 KiB) and crypto (`kcp_transport.rs::encrypt_with`) to the same pool.
   With the pool occupied, the write loop parked **inside
   `cpu_block(encode).await`** — past the previous `end_write()` (so
   `write_blocked_ms=0`, `out_age_ms` frozen-growing) and before the keepalive
   encode, so not even NOPs left the server.
5. Client saw SMUX-level silence (ACKs kept flowing from the KCP input path),
   `inbound_age` hit 120 s, watchdog closed the session, the pool was still
   occupied → the next session muted on its first >= 16 KiB batch. Repeat every
   2–4 minutes.

The pre-existing Bug C fingerprint (server `bucket` going negative,
`read_parked_ms` growing) is the *downstream* shape of the same stall: with the
write loop dead, `tokens_to_return` never gets reclaimed and unread request
bytes pile up in streams.

## Fix

1. **knet/tcpraw.rs**: one dedicated `tcpraw-srv-accept` thread per listener
   (same pattern as the existing `tcpraw-srv-capture` thread). The thread polls
   `accept()` non-blocking with a 100 ms sleep and hands connections over via a
   bounded(16) `async_channel`; `accept().await` only receives. Blocking waits
   never touch the cpu_block pool again. The thread is interruptible (close
   signal / drop ends it), so listener teardown no longer leaks a blocked
   accept.
2. **kcptun-common/kcptun_session.rs**: the write loop's Snappy offload waits
   at most `COMPRESS_CPU_BLOCK_WAIT` (200 ms, vs sub-millisecond actual work)
   for `cpu_block`; on timeout it logs (once per 30 s) and compresses **inline**
   from the still-owned `plain` (the offloaded job gets a refcounted `Bytes`
   clone, so no data loss either way). A starved pool now degrades to slightly
   slower batches instead of a session mute.

## Deployment note

Production server runs `--tcp` and the VPS is public — the trigger (scanner
handshakes + scanner silence windows) is environmental and constant. Both ends
need the rebuilt binaries (the write-loop change is in the shared
`kcptun-common`; the tcpraw change is server-side only).
