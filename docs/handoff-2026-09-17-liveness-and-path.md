# Handoff — session-liveness fixes + live-path diagnosis (2026-09-17)

Audience: the next agent picking this up. Everything below was measured or read
from source during this session; nothing is assumed.

- **HEAD**: `a8114c5c` on `main`, working tree clean. One squashed commit holds
  all of this session's code work (it was amended repeatedly on request); the
  message lists every change with its measurements.
- **Not pushed**: `origin/main` is an ancestor → plain `git push origin main`
  (no force needed).
- **Gates at HEAD**: `cargo test --workspace` 29 targets green, `make clippy`
  clean for the touched crates, `cargo fmt --check` clean.

---

## 1. Test environment

### 1.1 This machine (macOS arm64, the developer box)

- `cargo`/`rustc` are **not on the Bash tool's PATH**: run cargo through
  `zsh -lc 'cd <repo> && cargo …'`.
- Release binaries: `target/release/{kcptun-client,kcptun-server}`.
  Integration tests in `kcptun-server/tests/*` spawn these **release** binaries
  through `find_bin`, which prefers an existing `target/release` build — always
  `cargo build --release -p kcptun-client -p kcptun-server` before running them,
  or a stale binary silently tests old code.
- Integration tests that need the binaries:
  ```bash
  cargo test --release -p kcptun-server --test session_retirement_test -- --nocapture
  cargo test --release -p kcptun-server --test reconnect_test
  cargo test --release -p kcptun-server --test autoexpire_multi_port_test
  ```
  Ports: retirement test uses **19710-19712** (fixed), reconnect derives
  `19600 + pid%1000 …`, bench starts at **20000** and increments by 10.
  Leftover processes from an aborted run occupy those ports and make the next
  run flaky — check with `lsof -nP -iTCP:19712` and kill by PID (do **not** use
  `lsof -ti:PORT | xargs kill -9` on a tunnel port: it also matches the client's
  connected socket and takes the client down; the tests' `kill_port` has the
  same caveat).
- Benchmark harnesses (both compare Go vs Rust):
  - `bench/run_bench.sh` — the one the user runs. Defaults: `--crypt aes
    --nocomp --mode fast --sndwnd 1024 --rcvwnd 1024 --smuxver 2`, 1 connection,
    `BENCH_DATA_MB=200`, 3 rounds. Knobs: `BENCH_DATA_MB`, `BENCH_ROUNDS`,
    `BENCH_CONNECTIONS`, `BENCH_FORCE=1` (skip the load guard), `BENCH_KEEP_LOGS`,
    `BENCH_LOG_DIR`. **New this session**: every attempt's server/client output
    is kept in `/tmp/kcptun-bench-logs/<pair>-a<n>-{server,client}.log` and its
    tail is printed when an attempt fails; a failed attempt is retried once with
    fresh backends; `Throughput: 0.00 MB/s` now counts as failure.
  - `bench_rust_vs_go.py` — repo-root alternative, `--quick --rust-only --conn N`.
- Cross-build for the VPS: `make linux` → `x86_64-unknown-linux-musl` via
  Homebrew `x86_64-linux-musl-gcc` (`target/x86_64-unknown-linux-musl/release/`).
  Docker is **not** running on this box, so that is the only cross path.

### 1.2 Reproducers (local, no VPS needed)

- **Loss injection** — `/tmp/udp_loss.py` (recreate it, /tmp gets cleaned):
  ```python
  # usage: python3 udp_loss.py <listen_port> <target_host> <target_port> <loss 0.0-1.0>
  import random, socket, sys, threading, time
  lp, thost, tport, loss = int(sys.argv[1]), sys.argv[2], int(sys.argv[3]), float(sys.argv[4])
  sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); sock.bind(("127.0.0.1", lp))
  down = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); clients = {}
  def up():
      while True:
          d, a = sock.recvfrom(65536); clients[a] = time.time()
          if random.random() < loss: continue
          down.sendto(d, (thost, tport))
  def dn():
      while True:
          d, _ = down.recvfrom(65536)
          if random.random() < loss: continue
          now = time.time()
          for a, t in list(clients.items()):
              if now - t < 30: sock.sendto(d, a)
  threading.Thread(target=up, daemon=True).start(); dn()
  ```
  Client dials the relay (`-r 127.0.0.1:<lp>`), relay forwards to the real
  server. Loss 1.0 = silent blackhole (no ICMP, so the FATAL ICMP policy is not
  what fires). Restarting the relay changes the server-visible peer port, which
  makes the server build a *new* session while our KCP is mid-sequence — that
  artefact produced fake "session blackholes" during this session, so **keep the
  relay process stable across a measurement**.
- **Echo target** for local pairs: `python3 -u -c` accept-loop echoing
  `recv(65536)` back (see `bench/echo_server.py`).
- **Flow-control / memory probe**: temporarily sum `stream.pending_send()` over
  `smux.streams()` in `write_loop` and log it; used to show the sender's
  buffering (44,322,944 B → 65,536 B after the window fix). Remove afterwards.
- **Config discipline**: `--datashard/--parityshard` must match on both ends. A
  mismatch is a **total blackhole** that looks exactly like a dead peer (server
  never builds a session, client `snd_una` stays 0, watchdog closes at the
  keepalive timeout). This cost hours this session — always diff both ends'
  FEC flags before debugging a "silent" session.
- Other config rules: `--keepalivetimeout` must be ≥ `--keepalive` (smux
  `Config::verify` rejects otherwise), and `0` disables the check.

### 1.3 The VPS (38.145.210.53)

- `ssh -o BatchMode=yes root@38.145.210.53` works non-interactively (key-based).
- Current unit (verbatim):
  ```
  /usr/local/bin/kcptun-server -l :50200-50210 -t 127.0.0.1:8388 \
    --keepalivetimeout 90 --mode manual --nodelay 0 --interval 100 --resend 2 --nc 1 \
    --sndwnd 512 --rcvwnd 256 --mtu 1200 --datashard 10 --parityshard 2 \
    --snmpperiod 120 --tcp --snmplog /tmp/snmp.log --log /tmp/kcptun.log \
    --sockbuf 10485760 --smuxbuf 10485760 --shards 1
  ```
  (`--dscp` is absent from the current unit and present in the pristine backup; A/B showed no measurable effect either way.)
- Backups: `/etc/systemd/system/kcptun.service.bak-dscp` = the **pristine**
  original (dscp 46, mtu 1350, no keepalivetimeout);
  `/etc/systemd/system/kcptun.service.bak-mtu` = intermediate (mtu 1350,
  keepalivetimeout 120); `/usr/local/bin/kcptun-server.bak-20260915` = the
  pre-session binary. Rollback = copy the `.bak-dscp` unit back +
  `systemctl daemon-reload && systemctl restart kcptun`.
- Deploy recipe (binary only): `make linux` → `scp target/x86_64-unknown-linux-musl/release/kcptun-server root@…:/tmp/x`
  → `install -m 0755 /tmp/x /usr/local/bin/kcptun-server && systemctl restart kcptun`
  → verify the two startup lines (`session watchdog: ack-stall window=…`,
  `smux keepalive: interval=… timeout=…`) and `systemctl is-active kcptun`.
- Other services on the box: `ssserver` on 127.0.0.1:8388 (the tunnel target,
  config `/etc/shadowsocks.json`, credentials verified to match the Mac's
  ss-local), a second public `ssserver` on :18388 (different password),
  `hysteria-linux-amd64 server -c /etc/hysteria.yaml` on **443/udp** (ACME cert
  for `*.sean2021.top`, password auth — installed but unused by the user's
  chain; see §4), `gost`, `easytier-core` + its web port 11211. 1 vCPU / 512 MB,
  `load` was 0.0–5.7 during tests.

### 1.4 Browser chain on the Mac

```
browser → privoxy 127.0.0.1:1087 → ss-local 127.0.0.1:1086 (SOCKS5)
        → kcptun client 127.0.0.1:2900 → VPS:50200-50210 (UDP) → 127.0.0.1:8388 (ss-server)
```
- ss-local config: `~/Library/Application Support/ShadowsocksX-NG/ss-local-config.json`
  (server 127.0.0.1:2900). It is managed by **ShadowsocksX-NG** — if the app is
  off, nothing listens on 1086 and every request fails instantly (privoxy then
  answers HTTP 500). Check `lsof -nP -iTCP:1086 -sTCP:LISTEN` first.
- A hand-started ss-local from the bundle needs
  `DYLD_LIBRARY_PATH=~/Library/Application\ Support/ShadowsocksX-NG/ss-local-3.2.5`
  (the binary's rpath is the unexpanded `@@HOMEBREW_PREFIX@@`).
- The kcptun client currently running on 2900 was started with:
  `--keepalivetimeout 90 --mode manual --nodelay 0 --interval 100 --resend 2 --nc 1
   --mtu 1200 --sndwnd 512 --rcvwnd 1024 --dscp 46 --datashard 10 --parityshard 2
   --autoexpire 0 --sockbuf 10485760 --smuxbuf 10485760 --log /tmp/chain_client.log`
  (the user's own client must be restarted to pick up the new flags).

### 1.5 Live-path facts measured this session

| measurement | value |
|---|---|
| RTT to the VPS (ping 100) | 188–195 ms, 0–7 % ICMP loss |
| plain TCP, VPS → Mac (scp/curl) | **13–39 KB/s**, once a 50 MB scp did not finish in 10 min |
| plain TCP, Mac → VPS | **1.62 MB/s** |
| VPS → internet (Cloudflare, single stream) | **5.1 MB/s ≈ 41 Mbps** (4 parallel: 3.1 MB/s, load 5.7 → CPU-capped) |
| kcptun tunnel (Rust↔Rust, local HTTP target on the VPS) | 20 MB every run, 1.04–1.17 MB/s (median 1.12) |
| kcptun tunnel (Go↔Go control, same target, interleaved) | 20 MB every run, 1.14–1.43 MB/s (median 1.25); 7 rounds, order alternated → Go ~10 % faster on this lossy path |

Interpretation used throughout: the China-bound **downlink** is throttled and
appears to drop *large* packets first — with `--mtu 1350` the server's write loop
blocked ~4×/100 s on a full `--sndwnd 512` window; with `--mtu 1200` it was
~1×/150 s and data got through again. Bare ACK-sized segments kept flowing
throughout (`rx_age_ms` small while `inbound_age_ms` grew).

### 1.6 Reading the logs and counters

- **Watchdog close line** (`kcptun-common/src/kcptun_session.rs`) now carries
  everything needed to classify a death:
  - `kcp_dead=true` → KCP's own `dead_link` (retransmission budget spent; native
    KCP rule, minutes).
  - `smux_timeout=true, inbound_idle=true` → smux's frame-level keepalive
    timeout (`--keepalivetimeout`, 90 s in production).
  - `ack_stalled=true` → our own rule; `peer_restart_seen` says whether the peer
    restarted its KCP (fast path) or the 60 s grace applied.
  - `out_age_ms` / `write_blocked_ms` → our own writer parked in
    `kcp.write_all` (window full) — the peer cannot be blamed for that silence.
  - `inbound_age_ms` (payload) vs `rx_age_ms` (any datagram): frames stale +
    datagrams fresh = the peer's writer is stuck; both growing = nothing arrived.
  - `bucket` / `rmt_wnd` → whether *we* were the ones applying backpressure.
- Started-up lines report the effective keepalive/ack-stall settings; the 2 s
  debug heartbeat (`RUST_LOG=debug`) prints `snd_una/snd_nxt/rcv_nxt/wait_send/
  rmt_wnd` + smux bucket/streams + pump state.
- Server SNMP CSV (`/tmp/snmp.log`, 120 s rows): columns used were 14 `OutSegs`,
  17 `RetransSegs`, 20 `LostSegs`, 13 `InSegs`.

---

## 2. Completed work in `a8114c5c` (all measured)

1. **SMUX send buffer bounded by the peer window** (`smux-rs/src/{io,stream}.rs`):
   `SmuxIo::poll_write` used to bypass the window check entirely.
   `Stream::send_buffer_limit()` = peer window − in-flight,
   `poll_send_capacity()` parks the writer, `drain_send_max` wakes it.
   Measured: buffered 44,322,944 B → constant 65,536 B; live 6 parallel
   downloads 121 MB → 21 MB peak RSS and 28 MB → 68 MB delivered in the same
   30 s; loopback throughput unchanged.
2. **Listener evicts a stale session instead of swallowing a re-dial**
   (`kcp-rs/src/sharded.rs`, `conn.rs`, `conn/endpoint.rs`): signals are a conv
   mismatch and a sequence restart (`sn == 0 && una == 0 && rcv_nxt >= 16` —
   the `una` check and the floor were added after the naive `sn == 0` version
   evicted *healthy* new sessions and broke both integration tests). Live: 30
   evictions logged; client re-dials from the same source port 1–2 s after a
   close (33 same-port re-dials measured, minimum gap 1 s).
   Regression test: `kcp-rs/tests/conv_mismatch.rs`.
3. **Session liveness is observable**: close line fields above, write-stall
   (>3 s) and read-park (>5 s) warnings, every previously silent exit now logs
   (KCP EOF with transport counters + last error), 2 s debug heartbeat.
4. **Keepalive knobs**: `--keepalive` (default 10) and `--keepalivetimeout`
   (default 30, previously hardcoded) on both binaries, visible defaults, `0`
   disables, config file still wins over CLI (Go-compatible).
5. **Ack-stall rule no longer fires on loss**: `--ackstalltimeout` (default 10,
   0 = off) applies the fast window only with peer-restart evidence; otherwise a
   frozen `snd_una` is treated as loss and only a 60 s grace
   (`ACK_STALL_NO_RESTART_MS`) closes the session. `peer_restart_seen` is in the
   close line. Unit tests in `kcptun_session.rs` (`silence_tests`,
   `ack_stall_tests`).
6. **Bench harness**: keeps per-attempt backend logs, retries once, treats a
   zero throughput as failure (see §1.1).

Also in the commit: RepeatSegs counting now matches Go, with its regression test
and `bugs/BUGREPORT_RETRANSMIT_STORM_STARVED_SENDER.md` (the user's parallel
workstream).

---

## 3. How to re-verify (fast loop)

```bash
zsh -lc 'cd ~/Desktop/kcptun-rs && cargo build --release -p kcptun-client -p kcptun-server'
cargo test --release -p kcptun-server --test session_retirement_test -- --nocapture   # ~20 s
cargo test --release -p kcptun-server --test reconnect_test                           # ~17 s
cargo test -p kcp-rs --features async --test conv_mismatch                            # ~1 s
zsh -lc 'cargo test --workspace'          # 29 targets
```
Both integration tests were flaky before commit `a8114c5c` (reconnect ~50 % of
runs) and pass 5/5 afterwards; if they fail again, check for leftover processes
on 19710-19712 / 19600-20600 first.

---

## 4. Open items / next steps

1. **The downlink is the wall.** Nothing in the tunnel fixes it. Candidate
   experiments, in order: (a) `--tcp` (tcpraw) — needs a **Linux client** in
   China (the macOS client refuses: `--tcp requires Linux`), and TCP-shaped
   traffic still trickles where UDP dies, so it is the most promising;
   (b) the Hysteria2 server already installed on 443/udp — needs a Mac client
   (sing-box / Clash.Meta / hysteria) and a config review (`bandwidth` is set to
   1 Gbps with `ignoreClientBandwidth: true`; `obfs: salamander` and `udpHop`
   would help against DPI/QoS); (c) stricter sweeps of `--mtu` (1100) and the
   server's `--sndwnd` (512 → 256/128) with the interleaved method in §1.5.
2. **The 4 GiB / memory-blowup class** is fixed for the sender (item 1 above);
   the earlier handoff's OPEN ITEM A (fast retransmit not firing on the live
   path) is untouched — `bugs/BUGREPORT_RETRANSMIT_STORM_STARVED_SENDER.md` and
   `kcp-rs/tests/repeat_segs_counting.rs` are the current workstream.
3. **The ack-stall rule's literal variant** the user asked about first (gate on
   `write_blocked_ms`) was *not* implemented — it would disable the rule in both
   target cases (restart and loss both fill our window and block our writer).
   The restart-evidence gate achieves the intent; if the user insists, the
   trade-off is documented in the commit message.
4. **SS-layer mystery**: with the tunnel healthy (1.0–1.3 MB/s to a local HTTP
   target on the VPS) the *browser chain* still returned 0 bytes for long
   stretches while the ss-server itself was healthy (23 s CPU over 43 h, box
   idle). Client-side logs showed `pipe completed: 2373 sent, 0 recv`. Worth
   isolating whether the ss-server's outbound connections to specific
   destinations are being dropped (the VPS's own curl to the same hosts was
   fast: 10 MB from Cloudflare in 3.0 s).
5. **The user's own client binary** must be restarted for the new flags
   (`--keepalivetimeout 90`, `--ackstalltimeout 10`, `--mtu 1200`) — the current
   binary on disk is the new one, but a running process keeps the old image.
6. **Environment trap worth remembering**: `zsh` does not word-split unquoted
   variables (`$FLAGS` becomes one argument — use arrays), `${size_download}`
   inside a double-quoted `curl -w` is expanded by the shell before curl sees it
   (use single quotes), and `rm -f x_*.bin` aborts the whole command line when
   the glob matches nothing.
