# Handoff — session mute under proxy load (TX token + flush bound + SMUX window) — 2026-09-20

Audience: the next agent. Everything here was measured or read from source during
this session. The user's complaint: **"kcp 做代理基本没法用"** / **YouTube will
not load when refreshed** through the chain.

**Chain (do not change ss-local):** Browser PAC → ss-local `127.0.0.1:1086` →
kcptun-client `127.0.0.1:2900` → VPS `:50200-50210` → ssserver `127.0.0.1:8388`
→ internet. The user's ss-local config already targets `2900`. Do not kill or
reconfigure ss-local; ShadowsocksX-NG owns that process.

- **Branch**: `fix/tx-flush-timeout` (worktree `/Users/yangzhiqin/Desktop/kcptun-rs-yt-txfix`), merged to `main`.
- **Deployed (both ends, same tree)**:
  - VPS `/usr/local/bin/kcptun-server` sha256 prefix `4e9406693b1385c9` (smuxfix deploy ~08:14Z)
  - Mac client `target/release/kcptun-client` in tmux session `kcptun` (same CLI as before)
  - Rollback on VPS: `/usr/local/bin/kcptun-server.bak-txflush-20260920`, older `.bak-tokenfix-20260920`
- **Monitoring is still ON** (§4) — 1 Hz VPS sampler + debug session heartbeat. Turn off when the user is done.
- **GitNexus**: MCP tools were not loaded this session; `node .gitnexus/run.cjs` fails under Node 16 (`pino` `tracingChannel`). Impact analysis was done by caller grep. Re-index/run detect_changes from a Node ≥20 login shell before the next structural change.

---

## 0. What was wrong when the user refreshed YouTube

Real Chrome via SOCKS (CDP on port 9333, isolated profile) against
`https://www.youtube.com/` produced:

- Shell sometimes painted (`ytd-app` present) but `body` empty / `ERR_FAILED`
- Network: many `net::ERR_PROXY_CONNECTION_FAILED`, some `ERR_CONNECTION_RESET`
- Sequential curl to the same hosts could still be `200` (YouTube HTML ~880 KB,
  ytimg 20 KB) — **single requests looked fine; concurrent page load did not**
- Client pipes: dozens of `pipe completed: ~2KB sent, 0 recv`
- Debug Chrome also showed `--host-resolver-rules=MAP * ~NOTFOUND` as an
  unsupported flag — that is an artifact of the debug browser only; production
  uses system PAC (`SOCKS5 127.0.0.1:1086`), not that flag.

Two **project bugs** explained the mute. Both are fixed in this branch and live
on both ends. A third factor (path/DNS) remains — §5.

---

## 1. Bug A — single-sender TX token leaked on cancel (fixed)

### 1.1 Symptom

`pipe completed: <N> sent, 0 recv`; server heartbeat:

```
write_blocked_ms == out_age_ms  (both grow), rmt_wnd=1024, kernel tx_queue=0
```

### 1.2 Root cause

`SharedIoState::try_drain_and_send` (`kcp-rs/src/conn/endpoint.rs`) took
`is_sending` via CAS and held it across `flush_tx_batch(...).await`, releasing
only after the await. A stream closing mid-send dropped the future;
`finish_sending()` never ran → session permanently mute.

### 1.3 Fix

RAII `SendToken` in `endpoint.rs`; Drop calls `finish_sending()`. Used at:

1. `try_drain_and_send`
2. `spawn_send_remainder`'s spawned task
3. **Flush-loop fast drain + second drain** (these still used manual
   `finish_sending()` after an unbounded await — added this session)

Regression: `conn::integ::cancelled_send_releases_the_token` (fails without the
guard). Do not delete the guard without re-checking that property.

---

## 2. Bug B — unbounded `flush_tx_batch` await mutes the session (fixed)

This was §2.1 in the earlier handoff and **still reproduced after Bug A was
deployed**.

### 2.1 Fingerprint

Server:

```
wait_send=512  rmt_wnd=1024 (or 0)  write_blocked_ms == out_age_ms growing
kernel UDP tx_queue often 0
watchdog: closing ... write_blocked_ms=145899..155199
```

`knet` `send_batch` can park on `writable()` with no bound. Whoever holds
`is_sending` across that await freezes ACKs/retransmits/keepalives.

### 2.2 Fix

- `TX_FLUSH_TIMEOUT = 50ms` in `endpoint.rs` (mirrors `spawn_send_remainder`).
- `send_drained_batch`: `knet::timeout(TX_FLUSH_TIMEOUT, flush_tx_batch(...))`.
  - Success / IO error → recycle drained batch.
  - Timeout, **no FEC** → `requeue_raw_packets_front` (duplicates OK; KCP ignores).
  - Timeout, **FEC on** → do **not** requeue expanded wire groups; recycle pre-FEC
    inputs and leave recovery to KCP RTO (encoder would start a new RS group).
- Flush loop both drain sites now use `SendToken` + `send_drained_batch`.

Regression: `conn::integ::flush_tx_timeout_releases_token_and_requeues`.

**Blast radius (HIGH):** every `KcpStream` write, input-loop inline ACK send,
and flush-loop maintenance path. Caller set was mapped by grep (GitNexus
unavailable). After deploy: `cargo test -p kcp-rs --features async --lib` green
(92), clippy `-D warnings` clean on `kcp-rs --lib`.

---

## 3. Bug C — SMUX receive-window tokens leaked on stream reap (fixed)

This is what made **YouTube concurrent load** die even when TX was no longer
permanently muted.

### 3.1 Fingerprint (client)

```
smux(bucket=-23941 streams=4..14)   // stuck negative, never recovered
read_parked_ms=89415 → 155714+       // read_loop parked
rcv_nxt frozen                       // no SMUX payload consumed
pipe completed: ~2KB sent, 0 recv
```

Server on the same session:

```
rmt_wnd=0  wait_send=512  streams=170..186
write_blocked_ms == out_age_ms growing (peer window closed because we stopped reading)
```

### 3.2 Root cause

SMUX v1 has **no per-stream window**. `Session::process_data` charges
`token_bucket` for every buffered payload byte. Tokens come back via:

- `prepare_outbound` → `take_return_tokens` (bytes the app actually read)
- `Session::remove_stream` / `reap_stale_streams` → `Stream::recycle_tokens`
  (unread buffered bytes + pending returns)

`kcptun_common::write_loop` **stale-stream cleanup** did:

```rust
if let Some(stream) = stream_map.remove(&id) {
    stream.close();  // NO recycle_tokens / return_tokens
}
```

Browser cancel / `0 recv` pipes leave unread SMUX data. Each closed stream
leaked its unread share. Enough YouTube streams → `bucket < 0` forever →
`has_receive_capacity() == false` → `read_loop` parks → KCP rcv window fills →
peer `rmt_wnd=0` → session mute.

`~23941` bytes leaked ≈ unread tails on closed streams, not a 4 MB blow-up.

### 3.3 Fix

1. `kcptun-common/src/kcptun_session.rs` `write_loop`: collect stale ids, then
   call `smux.remove_stream(id)` (recycles tokens, closes, rebuilds snapshot,
   `return_tokens`). Do **not** remove from the map by hand.
2. `smux-rs/src/session.rs` `process_data`: on `push_data_bytes` failure
   (PSH or FIN-with-data), `return_tokens(frame.data.len())` — charge must not
   stick when the bytes were not buffered.

After deploy, healthy client heartbeat under load:

```
smux(bucket=4194304 streams=N)  read_parked_ms=0  write_blocked_ms=0
rcv_nxt advancing; pipes e.g. "929 sent, 915889 recv"
```

Smoke (socks5h://127.0.0.1:1086, post-deploy ~08:20Z):

| Probe | Result |
|-------|--------|
| `https://www.youtube.com/` | 200, ~1.4 s, ~890 KB |
| `i.ytimg.com/.../hqdefault.jpg` | 200, ~0.9 s, 21 KB |
| `www.gstatic.com/.../favicon_144...png` | 200 |
| 4× parallel `google.com/generate_204` | all 204 ~0.5 s |
| recent `0 recv` count | 0 |

---

## 4. Instruments (still on — remove when user is done)

### 4.1 Heartbeat

`RUST_LOG=info,kcptun_common::kcptun_session=debug` on server unit and client
tmux. One `session state:` line per session ~2 s.

| Pattern | Meaning |
|---|---|
| `write_blocked_ms == out_age_ms` growing, `rmt_wnd` open, txqueue 0 | TX token / unbounded flush (Bug A/B) — should be rare now |
| `rmt_wnd=0` + client `read_parked_ms` growing + `bucket<=0` | SMUX window (Bug C class) — check recycle paths |
| `rx_age_ms` small, `inbound_age_ms` large | datagrams arrive but no SMUX frames |
| both ages growing | peer silent / path dead |
| `wait_send` pinned at `snd_wnd` | local send window full; check peer `rmt_wnd` |

**Do not use blanket `RUST_LOG=debug`** (`smux_rs::io` per-frame logging melted
the 1-vCPU box).

### 4.2 VPS sampler / SNMP / tcpdump

Unchanged from the morning handoff: `/tmp/watch.sh` → `/tmp/kcptun_watch.log`;
SNMP CSV `/tmp/snmp.log` (VPS), `/tmp/client_snmp.log` (Mac), period 10 s;
tcpdump recipes on `venet0` / `lo` port 8388.

### 4.3 Turn instrumentation off (VPS)

```bash
sed -i 's/^Environment=RUST_LOG=.*/Environment=RUST_LOG=info/' /etc/systemd/system/kcptun.service
sed -i 's/--snmpperiod 10/--snmpperiod 120/' /etc/systemd/system/kcptun.service
systemctl daemon-reload && systemctl restart kcptun
for p in $(pgrep -f "^/bin/sh /tmp/watch"); do kill $p; done
```

Restart **client tmux** to drop `RUST_LOG=...=debug` as well.

---

## 5. Production state

Server ExecStart (unchanged flags):

```
/usr/local/bin/kcptun-server -l :50200-50210 -t 127.0.0.1:8388 --mode fast2 \
  --keepalivetimeout 120 --sndwnd 512 --rcvwnd 512 --mtu 1200 \
  --datashard 10 --parityshard 3 --snmpperiod 10 --tcp \
  --snmplog /tmp/snmp.log --log /tmp/kcptun.log \
  --sockbuf 2097152 --smuxbuf 4194304 --shards 1
Environment=TOKIO_CONSOLE=1
Environment=RUST_LOG=info,kcptun_common::kcptun_session=debug
```

Client (tmux `kcptun`, log `/tmp/chain_client.log`):

```
.../target/release/kcptun-client -l 127.0.0.1:2900 -r 38.145.210.53:50200-50210 \
  --mode fast2 --mtu 1200 --sndwnd 512 --rcvwnd 1024 --datashard 10 --parityshard 3 \
  --keepalive 10 --keepalivetimeout 120 --autoexpire 0 --dscp 46 \
  --sockbuf 4194304 --smuxbuf 4194304 --snmplog /tmp/client_snmp.log --snmpperiod 10 \
  --log /tmp/chain_client.log
RUST_LOG=info,kcptun_common::kcptun_session=debug
```

**No `--acknodelay`** (measured harmful — see `docs/handoff-2026-09-18-kernel-buffers.md`).

FEC `--datashard/--parityshard` must match on both ends; restart both when they
change. A running client keeps the old image until tmux is restarted.

---

## 6. Still open (not project mute bugs)

- **Path ceiling**: ~1.7 MB/s, ~5% ICMP loss, ~188 ms RTT; second stream does
  not add throughput (§ earlier measurements).
- **VPS DNS / some Google CDNs**: e.g.
  `rr3---sn-n4v7sn7z.googlevideo.com` — ssserver logged
  `dns resolve ... no records found`. Video CDN nodes can fail even when
  youtube.com HTML works. Check `journalctl -u shadowsocks` before blaming KCP.
- **ss-server CLOSE-WAIT orphans**: large Recv-Q on dead 8388 peers after
  session teardown; usually not fatal but looks scary in `ss -tn`.
- **ss-local**: owned by ShadowsocksX-NG. Manual `kill` without restarting the
  app breaks SOCKS (`dyld: libev` via `@@HOMEBREW_PREFIX@@`). Let the app
  manage it. Config already points at `127.0.0.1:2900`.
- **Debug Chrome** with `--host-resolver-rules` is not the user path; production
  is system PAC on 1086/1089.

---

## 7. Dead ends (do not re-chase)

- Token leak alone was not enough — flush bound + SMUX recycle were also required.
- "ss-server hung" with unread queues can be **effect**, not cause — check
  `rmt_wnd` / `read_parked` / `bucket` first.
- Docker on macOS cannot carry `--tcp` (tcpraw) — NAT drops fake TCP.
- Blanket `RUST_LOG=debug` on the VPS.
- IPv6-only DNS is not the YouTube HTML failure mode (VPS `curl -6` works).
- 2026-09-18 kernel-buffer / FEC / acknodelay / fast2 A/Bs — read that handoff
  before repeating.

---

## 8. Environment traps

- `pkill -f <pattern>` matches your own ssh command line — use anchored patterns.
- VPS python is 3.6; awk is mawk (`/proc/net/udp` is `%08X:%08X`).
- Heredocs inside ssh single quotes leak backslashes — write locally and `scp`.
- Cargo on the Mac needs a **login shell** (`zsh -lc`) for Homebrew PATH;
  `make linux` needs `PATH="/opt/homebrew/bin:$PATH"` for `x86_64-linux-musl-gcc`.
- GitNexus CLI under Node 16 is broken; use Node ≥20 or the MCP tools.
- Auto-worktree: do not edit `main` in place when another agent may be writing —
  this work lived in `kcptun-rs-yt-txfix` / `fix/tx-flush-timeout`.

---

## 9. Fast verification loop

```bash
# unit tests (worktree or main after merge)
zsh -lc 'cargo test -p kcp-rs --features async --lib'
zsh -lc 'cargo test -p smux-rs --lib'
zsh -lc 'cargo test --workspace'

# chain smoke — ss-local must already listen on 1086
curl -s -o /dev/null -w '%{http_code} %{time_total}\n' --max-time 25 \
  -x socks5h://127.0.0.1:1086 https://www.baidu.com
curl -s -o /dev/null -w '%{http_code} %{time_total} %{size_download}\n' --max-time 30 \
  -x socks5h://127.0.0.1:1086 -A "Mozilla/5.0" https://www.youtube.com/

# tunnel health
ssh root@38.145.210.53 'grep "session state" /tmp/kcptun.log | tail -3'
ssh root@38.145.210.53 'grep "watchdog: closing" /tmp/kcptun.log | tail -3'
grep "session state" /tmp/chain_client.log | tail -3

# deploy (from the branch that has the fix)
zsh -lc 'export PATH="/opt/homebrew/bin:$PATH"; make linux'
zsh -lc 'cargo build --release -p kcptun-client'
scp target/x86_64-unknown-linux-musl/release/kcptun-server \
  root@38.145.210.53:/tmp/x && ssh root@38.145.210.53 \
  'install -m 0755 /tmp/x /usr/local/bin/kcptun-server && systemctl restart kcptun'
# then copy client binary over target/release/kcptun-client and restart tmux `kcptun`
```

`make stress` after flush/lock/session changes (handoff requirement). E2E
(`make e2e`) still needs explicit user confirmation.

---

## 10. Change summary (this branch)

| File | Change |
|------|--------|
| `kcp-rs/src/conn/endpoint.rs` | `SendToken` RAII; `TX_FLUSH_TIMEOUT`; `send_drained_batch`; flush-loop drains use both |
| `kcp-rs/src/conn.rs` | regression `flush_tx_timeout_releases_token_and_requeues`; prior `cancelled_send_releases_the_token` |
| `kcptun-common/src/kcptun_session.rs` | write_loop stale reap via `smux.remove_stream` (token recycle) |
| `smux-rs/src/session.rs` | `return_tokens` on failed `push_data_bytes` (PSH/FIN) |

**AGENTS sync**: no public API renames/removals. `kcp-rs` pub surface unchanged
(published crate — still no breaking change). Common/smux behavior notes above
are captured in this handoff; nearest crate AGENTS already describe token-bucket
flow control — optional one-line tighten later, not required for correctness.

**Do not commit without** `detect_changes({scope:"compare", base_ref:"master"})`
when GitNexus is available. This session: tools down; reviewed by `git diff`
caller walk instead.
