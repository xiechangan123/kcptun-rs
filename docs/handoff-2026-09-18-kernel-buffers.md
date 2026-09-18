# Handoff — kernel socket buffers vs. the logical KCP windows (2026-09-18)

Audience: the next agent. Everything below was measured on the VPS
(38.145.210.53) or read from source during this session.

- **HEAD**: `753580be` + uncommitted work (this change is **not committed**).
- **Deployed**: VPS `/usr/local/bin/kcptun-server` = the new build
  (`c482d74a…`), unit `/etc/systemd/system/kcptun.service`, restart 02:36:09Z.
  Backups: `kcptun-server.bak-20260918`, `kcptun.service.bak-buf-20260918`.

---

## 1. What the container actually gives you

The VPS is OpenVZ-style (`venet0`, `bugetvm`, container-locked sysctls):

| fact | value |
|---|---|
| `net.core.rmem_max` / `wmem_max` | **133120**, and `sysctl -w` → `permission denied` (host-owned) |
| `SO_RCVBUF = 10 MB` → effective | `266240` (`getsockopt`, i.e. the kernel's *doubled* accounting value) |
| datagrams that fit in that queue | **173 × 1200 B = 207,600 B**, then silent drops |
| `SO_RCVBUFFORCE = 2 MB` → effective | **4194304** ✅ (container root holds CAP_NET_ADMIN) |
| `Udp.RcvbufErrors` on this box | **stays 0 while packets are dropped** |
| `Udp.InErrors` on this box | **is the counter that moves** (verified: +1662 == exactly the 1662 packets a burst test dropped) |

The last two rows matter more than they look: the previous handoff's "no buffer
drops, RcvbufErrors=0" reading was wrong for this host.

The mismatch being fixed: the server's send window is `sndwnd × mtu ×
(ds+ps)/ds` = `512 × 1200 × 1.2` = **737 KB**, and its advertised receive window
is `rcvwnd × mtu × 1.2` = **368 KB**, against a 208 KB kernel queue. Whenever the
1-vCPU worker stalls (the earlier handoff measured p90 85 ms / max 345 ms
stalls), the queue overflowed and the excess was dropped — invisible in the
service's own counters, visible to KCP only as loss it must retransmit.

## 2. The change

`knet-rs/src/net/sockbuf.rs` (new, public API `knet::set_socket_buffers` /
`knet::SocketBuffers` / `knet::net::sockbuf::describe`):

1. plain `SO_RCVBUF` / `SO_SNDBUF`,
2. read back what the kernel granted,
3. on Linux, retry a clamped direction with `SO_RCVBUFFORCE` /
   `SO_SNDBUFFORCE` (best effort — EPERM just leaves the plain value).

Wired into both binaries' socket setup (`kcptun-{server,client}/src/socket.rs`),
and the startup line now reports the truth instead of the request:

```
sockbuf: requested=2097152 effective recv=4194304 send=4194304
```

`--sockbuf` on the VPS was `10485760` (silently 266 KB); it is now `2097152`
(→ 4 MB granted). `--smuxbuf` went `10485760` → `4194304` so the session window
matches the socket buffer (SMUX `verify` requires `streambuf ≤ smuxbuf`;
`--streambuf` stays at its 2 MB default). Everything else in the unit is
unchanged — `--rcvwnd 256`, `--sndwnd 512`, `--mtu 1200`, FEC 10/2.

## 3. Verification (measured after the restart)

Burst ladder against a socket configured the way the server configures its own
(`/tmp/bufprobe2.py`, recreated in this session):

| mode | effective rcvbuf | burst (1200 B pkts) | received | dropped |
|---|---|---|---|---|
| plain `SO_RCVBUF` | 266,240 | 307 | 173 | **134** |
| `SO_RCVBUFFORCE` | 4,194,304 | 307 | 307 | 0 |
| `SO_RCVBUFFORCE` | 4,194,304 | 1200 | 1200 | 0 |
| `SO_RCVBUFFORCE` | 4,194,304 | 2500 | 2500 | 0 |

307 packets is exactly what `--rcvwnd 256` with FEC 10/2 puts on the wire.

Live traffic, 75 s window after the restart: `Udp.InDatagrams` +1668 while
`Udp.InErrors` stayed flat at 41230 (which is 41096 + the 134 the plain-mode
probe deliberately dropped). Server RSS 17.5 MB, service `active`, zero WARN
lines, 3 new sessions, and the client re-dialed on its own after the restart.

## 4. Ops notes

- **Watch `Udp.InErrors`, not `Udp.RcvbufErrors`, on this host.**
  `awk '/^Udp:/{getline; print $2, $4, $5, $6}' /proc/net/snmp` prints
  `InDatagrams InErrors OutDatagrams RcvbufErrors`.
- Rollback: `kcptun-server.bak-20260918` + `kcptun.service.bak-buf-20260918`,
  then `systemctl daemon-reload && systemctl restart kcptun`.
- The clamp warning is best effort: if a future host drops CAP_NET_ADMIN the
  startup line will say `(clamped by net.core.{r,w}mem_max)` and the effective
  value will be the small one — the config is then still safe, just tighter.

## 5. Open items

1. **The client is not updated.** Its `--sockbuf 10485760` / `--smuxbuf 10485760`
   still exceed the 8 MB macOS ceiling (that one *is* generous, so nothing
   breaks). Rebuild + restart it with `--sockbuf 4194304 --smuxbuf 4194304` to
   make both ends consistent.
2. **Host-level drops are still invisible.** The container's counters only cover
   its own queues; what the host does to a burst on the China-bound link cannot
   be seen from inside. If `RetransSegs` stays high with `InErrors` flat, the
   loss is downstream of the container, and the lever is the window
   (`--sndwnd`), not the buffer.
3. One session was closed by the ack-stall fast path 2 minutes after the
   restart (`ack_stalled=true, peer_restart_seen=true, write_blocked_ms=11001,
   rmt_wnd=256, bucket=4185645`), and the client re-dialed 1 s later. That is
   the designed path after a server restart, but note `write_blocked_ms` being
   large: with `--smuxbuf 4 MB` the server now applies backpressure sooner than
   it did at 10 MB. Re-check whether that costs downlink throughput before
   treating 4 MB as settled.
4. Previous session's parameter advice still unapplied and still worth an A/B:
   `--mode fast3` (measured +20–25% over `fast` on this path),
   `--rcvwnd 512` (uplink headroom — now safe, 737 KB ≪ 4 MB queue), and
   `--conn 2..4` on the client (per-flow shaping across the 11 listen ports).

---

## 6. FEC A/B — the path is loss-recovery-limited, not byte-quota-limited

Setup: a second kcptun pair on spare ports (50300-50303) with a plain HTTP target
on the VPS (`python3 -m http.server 18080` serving a 20 MB `/tmp/abtest.bin`), so
the measurement contains no internet path and no ss-server. Client flags mirrored
production (`--mode fast3 --sndwnd 512 --rcvwnd 1024 --mtu 1200 --acknodelay
--sockbuf 4194304 --smuxbuf 4194304`), servers differed **only** in FEC. Three
interleaved rounds of 20 MB per arm, order rotated, medians:

| FEC | samples (MB/s) | median |
|---|---|---|
| off (0/0) | 0.83 / 0.77 / 0.75 | 0.77 |
| 10/2 | 1.31 / 1.12 / 1.17 | 1.17 |
| **10/3** | 1.25 / 1.25 / 1.26 | **1.25** |
| 10/4 | 1.01 / 1.08 / 1.02 | 1.02 |

**Reading:** dropping FEC costs ~35 % of goodput, and one extra parity shard
beats both the 20 % and the 40 % variant. The earlier inference that "FEC's 20 %
overhead is 20 % of a byte-quota path" is therefore **wrong for this path**: the
ceiling is set by loss-recovery stalls (each unrecovered loss costs an RTO of
~200–400 ms on a 190 ms RTT), not by bytes. Paying overhead to avoid stalls wins
until parity stops buying recovery (10/4).

Applied to production the same day: both ends `--datashard 10 --parityshard 3`
(the Go default), verified by the parity/data ratio in the SNMP counters on both
directions (server uplink 0.30, client downlink 0.27–0.28) plus a working
handshake; 0 session reconnects in the 4+ minutes after the restart.

Two corrections to advice given earlier in the same session:

- **Keep `--acknodelay`.** ACKs are flushed once per RX batch (≤ 32 datagrams,
  `RECV_BATCH` / `READ_PREFETCH_MAX_MESSAGES`), not per segment, so it does not
  inflate packet count — and it removes the interval-long ACK delay.
- **Do not turn FEC off.** Measured above.

Still on the table, now with numbers: the **client's uplink retransmit rate is
~23 %** (`RetransSegs/OutSegs` in the client's own SNMP log) while the server's
downlink retransmits ~5–7 %. The uplink itself is clean (`FECRecovered` ≈ 0), so
those are spurious RTOs triggered by ACK loss on the throttled downlink — the
next thing worth attacking (levers: `--resend`, the nodelay/interval pair, or
making the server's ACKs more robust). `--snmplog` is now enabled on the
production client (`/tmp/client_snmp.log`) so this is measurable.

Harness leftovers for the next round: `/tmp/abtest.bin` on the VPS and
`/tmp/ab3.sh` on the Mac (the arm clients/servers themselves were stopped).


---

## 7. Rust vs Go under the optimal parameters (same day, same path)

Go binaries: `tests/kcptun-go/{client,server}` are macOS arm64, so the **server**
came from the Linux VM (`192.168.0.84:/root/kcpbench/go-server`, static x86-64,
copied to the VPS as `/tmp/go-server`); the client is the repo's macOS build.
Both arms ran the same flags — `--mode fast3 --mtu 1200 --sndwnd 512 --rcvwnd
512` (server) / `1024` (client), `-ds 10 -ps 3`, `--smuxbuf 4194304
--sockbuf 4194304 --acknodelay`, and (in the final run) `--nocomp` — against the
same VPS-local HTTP target, 20 MB per download, interleaved with rotated order.

| arm | samples (MB/s) | median |
|---|---|---|
| **Rust (optimal)** | 1.31 / 1.37 / 1.24 / 1.31 | **1.31** |
| **Go (same flags)** | 0.91 / 0.81 / 0.74 / 0.72 | **0.78** |

**Rust is ~68 % faster** end to end on this path under this parameter set.

Crossover run (4 combos, 3 rounds, compression on) to attribute it:

| client → server | median |
|---|---|
| Rust → Rust | 1.13 |
| Go → Rust | 1.02 |
| Rust → Go | 0.84 |
| Go → Go | 0.76 |

So the **server implementation carries most of the gap (~+35 %)**, the client
~+10 %. Two candidate explanations were eliminated: the socket buffer is not it
(a Rust arm pinned to Go's clamped 266 KB matched the 8 MB arm, 1.13 vs 1.14),
and neither side is CPU-bound (Rust 1.35 s and Go 2.00 s of CPU per 20 MB, each
~8 % of the one core; the VPS is a Xeon E5-2620 v2 *with* AES-NI, so crypto is
not the differentiator). Go does burn ~48 % more CPU per MB, which is worth
knowing but does not by itself explain the throughput gap. Root cause is still
open — the next step is per-arm SNMP (`RetransSegs`/`LostSegs`/`FECRecovered`)
to see whether Go retransmits more or recovers less.

This contradicts the earlier handoff note ("Go ~10 % faster on this lossy
path"): that was measured under a different parameter set (`--mode fast`,
`sndwnd/rcvwnd 1024`, `--nocomp`). The ranking is parameter-dependent; under the
current optimal set Rust wins decisively.

**`--nocomp` is worth +16 %** on incompressible data (1.24 vs 1.07 MB/s, same
Rust stack, 3 interleaved rounds). It is not applied to production yet: real
browsing mixes compressible HTML/JS with incompressible video, and the test file
was random, so this needs a look at real traffic before switching. Remember it
must be set on **both** ends.

Harness leftovers: `/tmp/go-server` on the VPS (13 MB, static), `/tmp/abtest.bin`
(20 MB) on the VPS, `/tmp/ab*.sh` on the Mac.

---

## 8. Root cause of the Rust-vs-Go gap: `--acknodelay`

The 68 % gap in §7 was an artefact. Isolating one flag at a time (same path,
20 MB per download, interleaved, `--nocomp` everywhere):

| stack | `--acknodelay` | samples (MB/s) | median |
|---|---|---|---|
| Rust | on | 1.54 / 1.36 / 1.44 / 1.37 | 1.41 |
| Rust | **off** | 1.71 / 1.63 / 1.65 / 1.62 | **1.64** |
| Go | on | 0.77 / 0.79 / 0.73 | 0.77 |
| Go | **off** | 1.28 / 1.75 / 1.47 | **1.47** |

Head-to-head with both stacks on the best settings: **Rust 1.70 vs Go 1.46 MB/s
(medians of 4 rounds) → Rust ~16 % faster**, not 68 %.

**Mechanism.** `--acknodelay` makes the receiver ACK every segment instead of
batching. Go takes that literally — the Go server received 980 inbound
segments/s while sending 1068/s (≈ 1 ACK packet per data segment, 3× the Rust
client's rate, which still coalesces per RX batch). The extra uplink packets
cost uplink loss, so the sender's RTO fires for data whose ACK was lost:
Go declared **36.6 % of its outbound segments lost** (Rust 22.4 %), retransmitted
**42.9 %** of everything it sent (Rust 22.6 %), and burned the downlink on
retransmits. Rust's acknodelay path is far less harmful (+16 % vs Go's +91 %),
which is why the gap looked like an implementation difference.

Applied to production: the client's `--acknodelay` is **removed** (server never
had it). This also retracts the earlier advice in §6 to keep it — the code
reading said ACKs batch per RX batch, the measurement says the resulting uplink
load still costs far more than the ≤ interval ACK delay it saves.

### Bug found while measuring: `OutPkts` counts the wrong thing

The Rust port increments `DEFAULT_SNMP.out_pkts` in the KCP output callback
(`kcp-rs/src/conn.rs:1299`), i.e. **once per KCP segment, before FEC expansion**.
Go counts datagrams actually handed to the socket. With FEC 10/3 the difference
is visible in the CSV: Go's server reports `OutPkts/OutSegs = 1.29`, the Rust
server reports `1.00`. Parity *is* sent — the Rust client's own counters show
`InPkts/InSegs = 1.23` and `FECParityShards/InSegs = 0.28` — so this is a
counter-semantics deviation in a column that is supposed to be Go-compatible,
not a behaviour difference. Fix: increment where the datagram is written
(`flush_tx_batch`/`send_packets`), not in the KCP callback.

---

## 9. Mode A/B: fast3 wastes bandwidth, fast2 is the pick

The live server log showed the waste directly — in the last two production
windows before the change, `RetransSegs/OutSegs` was **66 %** with
`LostSegs ≈ RetransSegs` and `FastRetransSegs = 8`, i.e. RTO-driven
retransmission, not fast retransmit.

Three arms, same flags (`--mtu 1200 --sndwnd 512 --rcvwnd 512/1024 -ds 10 -ps 3
--nocomp --sockbuf 4194304 --smuxbuf 4194304`, no `--acknodelay`), 20 MB per
download, 3 interleaved rounds with rotated order, server SNMP deltas per
download:

| mode | goodput median | retransmits | wire packets / MB payload |
|---|---|---|---|
| fast3 (nodelay 1, interval 10) | 1.35 MB/s | **20.0 %** | **1499** |
| **fast2 (nodelay 1, interval 20)** | **1.56 MB/s** | **12.0 %** | **1364** |
| fast (nodelay 0, interval 30) | 1.55 MB/s | 12.4 % | 1371 |

fast3 costs ~10 % more wire packets for the same delivered payload **and** is
13 % slower: the extra retransmits eat the policed path instead of buying
recovery. The mechanism is the backoff: with `nodelay = 1` KCP grows a
segment's RTO by half the current estimate (`seg.rto += rx_rto / 2`), with
`nodelay = 0` by the whole one — so on a path that really loses packets, fast3
re-sends each lost segment roughly twice as often.

fast2 and fast tie on goodput and efficiency; fast2 wins on interactivity
(`nodelay = 1` flushes immediately and floors the RTO at 30 ms instead of 100).
**Production switched to `--mode fast2` on both ends** the same day.

This also retracts the earlier advice in §6 ("`--mode fast3` — measured +20–25 %
over `fast`"): that measurement predates FEC 10/3, the socket-buffer fix and the
acknodelay removal, and it did not look at retransmits at all.
