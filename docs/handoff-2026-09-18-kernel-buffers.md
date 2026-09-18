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
