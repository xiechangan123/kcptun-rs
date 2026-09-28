# Handoff：feat/session-task-listener

日期：2026-09-24。工作目录是 `/Users/yangzhiqin/Desktop/kcptun-rs-session-task`，
分支 `feat/session-task-listener`。不要在 `/Users/yangzhiqin/Desktop/kcptun-rs`
（main，`8e8f8760`）里改代码，那个检出只用来编 main 的对照二进制。不要合并。

## 目标

把 `KcpListener` 从「自建 OS 线程 + 每线程一个 runtime」改成「调用方 tokio runtime
上的 task」：一个 rx task 收包分流，每条 `KcpStream` 自己跑 KCP。已经做到并能
过测试。没做到的是性能：在 4 核空载的 Linux 上，新实现比 main 慢，延迟差一个
数量级。

## 已提交（main..HEAD，从新到旧）

| 提交 | 内容 |
|---|---|
| `ede5a01f` | `close()` 丢掉还在建或未 accept 的连接；rx task 留到 Drop；关闭的队列不再入队 |
| `1f4d4e72` | `sweep` 扫整张会话表，去掉 `take(4096)` |
| `2f6abf6a` | `close()` 不再取消 rx/tx，已 accept 的连接继续跑 |
| `23ff7a83` | 删掉旧 worker 的 `feed_*`（后来又加回一个，见未提交部分） |
| `6ce0875b` | 共享 socket 一个 tx task 等 `writable()`；准入门解密副本、入队密文；`listener_crypto_gate` 三条测试 |
| `5bb94cea` | 建连先插 `sessions` 再删 `building`，避免一次握手拆成两个会话 |
| `5bc0b566` | 主体：删掉 `thread::Builder`、三套拓扑、`worker_count` |

## 未提交（必须先看）

`git status` 有 6 个已修改文件，都没有提交：

- `kcp-rs/src/conn.rs`、`conn/endpoint.rs`：新增 `KcpStream::feed_raw_single`，
  转发到 `SharedIoState::feed_raw_single`。当场解密、`process_inbound_batch`、
  `drain_and_flush_tx`。`drain_and_flush_tx` 和 `spawn_send_remainder` 的
  `#[cfg(test)]` 已去掉，生产构建也编译它们。
- `kcp-rs/src/kcp.rs`：新增 `KCP::flush_acks_only`，只把 `acklist` 编进
  数据报发出去，不扫发送缓冲区。
- `kcp-rs/src/conn/endpoint.rs`：`process_inbound_batch` 在 `acknodelay` 关闭且
  `acklist` 不空时调用 `flush_acks_only`。flush loop 里「还没有截止时间就预约
  一个 interval 再返回」的分支也删了，那次唤醒现在直接刷。
- `kcp-rs/src/sharded.rs`：已建立会话的数据报不再 `queue.push`，rx task 直接
  调 `conn.feed_raw_single`。只有 `building` 中的会话还走队列。
- `kcp-rs/src/transport.rs`：tx 通知、关闭队列拒绝入队（已随 `ede5a01f` 提交
  了一部分，工作区里还有后续改动，提交前用 `git diff` 看）。
- `kcp-rs/tests/kcpstream_listener.rs`：新增 `listener_close_drops_unaccepted_connections`、
  `listener_close_discards_inflight_build`、`remove_peer_drops_unaccepted_connection`、
  `sweep_reaps_closed_sessions_without_remove_peer`（1000 会话，不是最初的 5000）。

工作区里还有几份未跟踪的 `bugs/BUGREPORT_*.md` 和 `docs/session-task-*.md`，
是审核记录，不是这次的代码。提交代码时不要带上它们，除非用户明确要求。

## 性能：已知数字

机器 `root@192.168.0.18`，4 核，30GB，测试时负载约 0.3 起步、跑起来到 2–6。
二进制在 `/tmp/kcpbench-cmp/{old,new}`，端口用 45100 以上。**不许杀这台机器上
已有的服务**，只许动 `/tmp/kcpbench-cmp/` 下自己拉起的进程。脚本是
`/tmp/kcpbench-cmp/cmp.sh`，结果在 `cmp.out`、`cmp2.out`、`cmp3.out`。

参数：`--crypt aes --nocomp --mode fast --sndwnd 1024 --rcvwnd 1024 --smuxver 2`，
客户端 `--conn 8`，32 条 TCP 连接、每条 8MB、超时 60 秒。吞吐是 32 路合计。

32 连接，三轮交替，main 对 `ede5a01f`（**不含**下面的未提交改动）：

| | main MB/s | 新 MB/s | main 延迟 | 新延迟 |
|---|---|---|---|---|
| 中位数 | 79.95 | 56.28 | 0.23 ms | 9.00 ms |

单连接（同参数，`--connections 1`）也复现：main 0.17–0.21 ms、50–63 MB/s；
新实现 6–13 ms、37–45 MB/s。所以不是 32 路互相抢。

同一份未提交代码在这台 Mac 上单连接是 **0.27 ms、49 MB/s**，和 main 持平。
差距只出现在那台 Linux（内核 3.10，没有 bpftrace、tcpdump、strace）。

## 已经排除的原因

用 `RUST_LOG=warn` 临时打点测过，测完已从代码里删掉，不要再找那些日志：

- `process_inbound_batch` 里 KCP 计算本身不到 2ms（`inbound flush took` 从未触发）。
- `try_send_batch` 不到 1ms（`try_send took` 从未触发）。
- `send_drained_batch` 在服务端是 1–2ms，客户端偶发 6–40ms。
- 从 `recv` 返回到发送完成是 13–17ms。差的那一段是收包任务把数据报放进
  `PeerQueue`、再唤醒会话自己的 input task 的那一次调度。在这台机器的
  timer 粒度上，一次跨任务唤醒就是 6–13ms。main 不存在这次唤醒：worker
  线程在 `feed_raw_batch` 里同步跑完 KCP 并把 ACK 送进 socket。

`acknodelay` 默认关。ACK 只在 `flush_with_current(current, true)` 里发出，
而那个函数的返回值初始是 `self.interval`（fast 模式 30ms），flush loop 再把它
钳到 `ACTIVE_UPDATE_MAX_MS = 10`（`kcp-rs/src/conn.rs:71`）。所以「等 flush
loop 的下一次」至少是 10ms。未提交的 `flush_acks_only` 就是为了绕开这件事：
一个数据报装得下 `mtu/24` 个 ACK（默认 MTU 约 56 个），发送代价是一次
`sendmmsg`，大约一微秒，不值得为了攒批等 10ms。攒满一个数据报的情况本来就由
`flush_if_pending` 处理（`acklist` 达到 `mtu/24` 时 `pending_flush` 置位）。

## 未提交改动想做的事，以及为什么还没验证完

`sharded.rs` 的 `deliver` 对已建立会话直接调 `conn.feed_raw_single`，让 rx task
当场跑 KCP 并发 ACK，不再经队列唤醒 input task。建连中的会话仍走队列。

这一步**还没有在 Linux 上重新测过**。上次测的 `new` 二进制是 12:48 编的，早于
这次 `deliver` 的改动。本机测到 0.27ms 的那次也早于它。下一步第一件事就是用
这份代码重新交叉编译再测。

## 当前测试状态

最后一次跑 `cargo test -p kcp-rs --features async --test kcpstream_listener`
（已包含 inline feed）结果是 **15 通过，1 失败**。失败的是
`listener_close_does_not_hang_an_inflight_write`，断言在大约
`kcpstream_listener.rs:480`：`close()` 之后一个 8MB 的 `write_all` 没有在
2 秒内结束。

这个测试的前提是对端有人把数据读走，发送窗口才开得了。`_drain` 任务在做这件事。
失败可能是 inline feed 之后 rx task 持有 KCP 锁的时间变长，和 `write_all`
抢同一把锁；也可能是测试本身又被改过。先单独跑它看断言，不要直接加长超时
掩盖。

`sweep_reaps_closed_sessions_without_remove_peer` 曾经把 5000 个会话卡死：
sweep 每秒一次，但一次要逐个拿 KCP 锁，5000 个锁比 1 秒还长，而且活着的客户端
会用探测包刷新 `last_activity_ms`，idle 规则永不触发。现在是 1000 个会话、
先 `drop(clients)` 再等回收。如果有人把它改回 5000，会再卡。

加密路径有 `kcp-rs/tests/listener_crypto_gate.rs` 三条测试（echo、坏包拒绝、
双客户端）。准入门必须解密**副本**、入队**密文**，input loop 再解一次。
不要改回「解密后入队」。

## 下一步

1. 在 worktree 里 `cargo test -p kcp-rs --features async --test kcpstream_listener listener_close_does_not_hang -- --nocapture`，
   看这个失败是锁争用还是测试写错。修到 16 个测试全过。
2. `make linux` 交叉编译，scp 到 `192.168.0.18:/tmp/kcpbench-cmp/new/`，
   只覆盖 `new`。`old` 是 main，不要重编覆盖，除非确认它不是 `8e8f8760`。
3. 先跑单连接：`throughput.py --data-mb 8 --connections 1 --latency-iterations 30 --timeout-seconds 30`，
   新旧各两次，交替。端口从 47000 往上加，避开 `ss -lntu` 里已有的。
   目标是新实现延迟回到 1ms 以内、吞吐接近 main 的约 50 MB/s。
4. 单连接达标后再跑 `bash /tmp/kcpbench-cmp/cmp.sh`（32 连接那套）。
5. 达标才提交。提交只含 `kcp-rs/src` 和 `kcp-rs/tests` 里这次的文件，
   不要带 `bugs/` 和那些审核文档。不要合并到 main。

如果单连接在 Linux 上仍是数毫秒，就不要再调 ACK 阈值。那次开销是 rx task 把
包交给另一条 task 的调度，inline `feed_raw_single` 就是针对它的；它已经在
工作区里，缺的是一次干净的测量。
