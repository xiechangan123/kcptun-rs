# BUGREPORT — feat/session-task-listener: 闭环 RPS 回归 + 32 连接 timeout（误杀活会话）

Date: 2026-09-24
Affects: `feat/session-task-listener` 分支（相对 `main` 8 个提交，核心为
`5bc0b566` drive listener sessions as tasks / `6ce0875b` one tx task / `23ff7a83`
drop the worker feed path / `1f4d4e72` reap every session on each sweep）。
Scope: `kcp-rs/src/sharded.rs`（重写 1972→990 行）、`kcp-rs/src/conn/endpoint.rs`、
`kcp-rs/src/transport.rs`、`kcp-rs/src/conn.rs`、`kcp-rs/src/kcp.rs`（`flush_acks_only`）。
Type: 性能回归（吞吐）+ 可用性 bug（过载下驱逐活会话）。

---

## 一、测量事实（bench/run_p99.sh，macOS arm64，闭环 concurrency=32）

| 组合 | 09-16 (main) | 09-24 (feat) | 变化 |
|---|---:|---:|---|
| kcp-rs↔kcp-rs 闭环 RPS | 199,640 | 47,540 | −76%（原始 4.2×） |
| kcp-go↔kcp-go 闭环 RPS（对照组） | 94,091 | 48,554 | **−48%** |

**归因甄别**：Go↔Go 的二进制与 `run_p99.sh` 在两分支间零改动，却掉了一半 ⇒ 跨 8 天的
绝对值不可直接比较（`run_p99.sh` 无负载守卫；参见 9/3 bench 噪声教训）。用 Go 对照归一化：

- 环境归因：×0.516（= 48,554 / 94,091）
- 代码不变时 Rust 理论值 ≈ 199,640 × 0.516 ≈ **103k**
- 代码归因降幅 ≈ 103k → 47.5k ⇒ **−54%（≈2.2×）**；另一半是机器状态。

同次运行内 Rust(47.5k) vs Go(48.5k) 已打平；09-16 时 Rust 是 Go 的 2.1×。
开放模型（固定 500 RPS）P50 156.9→93.8µs、P99 560.5→369.6µs **反而变好**——
该分支是有意的"吞吐换延迟"取舍，但下列 ③ 是纯 bug、①② 的代价被放大了。

---

## 二、RPS 回归的代码侧根因（按影响排序）

### ① RX 路径批量处理被整体删除 — 最大根因

- **main**：RX 线程一次排空最多 `DRAIN_QUANTUM=1024` 包（5ms 配额）；worker 按 peer
  分组后每个 peer 只调**一次** `feed_raw_batch`（WORKER_BATCH=64）：整批一次 KCP mutex、
  一次 `process_inbound_batch`、一次 flush、一次 `sendmmsg`。
- **feat**：`feed_raw_batch`/`feed_batch` 从 conn.rs 删除，唯一入口 `feed_raw_single`。
  每个数据报独立走：sessions map 锁 → 解密 → KCP mutex → `process_inbound_batch([1])`
  → `flush_if_pending` + `flush_acks_only` → `drain_and_flush_tx`（一次 sendto）。

闭环饱和 ≈10 万 pps 时，等于每秒多出十万级 mutex/flush/sendto（loopback 单次
sendto ≈1–2µs），单 RX task 被逐包开销封顶。且新 `RECV_BATCH=32`/唤醒（旧 1024）。

### ② RX/worker 双线程流水线合并为单 task

main：专用 RX OS 线程 + worker OS 线程（各自 current-thread runtime）并行，收包系统调用
延迟被 KCP 处理隐藏。feat：**一个 RX task 串行**完成 recv → demux → 解密 → KCP → ACK 发送。
这是有意的延迟取舍（commit 注明：排队给 session input task 的调度跳变在粗定时轮上实测
6–13ms = 整个 fast-mode RTT；内联换来了开放模型 P50/P99 改善），但吞吐代价见 ①。

### ③ `spawn_tx` 忙旋 — 纯 bug（sharded.rs `spawn_tx`）

```rust
loop {
    if race(stop.cancelled(), socket.writable()) == stop { notify_waiters(); break; }
    ready.notify_waiters();
    knet::yield_now().await;   // = tokio::task::yield_now，纯重排程，不 park
}
```

UDP socket 有缓冲空间时 `writable()` 每次 poll 立即 Ready（readiness 只在 WouldBlock
发送后被清除）⇒ 从第二轮起该 task **全速空转 notify_waiters + yield_now**，listener
存活期内永久占死一个 runtime worker（yield_now 只把任务重排到本 worker 队尾，随即再次
执行），并向同 runtime 上的 client/echo/flush 任务倾倒排程抖动。空转、常态化、偷一个核。

### ④ 每次 RX 唤醒新建定时器

热循环每批重建 `knet::timeout(SWEEP_INTERVAL, race(stop, recv_from))`：每 32 包注册+注销
一个 1s Sleep（饱和时每秒数千次 timer wheel 操作），外加每批一次 `yield_now`、一次
`evict_stale`（HashMap 分配）、一次 `sweep`（克隆全部 session 句柄）。

### ⑤ flush 纪律放宽（endpoint.rs flush loop）

notify 唤醒不再遵守已设 deadline、立即整轮 flush（旧逻辑 deadline 未到直接 continue）；
acknodelay=false 新增 `flush_acks_only` 补发。延迟赢，吞吐输：每轮交互多出多次拿 KCP 锁
的 flush pass，饱和时进一步摊薄。

### ⑥ 每 session 新增后台任务与逐包锁

`deliver()` 不再传 `background_input(false)`（conn.rs 默认 true）：每个被接受 session 在
调用方共享 runtime 上多挂 input loop + flush loop；而 RX 内联喂入后 per-session input
loop 永远空转在无人 push 的 queue 上（仅 building 窗口用到 queue）。另每包过
building/sessions/pending 多次 `parking_lot::Mutex`（无竞争但逐包累加）。

---

## 三、32 个 UDP 连接时 timeout 的机制链（可用性 bug）

### 结论

timeout 不是单一 bug，而是**正反馈链**：新架构把 main 上"整 burst 全 stale 才驱逐"的
守卫打穿成"单个 stale 包即驱逐"，在 32 连接打满共享 socket 时形成
**误杀活会话 → 反复驱逐重建 → 客户端读超时**的循环。

```
32 条会话共享 1 个 listen socket（1 个内核发送缓冲 + 1 个 rx task）
  → server→client 方向被尾丢弃打洞（某些流的 ACK/回包全丢，客户端 una 冻在 0）
  → 客户端 RTO 重传 sn=0 且 una=0 的首段
  → 服务端会话 rcv_nxt ≥ 16 ⇒ peer_restart 计数
  → 逐包喂入，burst 恒为 1 ⇒ delta(1) ≥ burst(1) ⇒ stale_bursts++
  → evict_stale 判定"对端重拨"，关闭活会话（黑洞）
  → 按地址重建新会话，过载未缓解 ⇒ 再次攒到 rcv_nxt≥16 ⇒ 再次驱逐
  → 停滞超过客户端读超时（bench 闭环 10s 即判 "connection dead"）⇒ timeout
```

### 关键证据：守卫语义被改掉

**main** — `sharded.rs:1542 stale_peer_signal(conn, batch_len, ...)`：驱逐条件
`delta ≥ batch_len`，即该 peer 本 burst 内**全部**数据报 stale 才驱逐。批量喂入时
一条混进来的迟到 sn=0 重传被新鲜数据稀释，不驱逐。`process_inbound_batch` 注释
"one stale datagram inside a burst of fresh data does not count" 即此语义。

**feat** — 两处叠加废掉守卫：

1. 唯一喂入路径 `feed_raw_single` ⇒ burst **恒等于 1**，`delta ≥ burst` 退化为
   "单个 stale 包就计数"（endpoint.rs:1129-1139）。
2. `evict_stale`（sharded.rs:881）对整个批次窗口取 `min(before)` 再比较
   `stale_burst_count() > min(before)`：窗口内**任意一条**数据报触发 restart/mismatch，
   哪怕同窗口喂了几千个新鲜包，计数也必然大于窗口起点 ⇒ 驱逐。
   逐包喂入 + min 聚合 = 守卫只剩"见过一个 stale 包"。

触发信号本身（endpoint.rs:1180）要求 `sn==0 && una==0 && rcv_nxt≥16`
（`RESTART_MIN_RCV_NXT=16`）。`una==0` = 客户端**从建连起没收到过我们任何包**。正常运行
的会话 una 会前进，平时不触发；但 32 连接把共享发送缓冲打满时，按流尾丢弃可让某些流的
server→client 方向整段全丢（客户端 una 冻在 0、服务端 rcv_nxt 照常增长）——正是设计外
误触发面。低并发不复现、32 连接必现的原因即此。

### 为什么 32 连接是临界点

- **1 个内核发送缓冲**：所有会话的回包/ACK 从同一 listen socket 出去
  （`SharedSendLock` 注释自证 buffer 是 socket-wide）。32 路回包突发 → 持续
  WouldBlock/尾丢弃 → 单流方向黑洞常态化。
- **1 个 rx task**：每包完整走 解密→KCP 锁→flush→sendto（根因 ①），32 连接聚合包速
  已压到饱和；内核接收缓冲堆积 → 丢包 → 重传风暴 → 恶化。
- **spawn_tx 忙旋**（根因 ③）白烧一个 runtime worker，让上面两条更早到临界点。
- 驱逐后重建不能自愈：过载还在，新会话很快又攒满 rcv_nxt≥16，再被同一条链驱逐；
  客户端在某个窗口 10s 读不到回包即超时。

### 次要放大器

- `TX_READY_WAIT=50ms` fallback：`notify_waiters` 无 permit 存储，错过的通知等满 50ms；
  当前被 tx 忙旋高频重试掩盖，修 ③ 后会暴露成独立的 stall 源。
- listener `close()` 后 tx task 退出（`break`），残留会话的每次 WouldBlock 发送都付 50ms。
- sweep 在有流量时每批跑一次（克隆全部 session 句柄），会话多时逐批线性开销。

---

## 四、修复建议（按优先级）

1. **恢复 burst 守卫语义**（最小改动、直接掐断误杀链）：`deliver` 先按 peer 归组本批
   数据报，整批喂 `feed_raw_batch` 风格路径（`process_inbound_batch` 本就收 slice）；
   `evict_stale` 改为对比"本批该 peer 的 stale 增量 ≥ 该 peer 包数"。
2. **给 restart 信号加置信度**：una==0 && rcv_nxt≥16 在单向黑洞下完全合法；驱逐前要求
   连续 N 个独立窗口全 stale，或伴随 conv 变化（更强的重拨证据）。
3. **修 `spawn_tx` 忙旋**：仅在 sender 实际 WouldBlock 时 arm waker
   （AtomicWaker/信号量模式），tx task 醒来 notify 一次后重新 park。
4. **恢复批量摊销**（与 1 同一件事的两个目标）：既保住内联零跳变的延迟收益，又拿回
   每 burst 一把 KCP 锁/一次 flush/一次 sendmmsg。
5. 1s sweep 定时器移出热循环（持久 Sleep 或到点才 race）；`RECV_BATCH` 提回 256+ 或恢复
   drain quantum；sweep 只在空转时跑。
6. listener 会话显式 `background_input(false)`（或删除 per-session input task）。
7. **bench 卫生**：给 `run_p99.sh` 加 `run_bench.sh` 同款负载守卫（1-min load > 0.7×cores
   中止）；跨日绝对值必须用 Go↔Go 对照归一化，否则 09-16 与 09-24 的对比会得出 4.2× 的
   虚高结论。

## 五、验证路径

- 32 个客户端 socket 对 1 个 listener 回声压测；观察 `KcpListener::stats()` 的
  `unauthenticated_drops` 与日志 `listener: evicting stale session for {peer}`——
  过载中该日志高频出现即为误杀实锤。
- 对照实验：临时调大 `RESTART_MIN_RCV_NXT`（如 16→4096），timeout 应显著缓解。
- 修复后回归：`make test` + kcp-rs `--all-features` 集成测试
  （`kcpstream_listener.rs` / `listener_crypto_gate.rs`）+ `make stress`。

---

## 六、修复记录（2026-09-24，worktree `/Users/yangzhiqin/Desktop/kcptun-rs-session-task`）

### 6.0 测量修正：代码归因上调

当日 14:36 用 **main 代码**重跑同一脚本：Rust 闭环 **87,618**，Go 对照 47,316。
两次运行相隔 14 分钟、Go 对照稳定（48,554 / 47,316）⇒ 环境稳定，**feat 分支的真实
代码归因 = 87,618 → 47,540 ≈ −46%（≈2.2×）**，与第五节归一化估算一致。
（第一节的"环境归因一半"适用于跨日对比 09-16 vs 09-24；同日对比代码归因更高。）

### 6.1 已落地的修复

| # | 修复 | 位置 |
|---|---|---|
| ①④ | RX 按 peer 批量归组（保序），恢复 `feed_raw_batch`：每 burst 每 peer 一次解密遍、一把 KCP 锁、一次 flush、一次 send；`deliver` → `process_burst` + `deliver_group` | `sharded.rs`、`conn.rs`、`conn/endpoint.rs` |
| ①④ | `evict_stale` 守卫恢复整 burst 语义（`feed_raw_batch` ⇒ `stale_bursts` 仅在**全组** stale 时 +1；单 stale 包混入新鲜流量不再误杀）。新增回归测试 `stale_guard_counts_whole_burst_not_single_datagrams`（混合 batch 不计数、全 stale batch 计数，两个断言都钉死） | `endpoint.rs`、`conn.rs` 测试模块 |
| ③ | 删除 `spawn_tx`（忙旋 task）与 `tx_ready` 通知链；`send_all` 的 WouldBlock 改为有界 `TX_RETRY_SLEEP=5ms` 退避重试——绝不 `await writable()`（EPOLLET 边沿可能在"失败发送"与"注册等待"之间已错过 → 永久悬挂；与未提交的 transport.rs 修改同一结论，见 bugs/BUGREPORT_CLOSE_KILLS_TX_SENDER_HANG.md） | `sharded.rs`、`transport.rs` |
| ⑤ | `RECV_BATCH` 32→256（守卫以 burst 为单位，上限需大于单 peer 拥塞窗口）；sweep 从"每批一次"改为"按 `SWEEP_INTERVAL` 到期一次"（O(sessions) 不得随包率缩放） | `sharded.rs` |
| ⑦ | `run_p99.sh` 增加 `run_bench.sh` 同款负载守卫（1-min load > 0.7×cores 中止，`BENCH_FORCE=1` 覆盖） | `bench/run_p99.sh` |
| — | 清理：`Session.queue` 冗余字段、死常量 `MAX_IDLE_UPDATE_MS`、两处 `mut` lint（ed10d0ed 遗留）；`AGENTS.md` kcp-rs 段同步新架构 | 各处 |

**保留不动的（有意设计）**：inline feed（消灭 6–13ms 跨任务跳变，handoff 主因）、
`flush_acks_only`、flush loop 首醒即刷（handoff 已论证）。`SharedSendLock` 保留
（后续按 plan 2.2 分片或 2.1 TX 泵取代）。

### 6.2 验证状态

- `cargo test -p kcp-rs --features async`：**lib 87/87 + listener 15/15 + crypto 3/3 全绿**
  （`stale_guard` 新测试含内）。
- `cargo clippy -p kcp-rs --features async --all-targets -- -D warnings`：通过。
- **未测**：Linux 交叉编译 + 192.168.0.18 的 A/B（handoff 下一步 2–4）。
  达标判断：单连延迟 <1ms、吞吐 ~50 MB/s；32 连接吞吐收复至 ~70+ MB/s。

### 6.3 遗留：`sweep_reaps_closed_sessions_without_remove_peer` 既有挂死（非本次修复引入）

**该测试在干净 HEAD（ed10d0ed，N=5000 与 N=1000 两种配置）上都会挂死**，与本次
修复无关（对照实验：stash 全部修复后复现）。证据：

- macOS `sample` 抓栈：busy worker 全部落在 `spawn_flush_loop` →
  `knet::time::tokio::timeout` → `knet::sync::NotifyFuture::poll`
  （`AtomicUsize::swap` 热点）→ `flush_with_current` —— flush loop 的
  "等待-唤醒"机制在零间隔地空转。
- 卡点**不确定**（一次卡在 connects 阶段、一次卡在回收等待阶段），N=200 也复现
  ⇒ 不是会话规模问题，是竞态。
- **首要嫌疑**（与采样栈吻合的自旋闭环）：flush loop 第二次 drain 拿不到
  `is_sending` token 时执行 `flush_notify.notify_one()` 自存 permit
  （knet Notify `fetch_or(1)`），下一轮 `notified()` 立即 Ready —— 只要 token
  持有者不释放就形成"自唤醒热循环"；若 token 被某个永久阻塞的发送路径持有
  （BUGREPORT_CLOSE_KILLS_TX_SENDER_HANG 的残留场景）则永转。修复方向：CAS 失败
  不应自 notify_one，改为"token 释放时负责唤醒"（释放点 notify_waiters 或
  pending-send 标志位），并给 flush loop 的 CAS 失败路径加退避。
- 本次验证以 `-- --skip sweep_reaps` 排除该项；修复它需要独立一轮（建议与
  plan 2.3/2.1 的 tx 唤醒重构合并处理）。

### 6.4 追加修复：flush loop 自旋就是 32 路杀手（2026-09-24 17:3x，已修复）

§6.3 的嫌疑在 192.168.0.18 的 32 路隧道 A/B 中**被证实为主因**：

- 修复①-⑤⑦ 全部到位后的首跑（17:2x）：32 路仍 3/3 轮全挂
  （32/32 连接失败、warmup ~1MB 处停滞、endload 7.7-9.2）——批量守卫和
  TX 修复不够，flush loop 自旋才是把 runtime 打满、拖死数据面的那只手。
- **修复**：flush loop 第二次 drain 的 CAS 失败分支不再 `flush_notify.notify_one()`
  （knet Notify 的 permit 合并 + 自唤醒 = 零间隔热循环），改为
  `next_deadline = now + 1ms` 有界退避；token 持有者释放前本来就整批
  drain `raw_packets`，不会丢包（`endpoint.rs`）。
- **结果（3 轮交替，32 条 TCP × 8MB，--conn 8，aes/nocomp/fast）**：
  new 全部 rc=0，吞吐中位 80.88 vs old 72.74 MB/s（**+11%**），
  延迟中位 0.15 vs 0.24 ms（**−37%**）。64 路同样全部 rc=0：
  67.73 vs 71.70 MB/s（±5% 内持平），延迟 0.18 vs 0.33 ms（−45%）。
- 附带效应：new 跑分期间的系统负载高于 old（endload ~4 vs ~1.2）——
  每 session task + flush loop 的任务数多于 main 的专职线程，属预期取舍。
