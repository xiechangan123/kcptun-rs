# 性能优化方案：保持 session-as-tasks 架构

> **2026-09-24 修订（结合 `docs/handoff-session-task-listener-2026-09-24.md`）**  
> 根因排序已按 Linux 实测修正：**主因是 rx→input 跨任务调度（6–13ms）**，  
> 不是 32 连接抢锁（单连接也复现）。下文 P0 的 1.1/1.2/1.3 **已在工作区落地**  
> （`feed_raw_single` 内联 + `flush_acks_only` + flush loop 首醒即刷），  
> **缺的是 Linux 交叉编译后的一次干净测量**，不是更多代码。

## 0.1 与 handoff 对齐后的根因（覆盖下文第 0 节的猜测）

| 证据（handoff） | 含义 |
|-----------------|------|
| recv→send 全程 13–17ms，其中 KCP<2ms、try_send<1ms | 差的是**调度**，不是计算/发送 |
| 单连接 6–13ms 也复现 | **不是** 32 路 `send_lock` 争用 |
| Mac 上同一份未提交代码 0.27ms | 代码路径已对；Linux 内核 3.10 **timer 粒度**放大跨任务唤醒 |
| `acknodelay` 默认关 + flush loop 首醒只 arm 10ms | 第二层量化，`flush_acks_only` 已绕开 |
| main 无这次唤醒（worker 线程 `feed_raw_batch` 同步跑完） | 对照物确认 |

**因此实施顺序改为**：

```text
【已完成，工作区未提交】
  deliver → conn.feed_raw_single   （已建立会话跳过 PeerQueue/input task）
  KCP::flush_acks_only             （acknodelay 关时 ACK 立刻进数据报）
  flush loop 首醒即刷              （去掉 arm-then-return）
【下一步，handoff 第 1–5 条】
  1. 全测试绿（本机 16/16 已过，含 listener_close_does_not_hang）
  2. make linux → scp 到 192.168.0.18:/tmp/kcpbench-cmp/new/
  3. 单连接 A/B：目标延迟 <1ms、吞吐 ~50 MB/s
  4. 达标后再跑 32 连接 cmp.sh
  5. 达标才提交（只含 kcp-rs/src + kcp-rs/tests）
【若 Linux 仍是数毫秒】
  不要再调 ACK 阈值——那是调度开销；下文 P1 的 TX 泵/send_lock 分片
  解决的是吞吐，不是这条延迟。
```

### 本机验证记录（2026-09-24）

```
cargo test -p kcp-rs --features async --lib --test kcpstream_listener --test listener_crypto_gate
  lib 86 / listener 16 / crypto 3 — 全绿
  listener_close_does_not_hang_an_inflight_write  ok（连跑 8 次）
  sweep_reaps_closed_sessions_without_remove_peer ok（N=1000，handoff 口径）
```

`Session.queue` 字段在已建立会话走 `feed_raw_single` 后不再读取（编译警告）；  
building 路径仍在用 `PeerQueue`。提交前可改为 `Option` 或加 `#[allow]`，不影响功能。

---

> **2026-09-24 修订（结合 `docs/handoff-session-task-listener-2026-09-24.md`）**  
> 根因排序已按 Linux 实测修正：**主因是 rx→input 跨任务调度（6–13ms）**，  
> 不是 32 连接抢锁（单连接也复现）。下文 P0 的 1.1/1.2/1.3 **已在工作区落地**  
> （`feed_raw_single` 内联 + `flush_acks_only` + flush loop 首醒即刷），  
> **缺的是 Linux 交叉编译后的一次干净测量**，不是更多代码。

## 0.1 与 handoff 对齐后的根因（覆盖下文第 0 节的猜测）

| 证据（handoff） | 含义 |
|-----------------|------|
| recv→send 全程 13–17ms，其中 KCP<2ms、try_send<1ms | 差的是**调度**，不是计算/发送 |
| 单连接 6–13ms 也复现 | **不是** 32 路 `send_lock` 争用 |
| Mac 上同一份未提交代码 0.27ms | 代码路径已对；Linux 内核 3.10 **timer 粒度**放大跨任务唤醒 |
| `acknodelay` 默认关 + flush loop 首醒只 arm 10ms | 第二层量化，`flush_acks_only` 已绕开 |
| main 无这次唤醒（worker 线程 `feed_raw_batch` 同步跑完） | 对照物确认 |

**因此实施顺序改为**：

```text
【已完成，工作区未提交】
  deliver → conn.feed_raw_single   （已建立会话跳过 PeerQueue/input task）
  KCP::flush_acks_only             （acknodelay 关时 ACK 立刻进数据报）
  flush loop 首醒即刷              （去掉 arm-then-return）
【下一步，handoff 第 1–5 条】
  1. 全测试绿（本机 16/16 已过，含 listener_close_does_not_hang）
  2. make linux → scp 到 192.168.0.18:/tmp/kcpbench-cmp/new/
  3. 单连接 A/B：目标延迟 <1ms、吞吐 ~50 MB/s
  4. 达标后再跑 32 连接 cmp.sh
  5. 达标才提交（只含 kcp-rs/src + kcp-rs/tests）
【若 Linux 仍是数毫秒】
  不要再调 ACK 阈值——那是调度开销；下文 P1 的 TX 泵/send_lock 分片
  解决的是吞吐，不是这条延迟。
```

### 本机验证记录（2026-09-24）

```
cargo test -p kcp-rs --features async --lib --test kcpstream_listener --test listener_crypto_gate
  lib 86 / listener 16 / crypto 3 — 全绿
  listener_close_does_not_hang_an_inflight_write  ok（连跑 8 次）
  sweep_reaps_closed_sessions_without_remove_peer ok（N=1000，handoff 口径）
```

`Session.queue` 字段在已建立会话走 `feed_raw_single` 后不再读取（编译警告）；  
building 路径仍在用 `PeerQueue`。提交前可改为 `Option` 或加 `#[allow]`，不影响功能。

---

> **2026-09-24 修订（结合 `docs/handoff-session-task-listener-2026-09-24.md`）**  
> 根因排序已按 Linux 实测修正：**主因是 rx→input 跨任务调度（6–13ms）**，  
> 不是 32 连接抢锁（单连接也复现）。下文 P0 的 1.1/1.2/1.3 **已在工作区落地**  
> （`feed_raw_single` 内联 + `flush_acks_only` + flush loop 首醒即刷），  
> **缺的是 Linux 交叉编译后的一次干净测量**，不是更多代码。

## 0.1 与 handoff 对齐后的根因（覆盖下文第 0 节的猜测）

| 证据（handoff） | 含义 |
|-----------------|------|
| recv→send 全程 13–17ms，其中 KCP<2ms、try_send<1ms | 差的是**调度**，不是计算/发送 |
| 单连接 6–13ms 也复现 | **不是** 32 路 `send_lock` 争用 |
| Mac 上同一份未提交代码 0.27ms | 代码路径已对；Linux 内核 3.10 **timer 粒度**放大跨任务唤醒 |
| `acknodelay` 默认关 + flush loop 首醒只 arm 10ms | 第二层量化，`flush_acks_only` 已绕开 |
| main 无这次唤醒（worker 线程 `feed_raw_batch` 同步跑完） | 对照物确认 |

**因此实施顺序改为**：

```text
【已完成，工作区未提交】
  deliver → conn.feed_raw_single   （已建立会话跳过 PeerQueue/input task）
  KCP::flush_acks_only             （acknodelay 关时 ACK 立刻进数据报）
  flush loop 首醒即刷              （去掉 arm-then-return）
【下一步，handoff 第 1–5 条】
  1. 全测试绿（本机 16/16 已过，含 listener_close_does_not_hang）
  2. make linux → scp 到 192.168.0.18:/tmp/kcpbench-cmp/new/
  3. 单连接 A/B：目标延迟 <1ms、吞吐 ~50 MB/s
  4. 达标后再跑 32 连接 cmp.sh
  5. 达标才提交（只含 kcp-rs/src + kcp-rs/tests）
【若 Linux 仍是数毫秒】
  不要再调 ACK 阈值——那是调度开销；下文 P1 的 TX 泵/send_lock 分片
  解决的是吞吐，不是这条延迟。
```

### 本机验证记录（2026-09-24）

```
cargo test -p kcp-rs --features async --lib --test kcpstream_listener --test listener_crypto_gate
  lib 86 / listener 16 / crypto 3 — 全绿
  listener_close_does_not_hang_an_inflight_write  ok（连跑 8 次）
  sweep_reaps_closed_sessions_without_remove_peer ok（N=1000，handoff 口径）
```

`Session.queue` 字段在已建立会话走 `feed_raw_single` 后不再读取（编译警告）；  
building 路径仍在用 `PeerQueue`。提交前可改为 `Option` 或加 `#[allow]`，不影响功能。

---

> **2026-09-24 修订（结合 `docs/handoff-session-task-listener-2026-09-24.md`）**  
> 根因排序已按 Linux 实测修正：**主因是 rx→input 跨任务调度（6–13ms）**，  
> 不是 32 连接抢锁（单连接也复现）。下文 P0 的 1.1/1.2/1.3 **已在工作区落地**  
> （`feed_raw_single` 内联 + `flush_acks_only` + flush loop 首醒即刷），  
> **缺的是 Linux 交叉编译后的一次干净测量**，不是更多代码。

## 0.1 与 handoff 对齐后的根因（覆盖下文第 0 节的猜测）

| 证据（handoff） | 含义 |
|-----------------|------|
| recv→send 全程 13–17ms，其中 KCP<2ms、try_send<1ms | 差的是**调度**，不是计算/发送 |
| 单连接 6–13ms 也复现 | **不是** 32 路 `send_lock` 争用 |
| Mac 上同一份未提交代码 0.27ms | 代码路径已对；Linux 内核 3.10 **timer 粒度**放大跨任务唤醒 |
| `acknodelay` 默认关 + flush loop 首醒只 arm 10ms | 第二层量化，`flush_acks_only` 已绕开 |
| main 无这次唤醒（worker 线程 `feed_raw_batch` 同步跑完） | 对照物确认 |

**因此实施顺序改为**：

```text
【已完成，工作区未提交】
  deliver → conn.feed_raw_single   （已建立会话跳过 PeerQueue/input task）
  KCP::flush_acks_only             （acknodelay 关时 ACK 立刻进数据报）
  flush loop 首醒即刷              （去掉 arm-then-return）
【下一步，handoff 第 1–5 条】
  1. 全测试绿（本机 16/16 已过，含 listener_close_does_not_hang）
  2. make linux → scp 到 192.168.0.18:/tmp/kcpbench-cmp/new/
  3. 单连接 A/B：目标延迟 <1ms、吞吐 ~50 MB/s
  4. 达标后再跑 32 连接 cmp.sh
  5. 达标才提交（只含 kcp-rs/src + kcp-rs/tests）
【若 Linux 仍是数毫秒】
  不要再调 ACK 阈值——那是调度开销；下文 P1 的 TX 泵/send_lock 分片
  解决的是吞吐，不是这条延迟。
```

### 本机验证记录（2026-09-24）

```
cargo test -p kcp-rs --features async --lib --test kcpstream_listener --test listener_crypto_gate
  lib 86 / listener 16 / crypto 3 — 全绿
  listener_close_does_not_hang_an_inflight_write  ok（连跑 8 次）
  sweep_reaps_closed_sessions_without_remove_peer ok（N=1000，handoff 口径）
```

`Session.queue` 字段在已建立会话走 `feed_raw_single` 后不再读取（编译警告）；  
building 路径仍在用 `PeerQueue`。提交前可改为 `Option` 或加 `#[allow]`，不影响功能。

---

> **2026-09-24 修订（结合 `docs/handoff-session-task-listener-2026-09-24.md`）**  
> 根因排序已按 Linux 实测修正：**主因是 rx→input 跨任务调度（6–13ms）**，  
> 不是 32 连接抢锁（单连接也复现）。下文 P0 的 1.1/1.2/1.3 **已在工作区落地**  
> （`feed_raw_single` 内联 + `flush_acks_only` + flush loop 首醒即刷），  
> **缺的是 Linux 交叉编译后的一次干净测量**，不是更多代码。

## 0.1 与 handoff 对齐后的根因（覆盖下文第 0 节的猜测）

| 证据（handoff） | 含义 |
|-----------------|------|
| recv→send 全程 13–17ms，其中 KCP<2ms、try_send<1ms | 差的是**调度**，不是计算/发送 |
| 单连接 6–13ms 也复现 | **不是** 32 路 `send_lock` 争用 |
| Mac 上同一份未提交代码 0.27ms | 代码路径已对；Linux 内核 3.10 **timer 粒度**放大跨任务唤醒 |
| `acknodelay` 默认关 + flush loop 首醒只 arm 10ms | 第二层量化，`flush_acks_only` 已绕开 |
| main 无这次唤醒（worker 线程 `feed_raw_batch` 同步跑完） | 对照物确认 |

**因此实施顺序改为**：

```text
【已完成，工作区未提交】
  deliver → conn.feed_raw_single   （已建立会话跳过 PeerQueue/input task）
  KCP::flush_acks_only             （acknodelay 关时 ACK 立刻进数据报）
  flush loop 首醒即刷              （去掉 arm-then-return）
【下一步，handoff 第 1–5 条】
  1. 全测试绿（本机 16/16 已过，含 listener_close_does_not_hang）
  2. make linux → scp 到 192.168.0.18:/tmp/kcpbench-cmp/new/
  3. 单连接 A/B：目标延迟 <1ms、吞吐 ~50 MB/s
  4. 达标后再跑 32 连接 cmp.sh
  5. 达标才提交（只含 kcp-rs/src + kcp-rs/tests）
【若 Linux 仍是数毫秒】
  不要再调 ACK 阈值——那是调度开销；下文 P1 的 TX 泵/send_lock 分片
  解决的是吞吐，不是这条延迟。
```

### 本机验证记录（2026-09-24）

```
cargo test -p kcp-rs --features async --lib --test kcpstream_listener --test listener_crypto_gate
  lib 86 / listener 16 / crypto 3 — 全绿
  listener_close_does_not_hang_an_inflight_write  ok（连跑 8 次）
  sweep_reaps_closed_sessions_without_remove_peer ok（N=1000，handoff 口径）
```

`Session.queue` 字段在已建立会话走 `feed_raw_single` 后不再读取（编译警告）；  
building 路径仍在用 `PeerQueue`。提交前可改为 `Option` 或加 `#[allow]`，不影响功能。

---

> **2026-09-24 修订（结合 `docs/handoff-session-task-listener-2026-09-24.md`）**  
> 根因排序已按 Linux 实测修正：**主因是 rx→input 跨任务调度（6–13ms）**，  
> 不是 32 连接抢锁（单连接也复现）。下文 P0 的 1.1/1.2/1.3 **已在工作区落地**  
> （`feed_raw_single` 内联 + `flush_acks_only` + flush loop 首醒即刷），  
> **缺的是 Linux 交叉编译后的一次干净测量**，不是更多代码。

## 0.1 与 handoff 对齐后的根因（覆盖下文第 0 节的猜测）

| 证据（handoff） | 含义 |
|-----------------|------|
| recv→send 全程 13–17ms，其中 KCP<2ms、try_send<1ms | 差的是**调度**，不是计算/发送 |
| 单连接 6–13ms 也复现 | **不是** 32 路 `send_lock` 争用 |
| Mac 上同一份未提交代码 0.27ms | 代码路径已对；Linux 内核 3.10 **timer 粒度**放大跨任务唤醒 |
| `acknodelay` 默认关 + flush loop 首醒只 arm 10ms | 第二层量化，`flush_acks_only` 已绕开 |
| main 无这次唤醒（worker 线程 `feed_raw_batch` 同步跑完） | 对照物确认 |

**因此实施顺序改为**：

```text
【已完成，工作区未提交】
  deliver → conn.feed_raw_single   （已建立会话跳过 PeerQueue/input task）
  KCP::flush_acks_only             （acknodelay 关时 ACK 立刻进数据报）
  flush loop 首醒即刷              （去掉 arm-then-return）
【下一步，handoff 第 1–5 条】
  1. 全测试绿（本机 16/16 已过，含 listener_close_does_not_hang）
  2. make linux → scp 到 192.168.0.18:/tmp/kcpbench-cmp/new/
  3. 单连接 A/B：目标延迟 <1ms、吞吐 ~50 MB/s
  4. 达标后再跑 32 连接 cmp.sh
  5. 达标才提交（只含 kcp-rs/src + kcp-rs/tests）
【若 Linux 仍是数毫秒】
  不要再调 ACK 阈值——那是调度开销；下文 P1 的 TX 泵/send_lock 分片
  解决的是吞吐，不是这条延迟。
```

### 本机验证记录（2026-09-24）

```
cargo test -p kcp-rs --features async --lib --test kcpstream_listener --test listener_crypto_gate
  lib 86 / listener 16 / crypto 3 — 全绿
  listener_close_does_not_hang_an_inflight_write  ok（连跑 8 次）
  sweep_reaps_closed_sessions_without_remove_peer ok（N=1000，handoff 口径）
```

`Session.queue` 字段在已建立会话走 `feed_raw_single` 后不再读取（编译警告）；  
building 路径仍在用 `PeerQueue`。提交前可改为 `Option` 或加 `#[allow]`，不影响功能。

---

> **2026-09-24 修订（结合 `docs/handoff-session-task-listener-2026-09-24.md`）**  
> 根因排序已按 Linux 实测修正：**主因是 rx→input 跨任务调度（6–13ms）**，  
> 不是 32 连接抢锁（单连接也复现）。下文 P0 的 1.1/1.2/1.3 **已在工作区落地**  
> （`feed_raw_single` 内联 + `flush_acks_only` + flush loop 首醒即刷），  
> **缺的是 Linux 交叉编译后的一次干净测量**，不是更多代码。

## 0.1 与 handoff 对齐后的根因（覆盖下文第 0 节的猜测）

| 证据（handoff） | 含义 |
|-----------------|------|
| recv→send 全程 13–17ms，其中 KCP<2ms、try_send<1ms | 差的是**调度**，不是计算/发送 |
| 单连接 6–13ms 也复现 | **不是** 32 路 `send_lock` 争用 |
| Mac 上同一份未提交代码 0.27ms | 代码路径已对；Linux 内核 3.10 **timer 粒度**放大跨任务唤醒 |
| `acknodelay` 默认关 + flush loop 首醒只 arm 10ms | 第二层量化，`flush_acks_only` 已绕开 |
| main 无这次唤醒（worker 线程 `feed_raw_batch` 同步跑完） | 对照物确认 |

**因此实施顺序改为**：

```text
【已完成，工作区未提交】
  deliver → conn.feed_raw_single   （已建立会话跳过 PeerQueue/input task）
  KCP::flush_acks_only             （acknodelay 关时 ACK 立刻进数据报）
  flush loop 首醒即刷              （去掉 arm-then-return）
【下一步，handoff 第 1–5 条】
  1. 全测试绿（本机 16/16 已过，含 listener_close_does_not_hang）
  2. make linux → scp 到 192.168.0.18:/tmp/kcpbench-cmp/new/
  3. 单连接 A/B：目标延迟 <1ms、吞吐 ~50 MB/s
  4. 达标后再跑 32 连接 cmp.sh
  5. 达标才提交（只含 kcp-rs/src + kcp-rs/tests）
【若 Linux 仍是数毫秒】
  不要再调 ACK 阈值——那是调度开销；下文 P1 的 TX 泵/send_lock 分片
  解决的是吞吐，不是这条延迟。
```

### 本机验证记录（2026-09-24）

```
cargo test -p kcp-rs --features async --lib --test kcpstream_listener --test listener_crypto_gate
  lib 86 / listener 16 / crypto 3 — 全绿
  listener_close_does_not_hang_an_inflight_write  ok（连跑 8 次）
  sweep_reaps_closed_sessions_without_remove_peer ok（N=1000，handoff 口径）
```

`Session.queue` 字段在已建立会话走 `feed_raw_single` 后不再读取（编译警告）；  
building 路径仍在用 `PeerQueue`。提交前可改为 `Option` 或加 `#[allow]`，不影响功能。

---

- 对象：`feat/session-task-listener`（`5bc0b566` / `6ce0875b` 之后的架构）
- 基线：32 连接 main **79.95 MB/s / 0.23 ms**；新分支 **56.28 MB/s / 9.00 ms**
- 约束：**不回退**到 OS 线程 worker 池 / Mode B 内联流水；保留「rx task → per-session queue → session task → 共享 socket」模型
- 目标：延迟 P50 **≤ 0.5 ms**（消除 10ms 量化），吞吐 **≥ 75 MB/s** @32 conn（收复 ≥90% 的差距）

---

## 0. 问题拆解与优先级

| 现象 | 根因 | 阶段 | 预期收复 |
|------|------|------|----------|
| 延迟 9 ms（×39） | flush loop `next_deadline=None` 只 arm 不发；`now < deadline` 时 notify 也被跳过 | **P0** | 延迟 → 亚毫秒 |
| 延迟第 3 轮 17.9 ms | 同上 ×2 个 `interval=10ms` tick | **P0** | 同上 |
| 吞吐 -30% | 全局 `send_lock` + 每包队列跳 + 丢掉跨 peer `sendmmsg` | **P1** | +15～20 MB/s |
| 32 连接放大 | 32 流抢一把锁、`tx_ready` 惊群、66 个任务 | **P2** | 其余差距 |

原则：**先修延迟（便宜、收益大、风险小），再动发送路径，最后做调度/拷贝微优化。**

```text
                    P0：何时发                P1：怎么发                 P2：谁在发
                 ┌──────────────┐        ┌────────────────┐       ┌──────────────┐
  现在 ────────► │ flush loop   │ ─────► │ send_lock×1    │ ────► │ 66 tasks     │
                 │ 10ms 量化    │        │ 每 session 一次 │       │ notify_waiters│
                 └──────────────┘        └────────────────┘       └──────────────┘
  目标 ────────► │ notify 即发   │        │ 跨 peer 批量    │       │ 公平唤醒/合并 │
                 │ 无 tick 量化  │        │ sendmmsg 一次   │       │ 可选 shard 锁 │
                 └──────────────┘        └────────────────┘       └──────────────┘
```

---

## 1. P0 — 打破 10ms 量化（延迟 9ms → 亚毫秒）

工作量：0.5～1 天 · 风险：低 · 预期：**延迟 −95%**，吞吐小幅上升（少走慢路径）

### 1.1 落地「flush loop 首醒必做事」（工作区已有未提交补丁）

**位置**：`kcp-rs/src/conn/endpoint.rs` `spawn_flush_loop` 的 `ws` 块

**现状**（已提交）：

```text
match next_deadline {
    Some(d) if now < d => continue,          // 未到点：本轮什么都不做
    None => { arm(now + interval); continue } // 首醒：只上表！
    Some(_) => {}                            // 到点才 flush
}
```

**问题**：`IDLE_PARK_GRACE_MS=1000` 之后 `next_deadline=None`。下一次活动若 `try_drain_and_send` 失败或 `protocol_pending`，notify 到达 → 只 arm `ACTIVE_UPDATE_MAX_MS=10ms` → 包在 `raw_packets` 睡满一个 tick。

**改法**（保持架构，只改控制流）：

1. `None` = **马上 flush**，不要 arm 后 `continue`；
2. `was_notified == true` 时 **忽略 `now < deadline`**——被 notify 叫醒就是有活要干；只有「定时器提前到点」才允许 `continue`；
3. `was_notified == false`（纯 timer 到点）再走原来的 deadline 判断。

```text
// 语义
if !was_notified && next_deadline.is_some_and(|d| now < d) {
    continue;                       // 只有纯 timer 空转才跳过
}
// 否则：立刻 flush_with_current + 第二次 drain 发送
```

**验收**：单连接 request/response P50 ≤ 0.5 ms；抓包间隔不再是 10ms 格点。

### 1.2 `acknodelay=false` 时 ACK 随 inbound burst 发出

**位置**：`process_inbound_batch`（`endpoint.rs` 约 1045 行）

**现状**：`flush_if_pending` 只在 `pending_flush` 时 flush；`acknodelay=false` 时 ACK 留在 `acklist`，等 flush loop。

**改法**（工作区未提交补丁已有）：

```text
kcp.flush_if_pending(current);
if !acknodelay && kcp.acklist_len() > 0 {
    kcp.flush_with_current(current, true);   // 本 burst 直接把 ACK 写进 raw_packets
}
```

配合 1.1，内联 `try_drain_and_send` 当场把 ACK 发出去，不再等 tick。

**注意**：`acknodelay=true`（默认）本来就会 `pending_flush`，此项是给关掉 acknodelay 的配置兜底。

### 1.3 内联发送失败后的「一次重试」而不是直接丢给 flush loop

**位置**：`spawn_input_loop`（`endpoint.rs` 约 928 行）

**现状**：

```text
let sent_inline = shared.try_drain_and_send().await;
if !sent_inline || protocol_pending {
    shared.flush_notify.notify_one();
}
```

`try_drain_and_send` 在 `is_sending` CAS 失败时 **直接返回 false**，包全靠 flush loop。

**改法**（仍不改架构）：

1. CAS 失败时 `yield_now()` 后 **再试一次**（同连接 write/flush 通常在微秒级释放 token）；
2. 二次仍失败才 `notify_one`；
3. flush loop 的 **fast-send 段保留**（发 `raw_packets` 先于 KCP 维护），这样 fallback 仍是「快发 + 可选维护」，不会把包扣在 tick 后面。

**收益**：32 连接下 `is_sending` 冲突变多，重试把大部分 fallback 转成内联，压低 P99。

### 1.4 P0 验收实验（先测后合）

| 实验 | 通过标准 |
|------|----------|
| 1 conn 探针 × 3 轮 | P50 ≤ 0.5 ms，无 10ms 格点 |
| 32 conn 探针 × 3 轮 | P50 ≤ 1.0 ms，P999 ≤ 5 ms |
| `tcpdump` 时间戳 | ACK/数据间隔不再对齐 10 ms |
| `try_drain_and_send` 失败率 | < 5%（加临时计数） |

---

## 2. P1 — 发送路径：拿回收复吞吐的 15～20 MB/s

工作量：3～5 天 · 风险：中（碰 wire 批量与锁） · 预期：**56 → 70～75 MB/s**

### 2.1 跨 peer `sendmmsg` 聚合（最大头）

**问题**：`PeerTransport::send_all`（`transport.rs:466-497`）每次只发**一个** peer 的 batch，且整段持 `send_lock`。32 条流 = 32 次独立 syscall + 32 次抢锁。

**现状调用链**：

```text
session i: flush_tx_batch_tracking
  → PeerTransport::try_send_batch_to(packets, peer_i)
      → send_lock.lock()
      → socket.try_send_batch_to(...)   // sendmmsg，但只有 peer_i 一个目标
      → send_lock.unlock()
```

**改法**（架构不变，引入 listener 级 TX 泵）：

```text
各 session 的 raw_packets / flush 结果
        │  每 session 一条有界 outbox（保持 session 内 FIFO）
        ▼
┌───────────────────────────────────────┐
│  ListenerTxPump（替代/增强 spawn_tx） │
│  1. 从 N 个 session outbox 各取一批   │
│  2. 拼成 (buf, addr) 列表             │
│  3. 一次 sendmmsg_multi 全部发出      │
└───────────────────────────────────────┘
        │
        ▼
     共享 UDP socket
```

**关键约束（必须保住）**：

- **单 session 内 wire 顺序**：同一 session 的包在 outbox 内 FIFO；聚合时同一 session 的连续包排在一起，`sendmmsg` 对同一 dest 保序；
- **`is_sending` / 单写者语义**：session 内仍只有一个 drainer；outbox 只是把「立刻 sendto」换成「交给泵」；
- **`WouldBlock` 回压**：泵发不动时把剩余段放回各 session outbox，并只在 `writable()` 后再醒（沿用 `spawn_tx` 的角色）。

**knet 侧需要的小扩展**（`knet-rs/src/net/mmsg.rs`）：

- 现有 `sendmmsg_to(bufs, &target)` 是 **单一 dest**；
- 新增 `sendmmsg_multi(bufs, addrs: &[SocketAddr])`：每个 `mmsghdr.msg_name` 填不同地址（`recvmmsg` 路径已有 per-msg `names[]`，对称写法即可）；
- 非 Linux 退化为逐条 `send_to`（现有 fallback 路径）。

**收益估算**：32 conn 时 syscall 从 O(32) 降到 O(1～4)/轮，锁从 32 次争用降到泵内 1 次。参考 main 的 worker 批量，这一项大约值 **+10～15 MB/s**。

### 2.2 `send_lock` 分片（若 2.1 分期做，可先上这个）

**问题**：`SharedSendLock = Arc<Mutex<()>>` 全局一把（`transport.rs:344`）。

**改法**（低风险、可独立合入）：

```text
SharedSendLocks { shards: [Mutex<()>; 8] }   // 2^k 分片
PeerTransport 持 shard_id = hash(peer) % 8
send_all 只锁自己的 shard
```

- 同 peer 永远同 shard → 保序；
- 不同 peer 不互相挡 syscall（内核 UDP 发送本身线程安全）；
- 8 片在 32 conn 下把期望争用降到 4:1。

**若最终上了 2.1 TX 泵，分片锁可删**（泵内单消费者根本不需要它）。建议顺序：**先 2.2 快速止血 → 再 2.1 彻底合并**。

### 2.3 `tx_ready` 公平唤醒，去掉惊群

**位置**：`sharded.rs` `spawn_tx`（`notify_waiters` + `yield_now` 自旋）

**问题**：内核缓冲一满，32 个 session 全堵在 `tx_ready.notified()`；`writable()` 一到 **`notify_waiters` 全醒**，再集体抢 `send_lock`，大量无效唤醒。

**改法**：

1. `tx_ready` 从 `Notify` 换成 **计数信号量 / 公平队列**（每次 `writable()` 只放行 `可发送配额` 个 waiter，例如 8）；
2. 或：TX 泵模式下 session **不再等 `tx_ready`**，只等「我的 outbox 被泵发完」的 per-session Notify——惊群自然消失；
3. 去掉 `spawn_tx` 里的 `yield_now()` 自旋：改为 `writable()` edge-trigger 后只 `notify` 有限个 waiter。

**验收**：`snmp` 里加 `tx_ready_wakes` / `tx_send_attempts` / `tx_send_empty` 三个计数；优化后 `wakes / attempts → 1`。

### 2.4 发送侧批量已有能力——确认不要拆掉

`flush_tx_batch_tracking`（`endpoint.rs:601`）已经走 `try_send_batch_to` → Linux `sendmmsg`。**单 session 内的批量是够的**，P1 的重点是 **跨 session**，不要在 session 内再拆回逐包 `sendto`。

---

## 3. P2 — 每包开销：队列、调度、拷贝

工作量：2～3 天 · 风险：低～中 · 预期：**+3～8 MB/s**，P99 再降

### 3.1 `deliver` 按 peer 合并入队（减少 queue 锁次数）

**位置**：`sharded.rs` `deliver` / `spawn_rx` 内层 `try_recv_from` 循环

**现状**：`RECV_BATCH=32` 虽然一次收 32 个包，但 **每包单独 `deliver` → `queue.push` 一把锁**。同一 peer 连发 10 个包 = 10 次锁 + 10 次 `notify_one`。

**改法**：

```text
RX 循环内：按 peer 聚成 Vec<Vec<u8>>
→ queue.push_batch(vecs)     // 一次锁、一次 notify
→ 仅当队列从空→非空时 notify
```

`PeerQueue` 增加 `push_batch`（已有 `pop_batch` 对称）。同 peer 顺序不变。

### 3.2 减少每 session 的 task 数量

**现状**：每 session `spawn_input_loop` + `spawn_flush_loop`，32 conn ≈ 64 task + rx + tx。

**改法（可选，收益中等）**：

- input loop 与 flush loop **合并为一个 task**：`race(transport.recv, flush_deadline, flush_notify)`，单任务内完成「收 → KCP → 发 → 维护」；  
  这不改变架构（仍是 per-session async task），只是 2 task → 1 task。
- 或保留双 task，但 flush loop 的定时器用 **共享 timer wheel**（一个全局 deadline 堆 + 一个 ticker），避免 32 个独立 `knet::timeout`。

**权衡**：合并 task 改动面较大，建议做完 P0/P1 后用 profiler 决定是否值得。

### 3.3 确认入队/出队零拷贝

`PeerQueue::push(pkt: Vec<u8>)` 是 **move**，`pop_batch` 是 **swap**——已经零拷贝。  
P2 只需确认 `deliver` → `push` 之间没有多一次 `clone`（当前 `buf` 是 move 进 `push`，OK）。**不要引入 `Bytes` 再包一层的额外原子引用计数，除非要共享同一 payload 给 FEC。**

### 3.4 RX 侧小项

| 项 | 现状 | 建议 |
|----|------|------|
| `evict_stale` 每轮全表扫 | `watched` 只在有变化时才值得扫 | 保持；但 `watched` 非空才跑 |
| `sweep` 每包后调用 | `spawn_rx` 循环尾 `sweep` | 改为 `sweep_every_n` 或只在 idle timeout 路径跑，避免 32 conn 热路径每包全表 |
| 准入 gate 拷贝首包 | `buf.clone()` 一次 | 可接受（只发生在建连） |

`sweep` 在热路径上的成本随 session 数线性（全量扫描是 P2b 的正确性修复）；**不要为了性能重新引入 `MAX_SCAN`**，改为降低调用频率。

---

## 4. P0～P2 实施顺序

```text
第 1 天   1.1 + 1.2（flush loop + ACK）        → 重测延迟
第 1 天   1.3（内联二次重试）+ 计数器           → 看 fallback 率
第 2 天   2.2（send_lock 分片，独立小 PR）      → 重测吞吐
第 3-5 天 2.1（TX 泵 + sendmmsg_multi）         → 吞吐主战场
第 5 天   2.3（tx_ready 公平唤醒）              → 32+ 连接 P99
第 6-7 天 3.1（push_batch）+ 3.4（sweep 降频）  → 扫尾
（可选）   3.2 合并 input/flush task            → 有 profiler 证据再做
```

每一步都 **单独 PR、单独 A/B**，用同一套基准（32 conn，3 轮取中位）对比上表基线。

---

## 5. 可观测性（和优化一起上）

| 计数器 | 用途 |
|--------|------|
| `inline_send_ok` / `inline_send_fallback` | 验证 1.3，目标 fallback < 5% |
| `flush_loop_notify_wake` / `flush_loop_timer_wake` | 验证 1.1，notify 路径必须立刻发 |
| `tx_lock_wait_ns`（直方图） | 验证 2.2/2.1 |
| `tx_ready_wakes` / `tx_attempts` | 验证 2.3 惊群比 |
| `queue_push_calls` / `queue_push_batch_calls` | 验证 3.1 |
| `sweep_runs` | 验证 3.4 不再每包扫 |

已有 `recv-to-send took {us}us` 告警（>2ms）可保留，作为 P0 回归哨兵。

---

## 6. 预期结果

| 指标 | 现在 | P0 后 | P0+P1 后 | P0+P1+P2 后 |
|------|------|-------|----------|-------------|
| 延迟 P50 @32 conn | 9.00 ms | **0.3～0.8 ms** | ≤ 0.5 ms | ≤ 0.4 ms |
| 延迟 P999 | ~18 ms | ≤ 5 ms | ≤ 3 ms | ≤ 2 ms |
| 吞吐 @32 conn | 56.3 MB/s | 58～62 | **70～75** | **75～80** |
| 与 main 吞吐差 | -30% | -25% | **-6～12%** | **0～6%** |

吞吐与 main 的残余差距来自「共享单 socket + async 调度」本身，这是本架构的固有成本；不打算回退 worker 池的话，**P1 做完后 5% 以内是合理终态**。

---

## 7. 明确不做（保持架构的边界）

1. **不**恢复 OS 线程 worker 池 / `KCPTUN_WORKER_THREADS` / 三套 socket 拓扑；
2. **不**把 Mode B「同线程 feed_raw_batch」接回来当主路径（`feed_raw_*` 已删）；
3. **不**为性能重新引入 `MAX_SCAN` 截断 sweep（正确性优先，用降低频率解决）；
4. **不**在 session 内拆散已有 `sendmmsg` 批量；
5. **不**让 `send_lock` 覆盖 `await`（现有注释已约定只包 syscall，必须保持）。

---

## 8. 风险与回滚

| 风险 | 缓解 |
|------|------|
| TX 泵引入额外一次 outbox 拷贝/唤醒 | 用 move + 批量；泵空转时 park 在 `writable()` |
| `sendmmsg_multi` 部分发送语义复杂 | 沿用现有 `TxProgress` 前缀/后缀模型；非 Linux 自动退化 |
| notify 必做事导致 idle 连接多跑维护 | `flush_with_current` 无 pending 时开销是一次 KCP 扫描；idle 仍走 `IDLE_PARK_GRACE_MS` |
| 分片锁破坏同 peer 保序 | `hash(peer)` 固定 shard，同 peer 串行 |

每项独立 commit；延迟项（P0）可单独 cherry-pick 回补丁分支做热修。
