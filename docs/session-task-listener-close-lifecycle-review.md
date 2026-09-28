# 审查：`feat/session-task-listener` close / 资源回收

状态：审查报告（未改代码）  
分支：`feat/session-task-listener` vs `main`  
焦点：连接 close 后资源是否释放、回收是否合理、有无泄漏或挂死；是否引入新 bug  
测试基线：`listener_crypto_gate` 3/3、`kcpstream_listener` 11/11 通过

---

## 0. 结论摘要

| 级别 | 问题 | 相对 main | 处置建议 |
|------|------|-----------|----------|
| **P1** | `listener.close()` / `Drop` 杀掉 `spawn_tx` 后，`send_all` 在 `tx_ready` 上可能**永久挂起** | **新引入** | 必须修 |
| P2 | close 后 `PeerQueue` 未消费包不 `recycle_buf`，随 Session/map 滞留 | 队列化后更明显 | 建议修 |
| P2 | `sweep` 只扫 `MAX_SCAN=4096`，抬高 `max_sessions` 后 closed 会话可能晚回收 | 携带 | 建议修 |
| P3 | `push` 不检查 `closed`，仍可入队 | 新拓扑伴生 | 顺手修 |
| P3 | `close()` 后 in-flight build 仍可写入 `pending` | 新拓扑伴生 | 可接受，宜收紧 |

**拆除主路径（owner Drop → close → cancel token；map 由 remove_peer / sweep / deliver 回收）是合理且优于 main 的。**  
没有无界泄漏；阻断项是 P1 发送挂死。

---

## 1. 审查范围

```text
kcp-rs/src/sharded.rs      会话 task 化 listener（rx / tx / deliver / sweep）
kcp-rs/src/transport.rs    PeerQueue / PeerTransport / send_all / SharedSendLock
kcp-rs/src/conn.rs         KcpStream owner/clone、Drop、feed_*（遗留）
kcp-rs/src/conn/endpoint.rs SharedIoState::close、input/flush loop 退出
kcp-rs/src/conn/halves.rs  into_split 生命周期
knet-rs/src/net/*          新增 DatagramSocket::writable()
kcptun-server/src/*        生产 close / remove_peer 调用点
```

对照基线：`main` 的 sharded worker 线程模型（无 `spawn_tx`、无 per-session `PeerQueue` 入队）。

---

## 2. 资源清单与生命周期

| 资源 | 创建 | 销毁 / 回收 | 现状评价 |
|------|------|-------------|----------|
| rx task | `spawn_rx`（Listener `build`） | `closed`/`stop` → 循环 `break` | ✅ `close()` 用 `CancellationToken` 立刻打断 `recv_from` |
| tx task | `spawn_tx` | **`stop`（与 close 同一 token）** | ❌ 见 P1：close 杀 tx，存活会话发送依赖 tx |
| input task | `KcpStream::build` + `background_input(true)` | `cancel_token` / `is_closed` | ✅ close 即退；❌ 退出时不 drain queue（P2） |
| flush task | `spawn_flush_loop` | 同上 + `flush_notify` | ✅ |
| `SharedIoState` | build | 所有 `KcpStream` 克隆 Drop 且无外部 Arc | ✅ owner-only close；map 克隆撑住直到 remove |
| `PeerTransport` | deliver 建连 | `SharedIoState` Drop | ✅ `Drop` → `queue.mark_closed()` |
| `PeerQueue` | deliver 建连 | Session + PeerTransport 的 Arc 都释放 | ⚠️ `mark_closed` 不排空；P2 |
| session map 项 | build 完成 insert | `remove_peer` / `sweep` / `deliver` 发现 closed | ✅ 三条路径都 `close()` 后 remove |
| `pending`（accept 队列） | build 完成 push | `accept()` 弹出；Listener Drop 时 VecDeque Drop → owner close | ✅ 有界；close 后仍可能短暂写入（P3） |
| building 项 | deliver 开建 | build 完成 / 失败 / `building_timeout` | ✅ generation 防覆盖；超时 `mark_closed` |
| RX buf pool | `acquire_buf` / `recycle_buf` | push 满、decrypt 失败、deliver 丢弃路径 | ✅ 多数路径有 recycle；queue 内残留不进池（P2） |
| 发送锁 `SharedSendLock` | Listener 建一份 | 随 Listener / transports Drop | ✅ 只包 `try_send_*` syscall，锁内无 await |
| 共享 UDP socket | bind / from_socket | 最后一个 `Arc<DatagramSocket>` Drop | ✅ 会话与 listener 共享 Arc |

---

## 3. 关闭时序（设计意图）

### 3.1 单条会话由应用关闭（kcptun-server 路径）

```text
SMUX/accept 循环结束
  → session.close()
  → listener.remove_peer(peer)          // server.rs:134–136
        sessions.remove
        conn.close()                    // 非 owner 克隆也允许显式 close
  → KcpStream::close()
        SharedIoState::close
          closed = true
          cancel_token.cancel          // input 立刻退出
          flush_notify / read / write wake
  → 应用 Drop 自己的 owner KcpStream（若仍持有）
        owns_connection → close() 幂等
  → map 已无克隆 → SharedIoState Drop → PeerTransport Drop
        queue.mark_closed() → 唤醒仍阻塞的 recv
  → PeerQueue Drop → 未消费包交给 allocator（不进 bufpool）
```

**合理。** `remove_peer` 显式 `close()` 而不是裸 remove，避免只掉 map 项却留下 flush task（注释与实现一致）。

### 3.2 应用只 Drop owner、不调 `remove_peer`

```text
owner Drop → close()（input/flush 退出）
map 克隆仍在 → SharedIoState / PeerTransport / queue 仍存活
sweep（≤ SWEEP_INTERVAL=1s）发现 is_closed → remove + close（幂等）
  → 之后 Arc 断开，资源释放
```

**有界滞留（约 1s），不是泄漏。** 依赖 `sweep` 能扫到该条目 → 见 P2 的 `MAX_SCAN`。

### 3.3 Listener `close()` / `Drop`

```text
close():
  closed = true
  stop.cancel()          // 打断 rx 的 recv_from  ✅
                         // 同时打断 spawn_tx     ❌ P1
  accept_notify.notify_waiters()

Drop:
  close()
  _rx / _tx JoinHandle Drop → 仅 detach，不 abort
```

文档写明 “Existing `KcpStream`s are unaffected”，但 **tx 一死，存活会话的 `send_all` 失去唤醒源**（P1）。

### 3.4 `into_split`

```text
into_split: owns_connection = false
Lifecycle（两半共享）最后一半 Drop → close()
```
**正确**，不会双关，也不会泄漏。

---

## 4. 问题详情

### P1 — `close`/`Drop` 后 `send_all` 可能永久挂起（新引入）

**位置**

- `sharded.rs` `KcpListener::close` → `self.stop.cancel()`
- `sharded.rs` `spawn_tx`：`race(stop.cancelled(), socket.writable())`，退出前只 `notify_waiters()` 一次
- `transport.rs` `PeerTransport::send_all`：

```rust
if sent == 0 {
    match &self.tx_ready {
        Some(ready) => ready.notified().await, // 无超时，无兜底
        None => self.socket.writable().await?,
    }
    continue;
}
```

**触发条件**

1. 某会话发送时内核 send buffer 满（`try_send_batch_to` → `Ok(0)`）  
2. 此时调用 `KcpListener::close()` 或 drop listener  
3. `stop.cancel()` 使 `spawn_tx` 退出  
4. `send_all` 落在 `tx_ready.notified()` 上，且：
   - 未接住 tx 退出前那一次 `notify_waiters`，或  
   - 唤醒后重试仍 `Ok(0)`（缓冲仍满）

则 **该连接的 flush / `write_all` 永久阻塞**，并持有 `is_sending`，同连接后续发送全部排队堵死。

**为何是回归**：`main` 发送方自己等 socket readiness，没有“共享唤醒者先死”结构。  
**与文档冲突**：`close()` 声称不影响已有 `KcpStream`。

**修复建议（两处都做）**

1. **拆 token**：`stop_rx` 仅给 `spawn_rx`（`close()` 取消）；`stop_tx` 在 **Listener `Drop` 且明确接受会话进入拆除** 时取消，或干脆让 tx 活到进程/会话清空。  
2. **`send_all` 兜底**：`tx_ready` 等待加超时，超时后 `socket.writable()`；或 `race(tx_ready, timeout)`，避免无限等已死 task。

**验收测试**

- 两连接同 listener 持续 write，制造 `WouldBlock` 后 `listener.close()`，断言两边 `write`/flush 在有限时间内返回（成功或错误），进程不挂。  
- `close()` 后新建 `accept` 立即失败，已有流仍可收发（与文档一致）。

---

### P2 — close 后队列包不回收，内存随 map 滞留

**位置**：`PeerQueue::mark_closed`、input loop 退出路径、`PeerQueue` 无 `Drop` drain。

**行为**

- input loop 被 `cancel_token` 打断后直接 `break`，**不 drain** `PeerQueue`。  
- `mark_closed` 只 `closed=true` + `notify_waiters`，不 `recycle_buf`。  
- `Session` 同时持有 `queue` 与 `conn` 克隆；map 不删则 `SharedIoState`→`PeerTransport`→`queue` 整条都在。

**上界**：`SESSION_INBOX_CAP`（默认 2048）× `MAX_DATAGRAM`（2048）≈ **4 MiB/会话** 的最坏滞留；正常流量远小于此。  
**窗口**：约 1s（sweep）或直到 `remove_peer`；`MAX_SCAN` 会让大 map 更久（P2b）。

**不是无界泄漏**，但 close 风暴下会有内存尖峰，bufpool 也收不到这些块（最终只回到 allocator）。

**修复建议**

- `mark_closed()` 持锁清空 `packets`，逐个 `recycle_buf`。  
- input loop / `KcpStream::close` 路径同样 drain 一次。  
- `push`：`is_closed()` 时直接 `recycle_buf(pkt)` 并返回 `false`。

---

### P2b — `sweep` 只扫前 4096 条

```rust
const MAX_SCAN: usize = 4096;
sessions.iter().take(MAX_SCAN)
```

`HashMap` 迭代序不稳定。`max_sessions > 4096` 时，排在后面的 `is_closed` 会话可能长期不被摘掉，放大 P2 的滞留窗口。

**建议**：环形游标轮转扫，或 closed 时在 `close` 回调里直接 `sessions.remove`（需 listener 弱引用）；至少把 `MAX_SCAN` 与 `max_sessions` 对齐。

---

### P3 — 杂项

| 项 | 说明 | 建议 |
|----|------|------|
| `push` 不看 `closed` | mark_closed 后仍可入队 | 拒绝并 recycle |
| `close()` 后 in-flight build 仍 `pending.push_back` | `accept()` 对非空 pending 仍会弹出（先于 closed 判断） | build 完成时若 `closed` 则 `conn.close()` 不入队 |
| tx `notify_waiters` + `yield_now` | 缓冲仍满时全员惊群重试 | 可接受；长期可改为按连接积压的 tx 队列 |
| `feed_raw_*` / `drain_and_flush_tx` dead code | 编译警告 | 另 PR 删除 |
| `WorkerPoolLimits` 字段名 | 仍带 `worker_*` | 0.3.0 更名 |

---

## 5. 明确“不是 bug”的设计点

1. **map 中的 `KcpStream` 一律 `owns_connection=false`**（`Clone` 保证）。裸 `remove` 不会 close 流——所以所有 remove 点都必须 `conn.close()`；当前 `remove_peer` / `sweep` / `deliver` / `evict_stale` **都做了**。  
2. **`close()` 不杀已 accept 的流**。会话死亡靠上层、`is_dead`、`idle_timeout` 或 `remove_peer`。与 TcpListener 心智一致。  
3. **JoinHandle Drop 只 detach**。依赖 cancel/close 让 task 自退出；在 P1 修好后可接受。  
4. **building generation**。慢 build 不得覆盖替换会话；超时 `mark_closed` 握手残留。  
5. **发送锁短临界区**。`WouldBlock` 先放锁再等，不堵其它连接的 syscall。

---

## 6. 生产调用点核对

| 调用 | 位置 | 评价 |
|------|------|------|
| `listener.close()` | `kcptun-server/src/app.rs:507` | 优雅退出；**会触发 P1**（若退出瞬间仍有发送） |
| `session.close()` + `listener.remove_peer(peer)` | `kcptun-server/src/server.rs:134–136` | ✅ 正确成对 |
| 无 `remove_peer` 的库用法 | 测试 / 外部用户 | 依赖 sweep；需文档写清 |

---

## 7. 测试缺口

已有：加密 listener echo、坏包拒绝、多 peer、accept/close 基础路径。

缺失（按优先级）：

1. **close 时在途发送可结束**（锁死 P1）  
2. close 后 `session_count` 在 2s 内归零（锁 sweep/`is_closed`）  
3. close 后 queue 包进入 bufpool / 不再增长（锁 P2）  
4. 双连接写满发送缓冲时另一连接仍能发出（公平性，方案原验收）  
5. `into_split` 一半 Drop 不关、两半都 Drop 必关  

---

## 8. 修复顺序建议

1. **P1**：拆 `stop_rx`/`stop_tx` + `send_all` 超时/writable 兜底 + 测试（合入阻断）  
2. **P2**：`mark_closed`/input 退出 drain + recycle；`push` 看 `closed`  
3. **P2b**：sweep 轮转或与 `max_sessions` 对齐  
4. P3 / dead code / 更名：随后清理 PR  

---

## 9. 一句话

**close 语义和 map 回收是齐的，没有无界泄漏；但 `close()` 与 tx task 共用取消令牌，会让仍在发送的连接在 `WouldBlock` 后永久睡在已经死掉的 `tx_ready` 上——这是本分支相对 main 的新回归，必须先修再合。**
