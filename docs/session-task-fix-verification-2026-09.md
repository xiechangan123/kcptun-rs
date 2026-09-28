# 核对（五）：验收测试与小清理补全

- 时间：2026-09-24
- 基线：`ede5a01f` + 本轮未提交改动
- 回归：`cargo test -p kcp-rs --features async --lib --test kcpstream_listener --test listener_crypto_gate`
  - lib 86 passed
  - `kcpstream_listener` 16 passed
  - `listener_crypto_gate` 3 passed

## 本轮补了什么

### 代码小清理

| 项 | 处理 |
|----|------|
| `sharded.rs` 过时注释 | 已改：`discard_pending` 现在会碰 map，注释同步 |
| `remove_peer` 不清理 pending | **已修**：一并 drop 该 peer 的未 accept 连接，`accept` 不会再弹出已拆除的流 |
| `discard_pending` 误删同 peer 的新 session | **已修**：先 `close` pending 连接，仅当 map 里的 session 也已 `is_closed` 才 `sessions.remove`，避免 stale backlog 删掉 re-dial 后的活跃会话 |
| `testing_build_delay` 测试钩子 | 新增（`#[doc(hidden)]`），在 build 完成后、发布前 hold，用来确定性测 in-flight build |

### 验收测试（原报告缺口）

| # | 原验收 | 新测试 | 状态 |
|---|--------|--------|------|
| 1 | `mark_closed` 后 `push` 返回 `false` 且回到 bufpool | `mark_closed_recycles_backlog_and_refuses_push` | ✅ |
| 2 | close 与 deliver 并发下已关闭队列不增长 | `push_racing_mark_closed_leaves_queue_empty` | ✅ |
| 3 | N>4096 会话 close 后 sweep 归零 | `sweep_reaps_closed_sessions_without_remove_peer`（N=5000） | ✅ |
| 4 | 慢 build + `close()` 后 `try_accept` 拒绝且 `session_count==0` | `listener_close_discards_inflight_build` | ✅ |
| 5 | （加强）发送缓冲打满的 P1 | 仍靠 `listener_close_does_not_hang_an_inflight_write` | ⚠️ 保留原状 |

另补：`remove_peer_drops_unaccepted_connection`（覆盖 `remove_peer` 清 pending + 同址重拨）。

### 写测试时发现、已用测试锁住的行为

**re-dial 替换会话**：`deliver` 看到 `is_closed` 的 session 会 `remove` 后再建一条**新的**。所以「accept + close N 条」之后，仍在发包的客户端会为同 peer 再开 session；sweep 不会回收这些**活着的**替换会话（不是 bug，是 re-dial 语义）。`sweep_reaps_*` 测试因此：

1. close 后 `drop(clients)` 停掉后续入站；
2. 再 drain 掉最后一刻挤进来的替换 accept；
3. `idle_timeout=200ms` 作兜底。

N=5000（旧 `MAX_SCAN=4096` 之上）才能真正锁住「全量扫描」而不是「只扫前缀」。

## 新增测试清单

`kcp-rs/src/transport.rs`（单元）

- `mark_closed_recycles_backlog_and_refuses_push`
- `push_racing_mark_closed_leaves_queue_empty`
- `recycle_drops_undersized_buffers`

`kcp-rs/tests/kcpstream_listener.rs`（集成）

- `listener_close_discards_inflight_build`
- `remove_peer_drops_unaccepted_connection`
- `sweep_reaps_closed_sessions_without_remove_peer`

## 仍开着的口子

1. **P1 缓冲打满场景**：localhost 上 8MiB 写入可能在 `close()` 前就完成，`listener_close_does_not_hang_an_inflight_write` 对「发送缓冲满」覆盖偏弱。若要硬锁，需要 mock 传输在 `try_send_batch_to` 返回 `Ok(0)`。
2. **`conn.rs` 的 `MAX_IDLE_UPDATE_MS` dead_code 警告**：来自工作区里另一处 ACK 延迟改动（`conn/endpoint.rs`，非本轮范围），该常量最后一个使用点被删掉了。按仓库规范未动无关死代码，但警告还在。
3. **`conn/endpoint.rs` 有非本轮的 ACK 立即 flush 改动**（acknodelay off 时随 inbound burst 发 ACK，避免每个 ACK 等一个 interval tick）。与 listener close 生命周期无关，合入时请单独看。

## 总判

核对（四）列的「测试债 + 两处小清理」——**除 P1 缓冲打满加强测外，已全部补完**。6 项根因 + 上次 2 个残留 + `remove_peer`/pending + `discard_pending` 误删，代码和测试都齐了，可以按「全部修复」收口。
