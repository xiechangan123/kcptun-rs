# BUGREPORT: `PeerQueue::push` 不检查 `closed`，可在 `mark_closed` 后继续入队

- 日期：2026-09
- 分支：`feat/session-task-listener`（核对 `2f6abf6a` 后仍未修）
- 组件：`kcp-rs/src/transport.rs` `PeerQueue::push`
- 严重级别：P3（有界滞留；破坏 `mark_closed` 回收保证）
- 状态：**未修复**（`mark_closed` 已 drain + `recycle_buf`，但 push 无门禁）

## 现象

`mark_closed()` 会清空 `packets` 并 `recycle_buf`。若其后仍有 `push`，新入队的数据报**不会再被回收到 bufpool**，一直留在 `PeerQueue` 直到 `PeerQueue` Drop（只回到 allocator，不进池）。

## 根因

```rust
pub(crate) fn push(&self, pkt: Vec<u8>, cap: usize) -> bool {
    let mut buffers = self.buffers.lock();
    if buffers.packets.len() >= cap { ... }
    buffers.packets.push_back(pkt);  // 不看 self.closed
    ...
}
```

`mark_closed` 与 `push` 之间无 happens-before 约束。调用方（`deliver`）在读到 session/queue 后、`push` 前，对端会话可能已 `mark_closed`（`PeerTransport::drop`、building 超时）。

## 触发条件

1. 会话 close / transport drop / building 超时 → `mark_closed`（已排空）  
2. 同一时刻 rx `deliver` 仍持有该 `queue` 的 `Arc` 并 `push`  
3. 入队包无人 pop（input loop 已退出或 queue 已关）

上界：单队列 `SESSION_INBOX_CAP`（默认 2048）包；会话 churn 高时表现为 bufpool 不回流、allocator 压力变大，不是无界泄漏。

## 期望

`is_closed()` 时 `push` 应 `recycle_buf(pkt)` 并返回 `false`（与 cap 满时一致）；或在 `push` 与 `mark_closed` 之间用同一把锁做二次检查。

## 修复建议

```rust
pub(crate) fn push(&self, pkt: Vec<u8>, cap: usize) -> bool {
    let mut buffers = self.buffers.lock();
    if self.closed.load(Ordering::Acquire) || buffers.packets.len() >= cap {
        drop(buffers);
        crate::sharded::recycle_buf(pkt);
        return false;
    }
    ...
}
```

注意：`closed` 的 store 在 `mark_closed` 里可与 `buffers` 锁重排到同临界区，避免 TOCTOU。

## 验收

- 单测：`mark_closed` 后 `push` 返回 `false`，且 datagram 进入 bufpool（`acquire_buf` 可取回）。  
- 并发：close 与 deliver 竞态下无“已关闭队列仍增长”的包。
