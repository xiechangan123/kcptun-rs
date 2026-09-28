# BUGREPORT: `listener.close()` 杀掉 tx 后 `send_all` 可能永久挂起

- 日期：2026-09
- 分支：`feat/session-task-listener`
- 组件：`kcp-rs` `sharded.rs`（`spawn_tx` / `KcpListener::close`）、`transport.rs`（`PeerTransport::send_all`）
- 严重级别：P1（连接挂死；与 `close()` 文档语义冲突）
- 相对 `main`：**新引入**（main 无共享 tx task）

## 现象

`KcpListener::close()`（含 `Drop`）之后，已在运行的 `KcpStream` 在发送侧可能永久阻塞：`write_all` / flush 不返回，同连接后续发送被 `is_sending` 堵住。

## 根因

`close()` 对 **rx 与 tx 使用同一个** `knet::CancellationToken`：

1. `KcpListener::close` → `stop.cancel()`
2. `spawn_tx`：`race(stop.cancelled(), socket.writable())`，退出前仅 `notify_waiters()` 一次
3. `PeerTransport::send_all` 在 `try_send_batch_to` 返回 `Ok(0)`（内核发送缓冲满）时：

```rust
match &self.tx_ready {
    Some(ready) => ready.notified().await, // 无超时、无 writable 兜底
    None => self.socket.writable().await?,
}
```

tx 退出后没有再 `notify` 的一方；若未接住退出前的一次唤醒，或唤醒后重试仍 `Ok(0)`，则该 future **永久挂起**。

## 触发条件

- 会话仍在发送，且共享 socket 发送缓冲区满（高发送速率 / 对端慢）  
- 此时调用 `KcpListener::close()` 或 drop listener  
- 要求使用 listener 路径（`PeerTransport` + `tx_ready = Some`）；客户端独立 socket 走 `writable()` 不受影响  

## 复现思路

1. 单 listener + 两 `KcpStream`，持续大块 `write_all` 直到出现 `WouldBlock` 语义（发送缓冲打满）。  
2. 调用 `listener.close()`。  
3. 观察 write 任务是否在有限时间（例如 2s）内结束。  

## 期望

- `close()` 文档：“Existing `KcpStream`s are unaffected”——已有流的 read/write 应正常结束（成功或可观察错误），不得无限等。  
- `close()` 应停止 `accept` 与 rx；是否停止 tx 必须与上述语义一致。

## 修复建议

1. **拆分取消令牌**：`stop_rx` 仅用于 `spawn_rx`（`close()` 取消）；tx 不因 `close()` 退出，或仅在确认无存活发送时退出。  
2. **`send_all` 兜底**：对 `tx_ready` 加超时，超时后 `socket.writable()`；禁止在无生产者的 Notify 上无限等待。  
3. 补回归测试：close 时在途发送必须在时限内结束。

## 附带（同主题、非本 bug 本体）

- `PeerQueue::mark_closed` 不排空，input loop 退出不 drain：close 后队列包最多滞留至 sweep/remove（约 1s，上界 `inbox_cap × MTU`）。建议 drain + `recycle_buf`。  
- `sweep` `MAX_SCAN=4096` 可能推迟 closed 会话摘除。

详见：`docs/session-task-listener-close-lifecycle-review.md`。
