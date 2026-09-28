# BUGREPORT: `close()` 后 in-flight build 仍可写入 `pending` 并 `accept` 弹出

- 日期：2026-09
- 分支：`feat/session-task-listener`（核对后仍未修）
- 组件：`kcp-rs/src/sharded.rs`（`deliver` 的 build task → `pending.push_back`；`accept`/`try_accept`）
- 严重级别：P3（语义瑕疵；有界）
- 状态：**未修复**

## 现象

`KcpListener::close()` 已改为**只停止 accept**（rx/tx 仍在，符合“已有流不受影响”）。但：

1. `close()` 前已进入 building 的 task，完成后仍执行 `pending.push_back(PendingAccept { .. })`（`sharded.rs` ~755），**不检查 `closed`**。  
2. `accept()` / `try_accept()` 在 `pending` **非空时优先弹出**，**先于** `closed` 判断——因此 `close()` 之后仍可能 `accept` 成功。

这与注释 “Stop accepting new connections” 不完全一致。

## 根因

```text
build 完成:
  sessions.insert(...)
  building.remove(...)
  pending.push_back(...)      // 无 closed 检查
  accept_notify.notify_one()

accept:
  if let Some(v) = pending.pop_front() { return Ok(v) }  // 先弹 pending
  if closed { return Err(ConnectionAborted) }
```

## 触发条件

1. 新 peer 触发 build，`KcpStream::build().await` 未完成  
2. 此时调用 `listener.close()`  
3. build 完成后调用方再次 `accept()` / 轮询 `try_accept()`

结果：`close()` 之后仍拿到一条新连接；若应用已进入拆除流程，可能误当作“多出来的连接”或造成连接泄漏（取决于上层是否处理）。

无人再 `accept` 时：`pending` 在 listener 相关 Arc 掉完后 Drop，owner `KcpStream` Drop → `close()`，**不泄漏任务**，只是短暂停留。

## 期望

- `close()` 之后：**不再**产生新的可 `accept` 连接。  
- in-flight build 若在 `closed` 后完成：`conn.close()`，不进入 `sessions`/`pending`（或进入后立刻回收）。  
- `accept`/`try_accept`：`closed` 为真时应**丢弃** pending 或明确文档化“仍可取走 close 前已建立的连接”。

## 修复建议

```rust
// build 完成、generation 检查通过后：
if closed.load(Ordering::Acquire) {
    drop(b);
    conn.close();
    return;
}
// 再 insert + push_back

// accept / try_accept：
if self.closed.load(Ordering::Acquire) {
    // 方案 A：排空并 close 全部 pending，再返回 ConnectionAborted
    // 方案 B（更简）：仍允许取走，但文档写明 close 不撤销已建连接
}
```

推荐方案 A + 文档：`close()` = 停止 accept + 拒绝新建；已 `accept` 到手的流不受影响。

## 验收

- 测试：build 极慢（mock transport 延迟 build）→ `close()` → build 完成后 `try_accept` 为 `Err(ConnectionAborted)` 或 `Ok(None)` 且 `session_count==0`。  
- `close()` 前已 `accept` 的 echo 流仍可用（与 `2f6abf6a` 语义一致）。
