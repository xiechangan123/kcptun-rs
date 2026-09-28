# BUGREPORT: `sweep` 只扫 `MAX_SCAN=4096` 条会话，大 map 下 closed 会话可能晚回收

- 日期：2026-09
- 分支：`feat/session-task-listener`（核对后仍未修）
- 组件：`kcp-rs/src/sharded.rs` `sweep`
- 严重级别：P2（资源滞留窗口被拉长；放大队列内存滞留）
- 状态：**未修复**

## 现象

`sweep` 每次只看 `sessions.iter().take(MAX_SCAN)`（`MAX_SCAN = 4096`）。`HashMap` 迭代顺序不稳定，**不保证**每次都扫到同一批键。当 `max_sessions_per_worker`（或实际会话数）≫ 4096 时：

- 某条已 `is_closed` / idle 的会话可能连续多轮都不在前 4096 里；
- map 项、`PeerQueue`、`SharedIoState` 克隆因此多活很久；
- 连带放大 `mark_closed` 之前的队列包滞留（见 `BUGREPORT_QUEUE_PUSH_AFTER_CLOSED`）。

## 根因

```rust
const MAX_SCAN: usize = 4096;
sessions.iter().take(MAX_SCAN)...
```

上限与 `WorkerPoolLimits::max_sessions_per_worker` 无关；也无轮转游标。HashMap 迭代序在插入/删除后变化，存在“永远扫不到”的可能（实践上是概率性饿死，不是逻辑死循环）。

## 触发条件

1. `max_sessions_per_worker` > 4096，或运行中并发会话数超过 4096  
2. 大量短连接 close / idle（只靠 sweep，未调 `remove_peer`）  
3. 部分会话长期排在迭代序后部  

库用户不调 `remove_peer` 时更明显；`kcptun-server` 有 `remove_peer`，风险较低但仍存在（idle/dead 路径仍依赖 sweep）。

## 期望

- 任意已 `is_closed`/`is_dead`/idle 的会话应在**有界时间**内被摘除（与 `SWEEP_INTERVAL` 同阶，而不是“看运气”）。  
- `max_sessions` 调大后回收语义不应劣化。

## 修复建议（任选）

1. **轮转游标**：记录上次扫到的 peer，每轮从游标继续，`sweep` 一次可设 `max(4096, sessions.len()/k)` 或扫完整个 map（sessions 持锁只做 `clone` 键列表，已很短）。  
2. **closed 即摘除**：`KcpStream::close` / `SharedIoState::close` 回调 listener 弱引用立即 `sessions.remove`（注意锁序：勿在持 `sessions` 时再进 `building`）。  
3. 最低限度：`MAX_SCAN = max(MAX_SCAN, max_sessions_per_worker)`，并文档写明。

## 验收

- 插入 N=10k 会话，全部 `close()` 且不调 `remove_peer`，在 `SWEEP_INTERVAL * 2` 内 `session_count()==0`。  
- 回归 idle_timeout / building_timeout 行为不变。
