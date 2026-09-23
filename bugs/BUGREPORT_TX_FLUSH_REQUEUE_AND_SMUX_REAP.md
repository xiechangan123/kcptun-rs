# BUGREPORT：TX 超时重排重发已发前缀 + 提交 2660e262 复审（SMUX reap / token 记账）

> 分析日期：2026-09-20
> 复审对象：`2660e262` fix(kcp,smux): stop session mute under proxy load
> 方法：GitNexus CLI 重建索引后 impact/detect-changes + 静态走查 + `split_off` 语义实测 + 三 crate 测试与 clippy

## 一、结论摘要

`2660e262` 的三处修复方向都正确，编译通过、测试全绿、无新增 lint、无 wire 变更。但复审发现 **3 个真 bug**（其中 1 个由本次提交引入、2 个为既存但在本次改动范围内）和 3 项残留缺口：

1. **【本次引入】`send_drained_batch` 超时重排把"已交给内核的前缀"再发一遍**（§二）。在 Linux `sendmmsg` 部分成功 / 非 Linux `send_batch` 中途 park 的情况下，重复段会在拥塞链路上额外消耗带宽，并通过重复 ACK 诱发 `fastresend=2` 下的虚假快速重传。
2. **【既存，`b19fbf7c`】`spawn_send_remainder` 超时分支的 `unsent` / `already_sent` 语义相反**（§三）。代码实际丢弃"未发送的后缀"、重发"已发送的前缀"，与紧邻注释和方法文档描述的行为完全颠倒。已用最小程序实测确认 `Vec::split_off` 语义。本次新写的 `send_drained_batch` 正是照抄了这一语义。
3. **【本次引入的不完整修复】`push_data_bytes` 失败时 `return_tokens` 是不可达死代码**（§四）。`Stream::push_data_bytes` 没有失败路径，恒返回 `Ok(())`。提交信息与 handoff §3.3 声称的"push_data 失败时归还 token"实际未执行；缺陷 #3 的真实修复只有 `remove_stream` 一处。
4. **【既存，被本次放大】`rebuild_snapshot` 丢更新竞态**（§五）。`remove_stream` 每个 id 调一次 `rebuild_snapshot`，与 `accept_stream` 的并发重建可交错，导致新 accept 的流从快照中消失，直到下次重建。
5. **【残留缺口】token 回收仍晚于 Go**（§六）。Go 在 `streamClosed` 即回收；Rust 在 reap 时回收，linger 分支最长 30s。
6. **【设计耦合，非 bug】50ms 上界把"永久静默"换成"约 20Hz 重试"**（§七），拥塞期间输入循环与 KCP 维护节奏同步下降到约 20Hz；FEC 路径超时丢弃整批，恢复要等一个 RTO。

**未发现**破坏 wire 兼容、破坏构建或导致测试失败的问题。**未运行** `make stress`（AGENTS 要求 flush/lock/session 改动后必跑）与 E2E（需用户确认）。

## 二、BUG-1【本次引入】超时重排重发已发送前缀

**严重性**：中（带宽浪费 + 虚假快速重传；非正确性损坏）
**状态**：已修（`tx_delivered` 原子计数器 + 超时按已交付数切分）

### 证据

超时后无条件把整批放回队首：

```rust
Err(_) => {
    // Timed out inside flush_tx_batch. Token still held by the
    // caller's SendToken; it drops after we return.
    if used_fec {
        self.recycle_raw_packets(packets);
    } else {
        self.requeue_raw_packets_front(packets);   // ← 整批，含已发前缀
    }
}
```

但 `flush_tx_batch` 在 park 之前可能已经交付了一个前缀：

- 无 FEC 路径（`endpoint.rs:510-527`）：`try_send_batch` 返回 `Ok(sent)` 且 `sent < len` 时，走 `self.send_packets(&packets[sent.min(len)..]).await` —— 即前 `sent` 个包**已经进了内核**，随后才在 `writable()` 上 park。
- 平台行为已核对 `knet-rs/src/net/tokio.rs:80-115`：Linux 走 `sendmmsg` 循环（可部分成功），非 Linux 走逐包 `try_send` 循环，两者都会在 `writable().await` 处 park，因此**两个平台都存在"已交付前缀 + 超时"的组合**。

`try_send_batch` 的"部分成功"在 Linux 上是常态而非异常（`sendmmsg` 允许只接受前缀）。

### 影响

`raw_packets` 里装的是 KCP 段，接收端会按 `rcv_nxt` 去重，因此**不会**造成 SMUX 层重复数据。但：

1. 重复段在**已经拥塞**的链路上再占一份带宽——而拥塞正是触发 50ms 超时的前提。
2. 接收端对重复段回重复 ACK，我方 `parse_fastack` 的 `duplicate++` 累加；生产当前 `--mode fast2`（`fastresend=2`），可能触发对 `snd_una` 之后那一段的**虚假快速重传**。
3. `flush_tx_batch` 里的 `OutPkts` 会对同一批重复计数（`endpoint.rs:502/505`）。

### 建议修法

按已发计数切分，只重排未发送的后缀。注意需先把已发数量从 `flush_tx_batch` 透出（当前它只返回 `io::Result<()>`，前缀计数在内部丢失）：

```rust
// flush_tx_batch 返回 Result<usize, io::Error>（已交付的段数），
// 超时分支只重排 packets[sent..]。
```

若不改签名，退而求其次：超时分支整批重排但**保证重排不早于一次 `writable()` 成功**，避免在不可写时反复重发。

## 三、BUG-2【既存 `b19fbf7c`】`spawn_send_remainder` 超时分支变量语义颠倒

**严重性**：中（未发送数据被静默丢弃、已发送数据被重发）
**状态**：已修（`split_off` 变量语义纠正：前缀回收、后缀重排）
**位置**：`kcp-rs/src/conn/endpoint.rs:456-468`

### 证据

```rust
let split = sent.min(packets.len());
let mut unsent = packets;
let already_sent = unsent.split_off(split);
// 注释：Prefix reached the kernel; only its capacity is worth
//       recycling. The suffix is requeued for retry.
shared.recycle_raw_packets(already_sent);   // 实际回收的是 [sent..]（未发送的后缀）
shared.requeue_raw_packets_front(unsent);   // 实际重排的是 [0..sent]（已发送的前缀）
```

`Vec::split_off(at)` 返回 `[at, len)`，原 vec 留下 `[0, at)`。最小程序实测：

```
$ rustc -o /tmp/splittest /tmp/splittest.rs && /tmp/splittest
variable `unsent`     holds: ["p0", "p1"]      # 已交付内核的前缀
variable `already_sent` holds: ["p2", "p3", "p4"]  # 未发送的后缀
```

即：**代码与紧邻注释、方法文档（`endpoint.rs:403-411`）描述的行为完全相反**。文档承诺"未发送的后缀放回队列，比 KCP RTO 更早重试"，实际是丢弃未发送后缀（只能等 RTO）并重发已发送前缀（重复 + 可能虚假快速重传）。

### 影响

- 未发送的后缀被 `recycle` 掉 → 只能靠 KCP RTO 重传，延迟 = 一个 RTO 以上（与文档承诺的"立即重试"相反）。
- 已发送前缀被重排 → 与 BUG-1 同类的重复段后果。

触发路径：`drain_and_flush_tx` 的 `Ok(sent)` 部分发送分支与 `WouldBlock` 分支（`endpoint.rs:373-385`）→ `spawn_send_remainder` → 50ms 超时。

### 建议修法

```rust
let split = sent.min(packets.len());
let mut unsent = packets;              // 语义应为"未发送"
let already_sent = unsent.split_off(split);
// 现在 already_sent = [split..] 才是"未发送"，变量名与用法需一并纠正：
shared.recycle_raw_packets(unsent);            // [0..sent] 已交付，回收容量
shared.requeue_raw_packets_front(already_sent); // [sent..] 未发送，放回重试
```

（改完需同步 `endpoint.rs:448-452` 与 `403-411` 的注释，避免再次误导。）

## 四、BUG-3【本次引入】`push_data_bytes` 失败分支不可达，修复未生效

**严重性**：低（无害死代码），但**提交信息与 handoff 文档高估了修复覆盖面**
**状态**：已修（`push_data_bytes` 加 `max_recv_buf` 容量上限检查，溢出返回 `BufferOverflow`）
**位置**：`smux-rs/src/stream.rs:394-411`（`push_data_bytes`）；`smux-rs/src/session.rs:489/509` 的 `Err` 分支现为有效防线

### 证据

`Stream::push_data_bytes`（`smux-rs/src/stream.rs:394-406`）没有任何失败路径：

```rust
pub fn push_data_bytes(&self, data: Bytes) -> Result<(), StreamError> {
    if data.is_empty() { return Ok(()); }
    let n = data.len();
    { let mut inner = self.recv.lock(); inner.recv.push_back(data); }  // 无界 VecDeque
    self.recv_buf_bytes_avail.fetch_add(n, Ordering::Relaxed);
    self.wakeup_reader();
    Ok(())
}
```

全仓库只有这一个定义（`grep -rn "fn push_data_bytes"` 仅命中一处）。因此两个 `Err(e)` 分支永不进入，`return_tokens(frame.data.len())` 永不执行，那条 `log::warn!("push_data overflow ...")` 也永远不会打印。

提交信息 "return tokens when push_data fails" 与 handoff §3.3 第 2 条描述的动作**实际什么都没做**。缺陷 #3 的真实修复只有 `kcptun_session.rs` 的 `remove_stream` 接线一处——该处是正确的。

### 影响

无运行时危害（防御性代码）。风险在于**文档与提交记录声称了一条并不存在的防护**，后续维护者若据此认为"push 失败路径已被覆盖"会误判。同时它掩盖了真实问题：`push_data_bytes` 把数据放进**无界** `VecDeque`，真正的过载保护只依赖会话级 `token_bucket`。

### 建议修法

二选一：(a) 删除死分支并在 handoff/提交信息中更正；(b) 给 `push_data_bytes` 加真实容量上限（`max_stream_buffer`）使其真的会失败，届时 `return_tokens` 才成为有效防线。推荐 (b) 与 §六 的提前回收一并设计。

## 五、BUG-4【既存，被本次放大】`rebuild_snapshot` 丢更新竞态

**严重性**：中低（概率低，后果为流数据短暂不被排空）
**状态**：已修（收集 + 存快照合并到同一把 `streams` 锁内，消除交错写入）
**位置**：`smux-rs/src/session.rs:268-280`

### 证据

`rebuild_snapshot` 是"加锁→收集→解锁→存快照"，两步之间没有互斥：

```rust
fn rebuild_snapshot(&self) {
    let pairs: Vec<(u32, Arc<Stream>)> = {
        let streams = self.streams.lock();
        streams.iter().map(|(&id, s)| (id, s.clone())).collect()
    };                                              // ← 此处已释放 streams 锁
    let new_snapshot = Arc::from(pairs.into_boxed_slice());
    *self.stream_snapshot.lock() = new_snapshot;     // ← 后写入者可能携带更旧的集合
}
```

交错：T1（write_loop 的 `remove_stream`）收集 `{A,B}` 后释放锁；T2（read_loop 的 `accept_stream`）插入 C、收集 `{A,B,C}` 并存入；T1 随后存入 `{A,B}` → **C 从快照中消失**，其 PSH 在下次重建前不会被 `prepare_outbound_into_controlled` 排空。

本次提交把 `remove_stream` 变成 write_loop 每 tick 都可能调用的热路径，且**每个被 reap 的 id 都调一次 `rebuild_snapshot`**，调用频率显著上升，竞态窗口的命中概率随之上升。根因是既存的，但放大效应是本次引入的。

### 建议修法

把"收集 + 存快照"合并到同一把 `streams` 锁内（或给 `stream_snapshot` 加版本号 / 用 `RwLock` + 单一写入点），并把批量 reap 的 K 次重建收敛为 1 次（见 BUG-5）。

## 六、残留缺口：token 回收仍晚于 Go；reap 逻辑重复实现

**状态**：部分修复。§6.2 的 reap 重复实现已收敛到 `reap_stale_streams`（含 `pending_send()==0` 守卫 + linger 分支 FIN 补发）；§6.1 的 token 回收时机仍晚于 Go（需在 `Stream::close` 时提前回收，当前仍在 reap 时回收）。

**位置**：`kcptun-common/src/kcptun_session.rs:962-982`（write_loop reap）；对照 `smux-rs/src/session.rs:643-690`（`reap_stale_streams`）

### 6.1 回收时机

Go 的 smux 在 `streamClosed`（即 `Close()` 时）就 `recycleTokens`；本移植在 **reap 时**才回收：

- 分支 1 `local && remote && fin_sent`：等双方 FIN + 我方 FIN 已发；
- 分支 2 `local && pending_send()==0 && elapsed >= 30s`：等 **30 秒 linger**。

因此浏览器批量取消流的场景下，bucket 仍可能被压到接近 0 长达 30s，`has_receive_capacity()` 仍会短暂为 false、`read_loop` 仍会短暂 park。本次修复把"永久泄漏"变成"最长 30s 延迟回收"，**没有把回收提前到 close**。若现场仍复现 `bucket` 贴近 0 的停顿，应优先怀疑这里。

### 6.2 reap 重复实现且更贵

smux 已提供 `reap_stale_streams`（`session.rs:639-684`）：一次加锁、一次 `rebuild_snapshot`、回收 token，**并返回 `need_fin` 供调用方补发 wire FIN**。write_loop 抄了一份扫描逻辑再逐 id 调 `remove_stream`，带来三个问题：

1. 每个 id 一次 `rebuild_snapshot` → 批量 K 个陈旧流是 O(K·N)，而 `reap_stale_streams` 只重建一次；
2. write_loop 这份**从不给 linger 分支补发 FIN**，僵尸流对端永远收不到 FIN；
3. 两份逻辑将来会漂移。

不能直接替换：write_loop 版多了 `pending_send() == 0` 这一更严格的守卫（防止丢弃未发数据），而 `reap_stale_streams` 没有。正确做法是把该守卫加进 `reap_stale_streams`，再让 write_loop 调用它。

### 6.3 澄清（不是 bug）

`remove_stream` 里的 `recycle_tokens` 会清空 `inner.recv`，但 `Stream::close()`（`stream.rs:848-869`）**在本次提交之前就已经清 recv**，reap 的筛选条件也一字未改。因此本次改动**没有**引入新的接收缓冲截断风险——行为等价，只是补上了 token 记账。

## 七、设计耦合（非 bug，但需记录）

1. **50ms 上界同时拖慢输入循环与 KCP 维护。** `try_drain_and_send` 现在会在输入循环里 await 最长 50ms（`endpoint.rs:772`）。socket 不可写且队列非空时：输入循环每轮被拖 50ms（入站 ACK 处理降到约 20 轮/秒 × 最多 `MAX_INPUT_BATCH=64` 包）；flush 循环的 fast-send 排在 KCP 维护段之前，拥塞期间 KCP flush/重传节奏也从 50–100Hz 掉到约 20Hz。相比修复前的"永久静默"这是严格改善，但这是新引入的耦合，且恰在链路拥塞时生效。GitNexus `impact send_drained_batch` 判为 **HIGH** 并点名 `spawn_input_loop`，与静态分析一致。
2. **FEC + 超时 = 整批丢弃，恢复等一个 RTO。** FEC 分支不重排是正确的（重排会被二次 expand，打乱 RS 编解码器配对），代价是 encoder 的 shard-set 序号已推进、对端解码器留下 tombstone（`MAX_SHARD_SETS=3`），这批只能等 KCP RTO。生产两端为 `10/3`，故拥塞时每 50ms 一次的丢弃都意味着 ≥1 RTT 的额外延迟——非 FEC 路径反而更"幸运"。
3. **超时后 `finish_sending()` 会立即 `notify_one`**（`endpoint.rs:312-317`，重排后队列非空），flush 循环随即重入 fast-send，形成约 20Hz 的重试环。这是"比 RTO 更早重试"的设计意图，但与第 1 条叠加后即维护节奏下降。

## 八、验证记录

| 项 | 结果 |
|---|---|
| `git status` | clean，HEAD = `2660e262` |
| `cargo test -p kcp-rs --features async --lib` | **92 passed, 0 failed**（含 `cancelled_send_releases_the_token`、`flush_tx_timeout_releases_token_and_requeues`） |
| `cargo test -p kcptun-common` | **57 passed, 0 failed** |
| `cargo test -p smux-rs` | 退出码 0 |
| `cargo clippy -p kcp-rs -p smux-rs -p kcptun-common --all-targets` | 仅既存告警：测试文件 `cfg` 拼写 `async-tokio`/`async-smol`、`kcptun_session.rs:747` 的 `assertions_on_constants` |
| `Vec::split_off` 语义实测 | 确认 §三 的变量互换 |

GitNexus（索引原停在 `36765b9`，已重建为 6801 节点 / 16758 边 / 573 流）：

| 查询 | 结果 |
|---|---|
| `detect-changes --scope compare --base-ref HEAD~1` | 18 条受影响执行流，风险自评 `critical` |
| `impact send_drained_batch --direction upstream` | **HIGH**，4 个受影响符号 / 3 条流（`spawn_flush_loop`、`spawn_input_loop`）/ 2 个模块 |
| `impact try_drain_and_send` / `finish_sending` | LOW |
| `impact push_data_bytes` | HIGH（5 个受影响） |
| `impact remove_stream` | ambiguous（2 个候选：`Session::remove_stream`、`KcptunSession::remove_stream`） |

> 注：`KcptunSession::remove_stream`（`kcptun_session.rs:293`）在 `kcptun-client`/`kcptun-server` 中**无调用方**（仅定义），生产路径上移除单个流的唯一入口就是 write_loop 的 reap。
> 注：`gitnexus analyze` 会重写 `AGENTS.md`/`CLAUDE.md` 的自动生成块，本次已 `git checkout` 还原。

## 九、未覆盖

- 无测试覆盖 write_loop → `remove_stream` 这条**接线**。`smux-rs/src/session.rs:1517` 的 `receive_window_is_exhaustible_and_recycled_with_the_stream` 测的是 `Session::remove_stream` 单体，且早于本次提交即存在。
- 无 FEC 超时路径测试（现有新测试只覆盖无 FEC 的重排）。
- 无输入循环内联发送路径测试。
- `make stress` 未运行（AGENTS 要求 flush/lock/session 改动后必跑）。
- E2E 未运行（按约定需用户确认）。

## 十、修复优先级建议

| 序 | 项 | 理由 |
|---|---|---|
| 1 | §三 `split_off` 变量互换 | 一行改动，但决定线上"丢未发送 / 重发已发送"的实际行为 |
| 2 | §二 超时重排按已发计数切分 | 消除拥塞链路上的重复段与虚假快速重传 |
| 3 | §四 死代码定性 | 更正提交信息/handoff，或补上真实的容量上限 |
| 4 | §六 reap 收敛到 `reap_stale_streams` | 一并解决 O(K·N) 重建与缺失 FIN |
| 5 | §五 `rebuild_snapshot` 竞态 | 需要改 smux 内部同步，建议随 §六 一起 |
| 6 | §七 20Hz 重试环 | 需要权衡，建议先用 `make stress` + 现场 SNMP 观察再决定 |
