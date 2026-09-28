# 性能分析：保持 session-as-tasks，当前分支为什么比 main 慢

日期：2026-09-24。对照物是 `/Users/yangzhiqin/Desktop/kcptun-rs` 的 `main`（`8e8f8760`），
不是 `master`。数字全部来自 `docs/handoff-session-task-listener-2026-09-24.md` 里
`192.168.0.18`（4 核，内核 3.10）的实测。

## 结论

慢不是因为 KCP、加密或发送本身。同一份代码在这台 Mac 上单连接是 0.27ms、49 MB/s，
和 main 持平；在那台 Linux 上是 6–13ms、37–45 MB/s，main 是 0.17–0.21ms、50–63 MB/s。
打点把 13–17ms 的往返拆开后，KCP 计算不到 2ms、`try_send` 不到 1ms，剩下的全是
**跨任务唤醒**。内核 3.10 的 timer 粒度把一次唤醒放大到数毫秒，main 的 worker 线程
在 `feed_raw_batch` 里同步跑完，所以根本不存在这次唤醒。

因此优化方向只有一个：**让一条数据报的「收 → KCP → ACK 发出」在同一个 task 的同一次
poll 里跑完，中间不 `await`、不 `notify` 别人**。session-as-tasks 的骨架（一个 rx task、
每条会话自己的 flush loop、共享 socket 的 tx task）不用动。

## 数字

32 连接，三轮交替，main 对已提交的 `ede5a01f`（**不含**工作区里的未提交改动）：

| | main | 新实现 |
|---|---|---|
| 吞吐（中位数） | 79.95 MB/s | 56.28 MB/s |
| 延迟（中位数） | 0.23 ms | 9.00 ms |

单连接同样复现，所以不是 32 路互相抢锁。吞吐差（约 0.7×）基本是延迟差的下游结果：
窗口开得慢，管道里装不满。

## 架构差在哪

main 的 worker 是自建 OS 线程，自己的 current-thread runtime，`background_input(false)`，
收到的批在 `feed_raw_batch` 里同步解密、跑 KCP、把 ACK 送进 socket。收和回在同一条
调用栈上。

当前分支把这件事拆成了四段，每段之间都是一次调度：

```
rx task 收包
  → queue.push + notify          （唤醒 1：会话的 input task）
    → input task 跑 KCP
      → ACK 留在 acklist          （acknodelay 默认关）
        → flush loop 下一次唤醒   （唤醒 2：interval 钳到 10ms）
          → try_send 碰到 WouldBlock
            → 等 tx task 的 writable()（唤醒 3）
```

单连接测到的 6–13ms 就是前两段。第三段只在发送缓冲区满时出现，解释不了空载单连接。

## Linux 实测（2026-09-24，含未提交改动）

`make linux` 交叉编译后只覆盖了 `192.168.0.18:/tmp/kcpbench-cmp/new/`
（client `b1ac4044…`、server `7997d121…`，14:11）。`old/` 没动，仍是 main 的
`8e8f8760`（11:48 的二进制）。机器上 45100–48080 端口段有一批不属于这次的
`python3` 进程在听，一个都没动；测试只用了 49100 以上的空闲端口，跑完确认
没有残留进程。

单连接，`--shards 1 --conn 1`，aes / nocomp / fast / wnd 1024 / smuxver 2，
8MB，三轮交替（`/tmp/kcpbench-cmp/ab-single3-141750.log`）：

| 轮次 | main MB/s | 新 MB/s | main 延迟 | 新延迟 |
|---|---|---|---|---|
| 1 | 18.82 | 20.04 | 0.80 ms | 0.14 ms |
| 2 | 20.28 | 19.82 | 0.21 ms | 0.17 ms |
| 3 | 19.35 | 20.19 | 0.30 ms | 0.17 ms |

延迟目标（<1ms）达到了，而且稳定比 main 低：三轮中位数 0.17ms 对 0.30ms。
吞吐两者相同，约 20 MB/s。这比 handoff 里 main 的 50–63 MB/s 低，但**两边
一样低**，所以是今天这台机器的状态（负载此前就到过 2–6），不是这次改动的回退。

两个测量口径上的坑，记录下来免得下次再踩：

- 不带 `--shards 1` 时服务端按 CPU 数建 4 个 `SO_REUSEPORT` socket。客户端
  `--conn 8` 的 8 条 UDP 连接被内核按 4 元组哈希拆到 4 个 shard，server 日志里
  半分钟内出现 7 个 `new shared KCP session`，其中只有一个真正接到了 echo
  目标。新实现的会话因此被 watchdog 以 `kcp_dead` 关掉，吞吐记成 0。main
  的二进制没有露出同样的症状。单连接对比必须固定 `--shards 1 --conn 1`。
- 用固定 `sleep` 等 echo 端口就绪会让预热传输读到一半被重置（日志里
  `only received 32768/2097152`）。改成 `ss` 轮询端口就绪后不再出现。

32 连接没有跑：用户明确要求不做压力测试，这台机器上有正常服务。

## 已经做了但原先还没在 Linux 上测过的

工作区未提交的三处改动正好对着前两段，而且都没动架构：

| 改动 | 位置 | 去掉的是 |
|---|---|---|
| `deliver` 对已建立会话直接调 `conn.feed_raw_single` | `kcp-rs/src/sharded.rs` | 唤醒 1。rx task 当场解密、`process_inbound_batch`、`drain_and_flush_tx` |
| `KCP::flush_acks_only`，`acknodelay` 关且 `acklist` 不空时调用 | `kcp-rs/src/kcp.rs`、`conn/endpoint.rs` | 唤醒 2。ACK 跟着这个 burst 走，不等 flush loop 的 10ms |
| flush loop 去掉「还没到截止时间就预约一个 interval 再返回」 | `conn/endpoint.rs` | 唤醒 2 的另一半。第一次被唤醒时直接刷 |

上次编进 `/tmp/kcpbench-cmp/new/` 的二进制是 12:48 的，早于这些改动。Mac 上测到
0.27ms 的那次也早于 `deliver` 的内联。所以现在缺的是一次测量，不是更多代码。

**先做的事**：`make linux`，只覆盖 `192.168.0.18:/tmp/kcpbench-cmp/new/`（`old` 是
main，不要重编），单连接跑 `throughput.py --data-mb 8 --connections 1
--latency-iterations 30 --timeout-seconds 30`，新旧各两次交替，端口从 47000 起、
避开 `ss -lntu` 里已有的。目标是延迟回到 1ms 内、吞吐接近 main 的约 50 MB/s。
**不许动那台机器上已有的服务，只许动 `/tmp/kcpbench-cmp/` 下自己拉起的进程。**

## 如果测完还差，按这个顺序看

每一条都留在现有架构里，而且每一条都要先有数字再改。

### 1. `deliver` 里每包三次拿 `sessions` 锁

`kcp-rs/src/sharded.rs` 的 `deliver`，已建立会话这条路径依次做：

```rust
let existing = args.sessions.lock().get(&peer).map(|s| s.conn.clone()); // 1
// ...is_closed / is_dead 判断...
let queue = args.sessions.lock().get(&peer) ...                          // 2（内联后不再需要）
if let Some(session) = args.sessions.lock().remove(&peer)                 // 3，仅关闭时
```

这是一把 `parking_lot::Mutex<HashMap>`，rx task 和每条会话的 build task、sweep 共用。
单连接看不出，32 连接时每个包都要抢它，而抢锁失败的等待在 3.10 上同样被 timer 粒度
放大。改成一次加锁取出 `conn`（内联之后根本不需要 `queue`），关闭判断也放在同一次
加锁里。预期只影响 32 连接的吞吐，不影响单连接延迟，所以排在测量之后。

### 2. `feed_raw_single` 在 rx task 上做 AES 解密

解密现在发生在收包 task 里。单连接没问题；连接一多，一个慢会话的解密会挡住所有会话
的收包，等于把 main 里「N 个 worker 线程并行解密」收成了一条 task。这是 32 连接吞吐
差里扣除延迟因素后还可能剩下的部分。

不要为此加线程。`knet::cpu_block` 是现成的卸载池，但注意它**不能**跑阻塞 IO（见
`bugs/BUGREPORT_TCPRAW_ACCEPT_STARVES_CPU_POOL.md`）。AES 解密是纯 CPU，符合它的用途。
只在确认第 1 条之后 32 连接仍明显低于 main 时再做，而且只卸解密，KCP 和 ACK 发送留在
rx task 上——否则又把唤醒 1 加回来了。

### 3. rx task 顶部无条件的 `yield_now`

`spawn_rx` 每轮循环先 `knet::yield_now().await` 再 `evict_stale`。这是为了让会话 task
先把队列排空再判断过期。改成内联 `feed_raw_single` 之后，`stale_burst_count` 在
`deliver` 返回前就已经更新了，这次 yield 的理由消失，但它仍在每次收包路径上强制让出
一次。删掉它，把 `evict_stale` 留在批处理之后。这是一行改动，但同样先测再动，因为它
和 `evict_stale` 的正确性绑在一起。

### 4. 发送侧的 `SharedSendLock` 先不动

`PeerTransport::try_send_batch_to` 每次 `sendmmsg` 都拿这把 listener 级的锁
（`kcp-rs/src/transport.rs:451`）。单连接测到的差距排除了它：单连接没有竞争。它影响
的是 32 连接在发送缓冲区接近满时的尾部。main 没有这把锁是因为每个 worker 线程自己
串行发送。等延迟回到 1ms 内、32 连接吞吐仍低时再考虑按 peer 分片或去掉，不要现在动。

32 连接后来还是跑了，用户明确要求。口径同样是 `--shards 1 --conn 1`，32 条 TCP
连接各 8MB，超时 60 秒，三轮交替（`/tmp/kcpbench-cmp/ab-c32-142036.log`）。
跑完没有残留进程。

| 轮次 | main MB/s | 新 MB/s | main 延迟 | 新延迟 |
|---|---|---|---|---|
| 1 | 15.35 | 14.98 | 0.26 ms | 0.16 ms |
| 2 | 15.56 | 14.34 | 0.59 ms | 0.18 ms |
| 3 | 15.14 | 15.48 | 0.67 ms | 0.14 ms |

延迟中位数 0.16ms 对 0.59ms，新实现仍明显更低。吞吐中位数 14.98 对 15.35 MB/s，
差距在 4% 以内，落在这台机器负载从 0.39 爬到 2.18 的噪声里。也就是说在 32 连接、
单 shard 下，之前列出的三条后续优化（`sessions` 锁合并、解密卸载、去掉
`yield_now`）没有表现为可测量的回退。

绝对吞吐（约 15 MB/s）远低于 handoff 里同机器的 56–80 MB/s。当时的口径是
`--conn 8` 且服务端按 CPU 数分 shard，也就是 8 条 UDP 连接摊到 4 个 socket 上；
这次为了避免 `SO_REUSEPORT` 把一条会话拆散，固定了 `--shards 1 --conn 1`。
15 MB/s 对 20 MB/s 的单连接数字也说得通：单条 UDP 连接、单核处理 32 路 TCP。
所以这个数是口径差异，不是回退。

## 接下来

单连接的调度开销已经消掉，剩下的三条都只在连接数上来之后才看得出来，而 32
连接这台机器现在不让跑。所以下面这些先不动，等有一台可以压的机器再验证：

1. `deliver` 里已建立会话每个包拿三次 `sessions` 锁，收成一次。
2. 解密搬到 `knet::cpu_block`，只卸解密，KCP 和 ACK 发送留在 rx task。
3. 去掉 `spawn_rx` 开头无条件的 `yield_now`，先确认 `evict_stale` 仍正确。

`SharedSendLock`、ACK 阈值、`interval` 都不动，理由见上文。

另外编译出两个无害警告，提交前顺手清掉：`conn.rs` 的 `MAX_IDLE_UPDATE_MS`
不再被引用，`sharded.rs` 的 `Session.queue` 在已建立会话改走 `feed_raw_single`
之后没有读者（`building` 路径的队列是另一份，不读这个字段）。

## 明确不要做的

- **不要调 ACK 阈值或 `interval`。** 剩下的开销是调度，不是 ACK 攒得不够。`flush_acks_only`
  已经把「等 10ms」这条路绕开了。
- **不要把 `background_input(false)` 加回来让会话 task 消失。** 那就是退回 main 的
  线程模型，违反这次改动的目标。
- **不要加长 `listener_close_does_not_hang_an_inflight_write` 的超时来让它过。**
  内联之后 rx task 持 KCP 锁的时间变长，和 `write_all` 抢同一把锁，这是真实的行为变化，
  要看断言而不是掩盖。
- **不要重编覆盖 `/tmp/kcpbench-cmp/old/`。** 那是 main 的 `8e8f8760`，对照物丢了就没法比。
