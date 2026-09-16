# BUGREPORT：真实链路重传率远高于丢包率（伪 RTO 重传风暴）

> 分析日期：2026-09-15
> 复现环境：本机 macOS (loopback + 用户态链路模拟器)、测试服务器 `root@38.145.210.53`（1 vCPU / 512MB，宿主 load 20-30）

## 一、结论摘要

用户报"路径丢包 ~7%，服务端重传 30%+"。经可控复现与 Go 对照，结论是：

1. **不是 kcptun-rs 的实现缺陷。** 同一条真实链路上 Go kcptun 更差（重传 28.0% / 25.3s）而 Rust 为 13.4% / 13.5s；在干净的模拟链路（194ms RTT + 7% 均匀丢包）上同一份 Rust 代码重传仅 4.0%，且以快速重传为主（441 次 FastRetrans vs 96 次 RTO）。
2. **重传绝大多数是"伪重传"**：服务端以 RTO 超时重传的段，客户端其实已经收到（`客户端 KCP 输入段数 ≈ 服务端发送段数 × (1-线损) + FEC修复数`，缺口仅约 70/16940 段）。字面意义上 ~98% 的重传不必要。
3. **放大器是发送端进程被"冻结"**：测试服务器在传输期间，一个普通进程 45 秒内累计失去 CPU 24 秒（p90 间隔 84.9ms、p99 101.2ms、max 344.7ms；空载时 max 仅 80ms）。KCP 的定时器/flush 循环同样会被冻结，恢复后一次性把整个在途窗口判为超时 → RTO 重传风暴。盒子的内存状态印证这一点：`free` 仅 10-45MB，swap 已用 228/256MB，宿主 load 20-30。
4. **本地用 SIGSTOP 模拟"发送端每 1 秒冻结 150ms"，直接复现了线上特征**：重传 4.0% → 18.4%，其中 RTO 触发 2037 次、快速重传 832 次（与线上"LostSegs≈RetransSegs、FastRetrans≈0"一致）。

## 二、现象与证据

### 2.1 可控链路复现（本机，用户态 UDP 代理：双向 97ms 固定延迟 + 7% 独立丢包，FEC 10:3，`--mode fast`，16 MiB 下载）

| 场景 | 耗时 | 服务端重传 | FastRetrans | RTO(Lost) | 客户端 FEC 修复 |
|---|---|---|---|---|---|
| Rust 基线 | 3.93s | 548 (4.0%) | 441 | 96 | 844 (6.2%) |
| **Go 对照（同链路）** | 4.17s | 725 (4.3%) | 596 | 60 | 1009 (6.0%) |
| Rust + 发送端每 1s STOP 150ms | 4.85s | **2934 (18.4%)** | 832 | **2037** | — |
| 同上 + `--interval 100` | 4.69s | 2764 (17.5%) | 783 | 1902 | — |
| 同上 + `--sndwnd/--rcvwnd 256` | 13.11s | 1232 (8.7%) | 91 | 1123 | — |

模拟链路无停滞时 Rust/Go 差异 ≤0.3 个百分点、Rust 略快；**"发送端被冻结"一项就能把 RTO 重传从 96 推到 2037**。

### 2.2 真实链路对照（服务端在 38.145.210.53，客户端在本机，同配置同参数）

| 配置 | 耗时 | 服务端 OutSegs | 重传 | FastRetrans | RTO(Lost) | 客户端 FECRecovered |
|---|---|---|---|---|---|---|
| Rust `--mode fast` | 13.5s | 15054 | 2018 (13.4%) | 51 | 1967 | 734 (~4.9% 线损) |
| **Go `--mode fast`** | 25.3s | 22624 | **6344 (28.0%)** | 1695 | 4512 | 272 (~1.4%) |
| Rust `--nc 0` | 18.3s | 16962 | 3940 (23.2%) | 164 | 3775 | — |
| Rust `--sndwnd/--rcvwnd 256`（21 轮次 A/B 各 2 轮） | 14.4s / 18.0s | 13996 / 14634 | **8.2% / 11.7%** | — | — | — |
| Rust 窗口 1024（同 A/B 基线） | 11.7s / 13.0s | 16940 / 17180 | 23.2% / 24.1% | — | — | — |

同一台机器、同一链路、同一参数：Go 重传率是 Rust 的 2 倍、耗时是 1.9 倍。

### 2.3 "伪重传"的定量证据（`--mode fast` 基线轮）

服务端：`OutSegs=16940`（含 3923 次重传）→ 原始段 13017。
客户端：收报 21182 个 UDP 报文；FEC 校验分片 4900；`FECRecovered=549`；`InSegs=16870`（送入 KCP 的段数）。

由 `InSegs ≈ OutSegs × (1 - 线损) + FECRecovered` 反解：客户端 KCP 实际只缺 **约 70 段**（0.4%），而服务端按 RTO 重传了 3923 段。
即：**丢失的段几乎全部被 FEC 修好了，KCP 根本不需要重传；重传是由"ACK 没能及时被处理/判定"造成的。**

注：本轮客户端 `RepeatSegs=0` 是计数器问题（见 §5.1），不代表重传段没到达。

### 2.4 指令调度停滞（测试服务器实测）

`stall_probe.py`（10ms 定时器，记录实际间隔）：

| 状态 | p50 | p90 | p99 | max | >50ms 次数 | 累计停滞 |
|---|---|---|---|---|---|---|
| 空载 20s | 10.1ms | 10.1ms | 10.5ms | 80ms | 2 | 140ms |
| **传输中 45s** | 10.1ms | **84.9ms** | **101.2ms** | **344.7ms** | **249** | **24.0s** |

同期的盒子状态：`free` 10-45MB、swap 已用 228/256MB、`/proc/loadavg` 显示宿主 20-30（容器 1 vCPU，`%Cpu(s)` 中 50% 为系统态）。
KCP 的 RTO（`--mode fast`：`rx_minrto=100ms`，`RTO = srtt + max(interval, 4×rttvar)` ≈ 250-300ms）小于这些冻结时长，冻结恢复后 flush 会把整个在途窗口判超时。

### 2.5 用户生产实例的实时数据

```
RetransSegs=8441 / OutSegs=16378 = 51.5%
FastRetransSegs=311, LostSegs=8106, FECRecovered=149
```
**线损 <1%（FEC 仅修复 149 个分片），却重传 51.5%** —— 与上述机制完全一致。

## 三、根因

发送端的 KCP 定时器/flush 循环在传输期间被反复冻结（内存换页、单核争用、宿主超卖），恢复后 `resendts` 已大面积过期，flush 一次性重传整个在途窗口；而被"丢失"的段其实早已被客户端 FEC 修复并送达。`--nc 1`（kcptun 所有 `--mode` 预设都是 nc=1，即关闭拥塞控制）使发送端不会因此降速，于是形成稳态：吞吐被无谓重传挤占 → 队列更满 → 冻结更频繁。

协议层无法区分"排队延迟"与"丢包"，因此在这样的宿主机上，KCP 的 RTO 语义必然放大问题——这也是 Go 表现相同（更差）的原因。

## 四、可执行的缓解措施（实测）

按"实测有效"排序：

| 措施 | 实测效果 | 代价 |
|---|---|---|
| **缩小窗口**（服务端 `--sndwnd 256` + 客户端 `--rcvwnd 256`） | 重传 23-24% → **8.2-11.7%** | 吞吐 -20~40%（11.7s → 14.4s） |
| **释放宿主机内存压力**（当前 swap 228/256MB，另有 155MB 的测试实例常驻；`--smuxbuf/--sockbuf` 从 10MB 下调） | 直接消除冻结来源 | 无 |
| **换更空闲的机器/宿主**（宿主 load 20-30） | 根治 | 成本 |
| `--interval 100` | 无显著改善（18.4% → 17.5%） | — |
| `--nc 0` | **更差**（18.3s / 23.2%）：Reno 无法区分排队与丢包，只会持续退避 | — |
| 换回 Go kcptun | **更差**（28.0% / 25.3s） | — |

不建议改动协议/RTO 常量去"消除"这些重传：客户端已经收到数据，多发的重传只是浪费带宽；真正的损失来自宿主机冻结。

## 五、附带发现（与本问题相关，未改动代码）

### 5.1 `RepeatSegs` 少计一类重复段（诊断盲区）
`kcp.rs` 的 `Command::Push` 分支只在 `itimediff(sn, rcv_nxt) >= 0` 时调用 `parse_data`，因此 `sn < rcv_nxt` 的旧重复段（正常重传到达的情形）**完全不计入 `RepeatSegs`**。
Go 的实现是 `repeat := true`，仅当进入 `parse_data` 分支时才可能被改写，因此旧重复段会被计入：

```go
repeat := true
if _itimediff(sn, kcp.rcv_nxt+kcp.rcv_wnd) < 0 {
    kcp.ack_push(sn, ts)
    if _itimediff(sn, kcp.rcv_nxt) >= 0 { repeat = kcp.parse_data(...) }
}
if pktType == IKCP_PACKET_REGULAR && repeat { atomic.AddUint64(&DefaultSnmp.RepeatSegs, 1) }
```

影响：`RepeatSegs` 是判断"重传是否到达对端"的关键计数器，少计会让排查结论恰好反向（本次排查中即是如此）。**建议修正为与 Go 一致。**

### 5.2 cwnd 初值与注释不符
`kcp.rs`：`cwnd: KCP_DEFAULT_WND, // Go: 0, but we follow C KCP (ikcp_create sets cwnd = IKCP_WND_SND)`。
实际核对：C KCP 的 `ikcp_create` 也是 `kcp->cwnd = 0`（`kcp/ikcp.c:250`），Go 亦为 0；本仓库 `KCP_DEFAULT_WND = 32`。
影响有限（仅 `--nc 0` 的启动行为：初值 32 段 vs 从 1 段慢启动），但注释依据是错的。

### 5.3 ACK 列表截断与 Go 一致（非缺陷）
`flush_with_current` 只发送 `sn >= rcv_nxt` 的 ACK 项（外加最后一项），其余依赖 ACK 包的 `una` 覆盖——与 kcp-go 的 "filter jitter" 行为一致，属正常。

### 5.4 实验踩坑：Go 的 `--snmplog` 文件名会被当作时间格式
`std/snmp.go`：`os.OpenFile(logdir+time.Now().Format(logfile), ...)`。文件名中的单个数字是 Go 时间布局占位符（`4`=分钟、`5`=秒…），因此 `--snmplog /tmp/r4_go.csv` 会写成 `/tmp/r31_go.csv` 之类。做 Go/Rust 对照实验时务必用纯字母文件名。

## 六、复现材料

本次排查使用的工具（临时目录 `/tmp/impair/`，未入库）：

| 文件 | 用途 |
|---|---|
| `impair_proxy.py` | 用户态 UDP 链路模拟：双向固定延迟、独立丢包 / 突发丢包（`BURST_LEN`） |
| `run.sh` | 本地实验驱动（延迟/丢包/CPU 争用 `LOAD=`/发送端冻结 `STALL_MS=`） |
| `vps_run.sh` / `tap_run.sh` | 真实链路实验（VPS 服务端 + 本机或 VPS 客户端；后者带 KCP 层透明抓包） |
| `stall_probe.py` / `cpu_probe.py` | 宿主机调度停滞与 CPU/内存采样 |
| `analyze_tap.py` | 抓包分析：每段发送→ACK→重传时序、伪重传判定 |

关键判据：**服务端 `RetransSegs ≈ LostSegs` 且 `FastRetransSegs` 极小 + 客户端 `FECRecovered` 远小于重传数 = 伪 RTO 重传，先查宿主机调度，而非链路丢包。**
