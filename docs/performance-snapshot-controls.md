# 控制实验：快照写缓冲与端到端回退信号

[返回 README](../README.md) · [扩大数据集的 pilot](performance-snapshot-pilot.md) · [实验程序](benchmark-experiments.md) · [指标口径](observability.md)

`bfeddf1` 为快照文件增加 64 KiB 有界写出缓冲。本次固定 100k key、1 KiB value 和同一组 Gateway / Benchmark 文件，以 A=`d2d5eb8`、B=`bfeddf1` 交错运行 16 轮。**四个 ON 配对的写出阶段都更短，B/A 中位数为 0.711779；但三个 ON 配对的 QPS、P99、P99.9 和最大延迟都更差，这是未解决的回退信号。**

较短的局部阶段不代表服务整体通过性能验收。`03-B` ON 的最大延迟达到 **266.577726 ms**，所有慢轮次完整保留。共享主机是测量限制，不能据此解释掉负向证据；这里不宣称服务无回退，也不把原因预先归给环境。缓冲仍是需要进一步定位和验证的局部候选。

## 预声明设计与执行文件

运行前冻结顺序 **A B B A B A A B**，每阶段完整执行 throughput OFF→5,000 ms ON 各一次，共 16 轮。相邻配对为 `(1,2)`、`(3,4)`、`(5,6)`、`(7,8)`，在相同快照条件下始终计算 B/A，不因 BA 顺序反算。每个条件各版本有四轮；两对 AB、两对 BA 不是随机化，也不能消除墙钟时间漂移或固定 OFF→ON 顺序的影响。

每轮使用新进程、新数据目录，先顺序预置 100,000 个 key，再测量 1,500,000 请求。40 个闭环客户端 worker，mixed PUT 20% / DELETE 0% / 其余 GET，value 1,024 字节、seed 1、请求超时 2 s。Engine 数据线程 20、队列容量 128、连接上限 256、RPC 池 64，WAL batch 64、flush 2 ms、未同步字节限额 16 MiB。两个 Go 进程均 GOMAXPROCS=4、GOGC=100、GOMEMLIMIT=off。

资源每 100 ms、状态每 250 ms 采样；压测进程预算 900 s（包含预置与报告生成），启动 10 s、关闭 60 s、WAL 排空 60 s。没有额外稳态预热或清空系统缓存。主机为 Xeon E3-1270 v3、8 个逻辑 CPU、Linux 6.8，数据位于 NTFS3 的 `/dev/nvme0n1p2`，构建背景为 RelWithDebInfo。八阶段开始时的 1 分钟 load 为 9.22、13.67、15.45、13.64、15.31、16.66、10.68、15.02，原始主机背景均保留。测量期间没有并行的项目构建、测试、分析、归档或其他矩阵；主机其他负载、CPU 频率、缓存及阶段间隔未隔离。

预声明主要观察是 **ON 每轮共同 idle 窗口内的 write 阶段累计增量 / 完成快照数**，再计算相邻 B/A；客户端 QPS、P99、P99.9、max 与其余阶段、资源指标全部报告。覆盖门槛为测量 ≥30 s、ON 内部完成 ≥3 次快照，阶段均值还要求共同 idle 窗口内完成 ≥3 次且无失败。覆盖不足时保留原始客户端结果并标记对应阶段指标不可用，继续已声明顺序；实验失败或中断则停止，不补跑替代轮次。本次全部达到门槛，测量为 47.173–56.002 s。

| 文件 | 运行来源 | SHA-256 |
| --- | --- | --- |
| A Engine | `d2d5eb862ee1cf11fcbeb68c04c66d740613338e` | `efabc96b11ae376fa020b052215f57af286729d7c5b76722b9ddaba6d9f022a0` |
| B Engine | `bfeddf139aab09442207e9938617bff63725ccd1` | `7ebe0839e69478d7ed78ec997aa178b61e07294fafb168371cc6555024940dc0` |
| 固定 Gateway | `7d29382` | `6fed313c6f924c21871cc05cf5589a5a6189cde1663492d682ec5b5ea3981882` |
| 固定 Benchmark | `7d29382` | `2ef55eb18244b0e0e8b4b9d94318300c6090b59dda7423df529ac370f6bca4fc` |

全部 24 份 stage 执行文件副本在结束后重新计算哈希，八个 stage 原位置验证均为 `copies_verified`。Go 文件实际字节固定；所有报告的客户端版本均为 `7d29382`。采集工作区为 clean `897bc0a`，这是 driver 的来源，不能替代 A/B 运行版本。plan SHA-256 为 `a74465c47cedc4d90ac27355df76cf91c10b73654d4204d12ac8eec8e061bace`。

## 全部客户端结果与配对

以下每行保留原始单轮值，延迟来自该轮的全部 1,500,000 个样本。

| 阶段 | 自动快照 | 成功 QPS | P99，ms | P99.9，ms | max，ms |
| --- | --- | --- | --- | --- | --- |
| 01-A | off | 30,519.190 | 2.752785 | 4.359069 | 15.466002 |
| 01-A | on | 30,574.967 | 2.606660 | 3.717849 | 65.891789 |
| 02-B | off | 31,341.189 | 2.536735 | 3.534293 | 12.567055 |
| 02-B | on | 29,797.405 | 2.947107 | 8.029996 | 68.329597 |
| 03-B | off | 30,861.355 | 2.679024 | 7.644356 | 12.599388 |
| 03-B | on | 26,784.747 | 6.946979 | 11.500556 | 266.577726 |
| 04-A | off | 31,797.633 | 2.453795 | 3.361873 | 17.358655 |
| 04-A | on | 29,294.103 | 3.158957 | 8.269779 | 67.984279 |
| 05-B | off | 31,460.973 | 2.498215 | 3.856198 | 10.320568 |
| 05-B | on | 27,987.326 | 4.632245 | 8.988728 | 71.991000 |
| 06-A | off | 30,463.258 | 2.799095 | 7.783220 | 11.458393 |
| 06-A | on | 26,913.450 | 7.481980 | 10.836110 | 88.470872 |
| 07-A | off | 31,609.426 | 2.485128 | 3.389100 | 18.937280 |
| 07-A | on | 30,247.360 | 2.702620 | 7.517607 | 63.444066 |
| 08-B | off | 31,212.218 | 2.588813 | 6.941505 | 14.127931 |
| 08-B | on | 28,586.437 | 3.955661 | 9.667578 | 85.131494 |

各组四轮的**中位数 [最小值，最大值]**如下。它们是轮次值的分布，不是合并请求后的总体 P99 或 P99.9。

| 自动快照 | Engine | 成功 QPS | P99，ms | P99.9，ms |
| --- | --- | --- | --- | --- |
| off | A | 31,064 [30,463，31,798] | 2.619 [2.454，2.799] | 3.874 [3.362，7.783] |
| off | B | 31,277 [30,861，31,461] | 2.563 [2.498，2.679] | 5.399 [3.534，7.644] |
| on | A | 29,771 [26,913，30,575] | 2.931 [2.607，7.482] | 7.894 [3.718，10.836] |
| on | B | 28,287 [26,785，29,797] | 4.294 [2.947，6.947] | 9.328 [8.030，11.501] |

先逐对计算 B/A，再汇总四个比值；不把两个版本的组中位数相除当作配对结果。QPS 比值大于 1 表示 B 较高，延迟比值小于 1 表示 B 较低。

| 自动快照 | 分子 / 分母 | QPS B/A | P99 B/A | P99.9 B/A | max B/A |
| --- | --- | --- | --- | --- | --- |
| off | 02-B / 01-A | 1.026934 | 0.921516 | 0.810791 | 0.812560 |
| off | 03-B / 04-A | 0.970555 | 1.091788 | 2.273838 | 0.725827 |
| off | 05-B / 06-A | 1.032751 | 0.892508 | 0.495450 | 0.900699 |
| off | 08-B / 07-A | 0.987434 | 1.041722 | 2.048185 | 0.746038 |
| on | 02-B / 01-A | 0.974569 | 1.130607 | 2.159850 | 1.036997 |
| on | 03-B / 04-A | 0.914339 | 2.199137 | 1.390673 | 3.921167 |
| on | 05-B / 06-A | 1.039901 | 0.619120 | 0.829516 | 0.813725 |
| on | 08-B / 07-A | 0.945089 | 1.463639 | 1.285991 | 1.341835 |

| 四对比值中位数 | QPS B/A | P99 B/A | P99.9 B/A | max B/A |
| --- | --- | --- | --- | --- |
| off | 1.007184 | 0.981619 | 1.429488 | 0.779299 |
| on | 0.959829 | 1.297123 | 1.338332 | 1.189416 |

ON 的第 1、2、4 对端到端四项指标都较差，第 3 对较好。`03-B` ON 为 26,784.747 QPS / P99 6.946979 ms / max 266.577726 ms；同对 `04-A` ON 为 29,294.103 QPS / P99 3.158957 ms / max 67.984279 ms。旧版 `06-A` ON 也有 P99 7.481980 ms，不能只挑旧版最快轮次或新版最快轮次下结论。

OFF 的 QPS、P99 与 P99.9 方向也不一致。保留 OFF 是为了观察相同公共请求路径下的变化，不能把 ON 差异减去 OFF 差异就自动归因为快照。四个配对是描述性证据，不构成统计显著性、等价性或容量保证。

P99.9 与 max 从原始 report 提取，不能从只含 P99 的缓存 metrics 推断。闭环 worker 等待响应时不会继续按固定到达率发送请求，暂停期没有形成相同的外部积压；这些分位数只描述已发出的请求。max 是一次测量中的极值，不能充当尾延迟上界。

## 快照阶段与 08-B 的窗口边界

只选取查询开始、结束都在客户端测量区间内的原始状态样本。对每个 ON 轮次，确定性地取**最早和最晚的 `snapshot_in_progress=false` 内部样本**作为三阶段共同窗口，不根据耗时选择端点；检查累计计数没有倒退、快照失败为 0、完成数 ≥3。计数与状态由引擎同一次状态视图读取，客户端和查询仍通过墙钟近似对齐。

以下均为 2026-09-14 UTC 的查询结束时间：

| ON 阶段 | 首查询结束，UTC | 末查询结束，UTC | 窗口长度，s | 内部 / 子窗口样本数 | 完成数 |
| --- | --- | --- | --- | --- | --- |
| 01-A | 12:22:36.814279 | 12:23:25.726624 | 48.912345 | 189 / 189 | 9 |
| 02-B | 12:25:11.177582 | 12:26:01.200283 | 50.022701 | 193 / 193 | 9 |
| 03-B | 12:27:44.435571 | 12:28:40.208297 | 55.772726 | 215 / 215 | 10 |
| 04-A | 12:30:41.625019 | 12:31:32.469769 | 50.844750 | 197 / 197 | 9 |
| 05-B | 12:33:30.466112 | 12:34:23.902331 | 53.436219 | 207 / 207 | 9 |
| 06-A | 12:36:07.631340 | 12:37:03.082024 | 55.450684 | 215 / 215 | 10 |
| 07-A | 12:40:06.680240 | 12:40:56.046253 | 49.366013 | 191 / 191 | 9 |
| 08-B | 12:42:39.036603 | 12:43:30.658046 | 51.621443 | 202 / 200 | 9 |

共同窗口中的三阶段总增量完整保留：

| ON 阶段 | capture 总增量，ns | write 总增量，ns | compact 总增量，ns |
| --- | --- | --- | --- |
| 01-A | 547,367,190 | 5,724,727,905 | 200,437,771 |
| 02-B | 555,866,288 | 4,051,552,335 | 173,358,288 |
| 03-B | 685,040,230 | 4,588,519,409 | 192,948,618 |
| 04-A | 590,079,771 | 6,426,953,404 | 196,187,306 |
| 05-B | 580,728,937 | 4,225,861,892 | 173,437,475 |
| 06-A | 686,032,495 | 6,559,379,343 | 215,293,861 |
| 07-A | 545,719,426 | 5,708,925,585 | 194,057,007 |
| 08-B | 614,267,156 | 4,168,018,941 | 175,273,374 |

各阶段均值按 `Δ snapshot_*_duration_ns_total / Δ snapshot_successes_total / 1e6` 得到，三项共享同一完成快照集合：

| ON 阶段 | capture / 次，ms | write / 次，ms | compact / 次，ms |
| --- | --- | --- | --- |
| 01-A | 60.818577 | 636.080878 | 22.270863 |
| 02-B | 61.762921 | 450.172482 | 19.262032 |
| 03-B | 68.504023 | 458.851941 | 19.294862 |
| 04-A | 65.564419 | 714.105934 | 21.798590 |
| 05-B | 64.525437 | 469.540210 | 19.270831 |
| 06-A | 68.603250 | 655.937934 | 21.529386 |
| 07-A | 60.635492 | 634.325065 | 21.561890 |
| 08-B | 68.251906 | 463.113216 | 19.474819 |

| ON 分子 / 分母 | capture B/A | write B/A | compact B/A |
| --- | --- | --- | --- |
| 02-B / 01-A | 1.015527 | 0.707728 | 0.864898 |
| 03-B / 04-A | 1.044835 | 0.642554 | 0.885143 |
| 05-B / 06-A | 0.940559 | 0.715830 | 0.895094 |
| 08-B / 07-A | 1.125610 | 0.730088 | 0.903206 |
| 四对中位数 | 1.030181 | 0.711779 | 0.890118 |
| 四对范围 | 0.940559–1.125610 | 0.642554–0.730088 | 0.864898–0.903206 |

四对 write 比值都低于 1；capture 三对较高、一对较低，不能将缓冲描述为已经减少状态捕获暂停。write 阶段包括编码、CRC、写入、文件同步、rename 和目录同步，不是纯系统调用时间；capture 包括等待 I/O 锁、复制状态、同步捕获 WAL 和确定边界；compact 包括等锁、复制 WAL 后缀和持久化安装。这些阶段均值不是锁持有时间或 P99，capture 还与 WAL 提交耗时重叠，不能相加为客户端延迟或总暂停。

**`08-B` ON 必须剔除末两条内部样本后再计算阶段均值。** 完整内部流有 202 条样本，最后查询结束于 12:43:31.238052，此时 `snapshot_in_progress=true`。按预声明规则，共同子窗口止于 12:43:30.658046，保留前 200 条、窗口 51.621443 s、9 次完成。其 write 总增量为 **4,168,018,941 ns**，均值为 **463.1132156667 ms**。这只改变阶段观测的窗口，不删除该轮请求、慢请求或原始采样。

同一轮 Engine 的完整测量资源窗口为 **52.435292682 s**，从 12:42:38.982 到 12:43:31.417 UTC，已涵盖子窗口结束后未完成快照的部分活动。因此该窗口的 CPU / I/O 不能除以 9 就称作“九次相同工作量的平均成本”。其 write_bytes 增量为 **1,441,239,040 B**，保留完整窗口口径。

其他 ON 轮次首末内部样本已空闲，不需裁剪。各轮有 9 或 10 次内部完整快照；自动线程在一次执行结束后再等 5,000 ms，阶段变短会影响快照频率、WAL 后缀长度和整轮工作量。即使 key 数与 value 大小相同，整轮 CPU/I/O 也不是严格相同快照工作量的比较。OFF 的完成增量为 0，阶段均值为 null；启动及预置期快照不计入这些内部完成数。

## 正确性、资源与积压

16 轮均有效，没有失败、未命中、缺失、中断、invalid、未完成或覆盖不足。每轮 PUT **300,879**、GET **1,199,121**、DELETE **0**，共 1,500,000 次 HTTP 200；预置完成 100,000 个 key。测量内部 keys 始终为 100,000，after / settled 均为 applied=durable=**400,879**、WAL pending=0。48 个子进程全部正常退出，没有强制终止。

throughput 在内存更新和 WAL 入队后即可确认。QPS 不等于已持久化吞吐，预置、百分位计算、报告生成和排空不计入测量。after 查询在客户端退出后才发生，不能用其已排空反推最后一个成功响应返回时已经落盘。

以下每格为逐轮计算后的四轮中位数；pending 一行保留四轮样本最大值的范围。

| 指标 | A off | B off | A on | B on |
| --- | --- | --- | --- | --- |
| engine CPU，核 | 1.238 | 1.243 | 1.320 | 1.227 |
| gateway CPU，核 | 2.737 | 2.756 | 2.623 | 2.491 |
| bench CPU，核 | 1.920 | 1.929 | 1.839 | 1.749 |
| 平均排队，ms | 0.057739 | 0.057580 | 0.065286 | 0.068670 |
| 队列深度样本均值 | 1.266 | 1.455 | 0.924 | 1.074 |
| 活动数据线程样本均值 | 1.467 | 1.521 | 1.349 | 1.419 |
| 逐轮 pending 样本最大值范围，B | 62241–68568 | 64348–71732 | 70678–190932 | 56962–137132 |

CPU 使用各进程自身 monotonic 采样区间，固定 PID / starttime，按用户与系统 tick 增量除以 100 tick/s 及区间秒数。1 核代表一个逻辑 CPU 的平均计算时间，不表示其他 CPU 空闲。新版本 ON 三个进程的 CPU 核数中位数都较低，可能是闭环请求完成变慢的结果，不能据此排除代码因素或推断具体的等待原因。

排队均值为 `Δ request_queue_wait_duration_ns_total / Δ requests_started_total`；gauge 均值是未加权样本算术均值，样本最大值不是真实峰值。全部内部窗口的 WAL 容量等待和可靠确认等待完成数、等待耗时增量、等待者样本值均为 0，等待均值保留 null。没有请求拒绝、RPC 错误或重试。它们排除了这些观测到的失败路径，但不能证明尾延迟回退不存在。

内存每格为“测量 RSS 样本最大值 / 已观察 HWM”的四轮中位数，单位 MiB：

| 进程 | A off | B off | A on | B on |
| --- | --- | --- | --- | --- |
| engine | 114.992 / 114.992 | 115.031 / 115.031 | 225.129 / 225.129 | 225.186 / 225.186 |
| gateway | 14.369 / 14.369 | 14.410 / 14.410 | 14.393 / 14.404 | 14.371 / 14.371 |
| bench | 34.883 / 34.895 | 34.961 / 34.973 | 35.246 / 35.250 | 35.355 / 35.361 |

ON 的 Engine 仍约 225 MiB；64 KiB 写出缓冲没有取消完整状态副本。RSS 离散采样可能漏峰，HWM 自进程启动累计，可能包含预置或报告阶段。Benchmark 保留精确延迟样本，内存随测量请求数增长。`08-B` OFF 的 Benchmark RSS 样本最大值为 **40,353,792 B**、HWM **46,977,024 B**，高于其他轮次，原始观测完整保留；不能只展示接近的组中位数就声称内存无变化。

Engine 进程存储 I/O 增量的四轮**中位数 [最小值，最大值]**如下，每轮都使用自己的完整测量资源窗口：

| Engine 进程 I/O | A off | B off | A on | B on |
| --- | --- | --- | --- | --- |
| Δ read_bytes，B | 8,192 [8,192，12,288] | 6,144 [4,096，12,288] | 12,288 [4,096，16,384] | 335,872 [8,192，1,007,616] |
| Δ write_bytes，B | 360,118,272 [358,707,200，361,148,416] | 359,651,328 [358,944,768，360,448,000] | 1,342,996,480 [1,342,746,624，1,454,665,728] | 1,388,062,720 [1,334,136,832，1,444,683,776] |
| Δ cancelled_write_bytes，B | 0 [0，0] | 0 [0，0] | 0 [0，0] | 0 [0，0] |

read_bytes / write_bytes 是 `/proc/<pid>/io` 的进程存储计数，包含 WAL、快照和后缀回收等工作，不是 syscall payload 字节或纯快照写放大。不得把 ON−OFF 的差额称为设备写放大，也不擅自减去 cancelled_write_bytes。Gateway 与 Benchmark 的 write / cancelled 增量均为 0；少量 read 计数仍保留在原始流中。不同角色的资源窗口与状态、idle 子窗口不同，不能互换区间或把时间累计相加。

## 当前结论与下一步验收

这组证据支持“快照 write 阶段在四个配对中变短”，同时留下 **ON 三个配对端到端更差**的未解决回退信号。不能把前者写成服务整体提速，也不能把后者无证据地归为噪声。没有硬故障并不等于性能验收通过，缓冲目前仍是局部候选。

下一步应在新旧版本上使用同等口径，进一步区分捕获锁内时间、实际写调用和写入工作量，继续保留相同负载与原始端到端指标。如果后续对照仍复现负向趋势，应回退候选或明确接受的代价；不能仅凭一个阶段指标改善就宣布完成。这里的运行文件只覆盖 d2 / bfed，不包含后续增加的观测或优化实现。

## 完整归档与搬移复查

[snapshot-d2-bfed.tar.gz](../benmark/baselines/2026-09-14-snapshot-control/snapshot-d2-bfed.tar.gz) 保留完整实验父目录：冻结 plan、8 份 host-before、8 个 stage 的 host-context / manifest / index，以及全部 16 轮 report / commands / result / 状态 / 资源 / 日志。只排除实际 `binaries/` 副本和生成的 `data/`。派生 summary / details / audit 在原父目录之外，不作为原始归档成员；没有删减慢轮次。

归档为 **209 文件、20,722,356 原始字节、1,565,422 压缩字节**，SHA-256 为 `fe127a8179023a80b56835a034c721c0002c60707d6dca7276a9e7bbd4ccd89b`。完整字段见 [MANIFEST.json](../benmark/baselines/2026-09-14-snapshot-control/MANIFEST.json)。所有成员均为路径安全、无重复的普通文件，已逐成员核对字节和排除 binaries/data 后的完整源文件集合。

```sh
(cd benmark/baselines/2026-09-14-snapshot-control && sha256sum -c SHA256SUMS)
snapshot_control_extract_dir="$(mktemp -d)"
tar -xzf benmark/baselines/2026-09-14-snapshot-control/snapshot-d2-bfed.tar.gz \
  -C "$snapshot_control_extract_dir"
python3 benmark/summarize.py \
  "$snapshot_control_extract_dir/snapshot-d2-bfed"/[0-9][0-9]-[AB] --format json
```

实际搬移验证退出码为 0，16 valid，其余失败 / 缺失 / 中断 / invalid / 未完成 / 快照证据不足计数均为 0，所有 per-run groups 与原位置汇总相同。省略执行文件后八个 stage 均显示 `recorded_hashes_only` 提示，记录的哈希仍可追溯，但归档不能重验省略的实际文件；原绝对路径无需在新位置存在。CLI 分别统计八个 stage，不自动按 A/B 合并、配对，也不负责本次 ≥3 完成数与共同 idle 窗口门槛；这些按 plan 与原始流另外核对。

## 复现版本、负载与停止规则

先构建、冻结所有执行文件，再开始测量。Gateway / Benchmark 始终来自同一批 7d 文件；只在两个版本中构建 Engine。以下使用 7d 工作树的实验程序，其实验与汇总源码与本次 driver 897 相同。

```sh
snapshot_control_repo="$PWD"
snapshot_control_output_root="/tmp/minikv-snapshot-controls"
snapshot_control_frozen_dir="/tmp/minikv-snapshot-controls-binaries"
git worktree add --detach /tmp/minikv-snapshot-control-go7d 7d29382
git worktree add --detach /tmp/minikv-snapshot-control-d2 d2d5eb8
git worktree add --detach /tmp/minikv-snapshot-control-bfed bfeddf1
make -C /tmp/minikv-snapshot-control-go7d go
make -C /tmp/minikv-snapshot-control-d2 engine JOBS=4
make -C /tmp/minikv-snapshot-control-bfed engine JOBS=4
mkdir "$snapshot_control_output_root" "$snapshot_control_frozen_dir"
cp /tmp/minikv-snapshot-control-d2/build/engine "$snapshot_control_frozen_dir/engine-A"
cp /tmp/minikv-snapshot-control-bfed/build/engine "$snapshot_control_frozen_dir/engine-B"
cp /tmp/minikv-snapshot-control-go7d/bin/minikv-go "$snapshot_control_frozen_dir/gateway"
cp /tmp/minikv-snapshot-control-go7d/bin/minikv-bench "$snapshot_control_frozen_dir/bench"
chmod 0555 "$snapshot_control_frozen_dir"/*
sha256sum "$snapshot_control_frozen_dir"/* > "$snapshot_control_output_root/binaries.sha256"
printf '%s\n' 01-A 02-B 03-B 04-A 05-B 06-A 07-A 08-B \
  > "$snapshot_control_output_root/stage-order.txt"
printf '%s\n' 'Measurement >=30s; ON internal and shared-idle completions >=3.' \
  'Use earliest/latest internal idle samples; primary observation: ON write mean.' \
  'Keep insufficient evidence, stop on failure/interruption, never replace runs.' \
  > "$snapshot_control_output_root/coverage-plan.txt"

for snapshot_control_stage in 01-A 02-B 03-B 04-A 05-B 06-A 07-A 08-B; do
  snapshot_control_version="${snapshot_control_stage#*-}"
  if python3 /tmp/minikv-snapshot-control-go7d/benmark/experiment.py \
    --output "$snapshot_control_output_root/$snapshot_control_stage" \
    --engine "$snapshot_control_frozen_dir/engine-$snapshot_control_version" \
    --gateway "$snapshot_control_frozen_dir/gateway" \
    --bench "$snapshot_control_frozen_dir/bench" \
    --modes throughput --repeats 1 --requests 1500000 --workers 40 --keyspace 100000 \
    --op mixed --write-ratio 20 --delete-ratio 0 --value-size 1024 --seed 1 \
    --engine-workers 20 --rpc-pool 64 --gomaxprocs 4 \
    --wal-batch 64 --wal-flush-ms 2 --snapshot-ms 5000 \
    --sample-ms 100 --stats-ms 250 --run-timeout 900 \
    --startup-timeout 10 --shutdown-timeout 60 --settle-timeout 60; then
    :
  else
    printf 'Stage %s failed or was interrupted; keep artifacts and stop.\n' \
      "$snapshot_control_stage" >&2
    break
  fi
done

set --
for snapshot_control_stage in 01-A 02-B 03-B 04-A 05-B 06-A 07-A 08-B; do
  if [ -d "$snapshot_control_output_root/$snapshot_control_stage" ]; then
    set -- "$@" "$snapshot_control_output_root/$snapshot_control_stage"
  else
    printf 'Planned stage %s was not executed.\n' "$snapshot_control_stage" >&2
  fi
done
if [ "$#" -gt 0 ]; then
  python3 "$snapshot_control_repo/benmark/summarize.py" "$@" --format json
else
  printf 'No stage artifacts exist; the control plan is incomplete.\n' >&2
fi
```

工作树、冻结目录和输出父目录必须尚不存在。输出目录所在文件系统就是数据盘，可以改到待测盘的新路径并记录环境。原归档的 JSON plan 与 host-before / host-context 是额外手工采集，实验程序不会自动生成。覆盖门槛需按上文另行验证；不足时保留并标记，不因观察到快照活动就当作满足三次完成。

失败与中断都可能返回 1，脚本一律停止后续阶段；已创建但不完整的阶段仍交给 CLI 显露问题，未创建阶段按预声明名单显示。部分目录汇总退出 0 也不代表完整计划成功，需要另外核对八阶段、16 轮状态和覆盖。不同构建环境不保证相同文件哈希，不同运行环境也不保证重现相同性能。
