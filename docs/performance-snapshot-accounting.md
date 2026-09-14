# 控制实验：快照写调用、捕获锁时间与保留的尾延迟风险

[返回 README](../README.md) · [前一轮缓冲对照](performance-snapshot-controls.md) · [实验程序](benchmark-experiments.md) · [指标口径](observability.md)

本次在两个版本上保留相同七项快照计量，只切换 64 KiB 写出分组：A=`1470c5f` 不分组，B=`3179b64` 分组。16 轮交错实验中，ON 每完成一次快照的 write 调用由 **100,001 次变为 1,613 次**，写出阶段 B/A 的四对中位数为 **0.681521**；实际写入与安装的文件字节相同。**端到端负向判定为 2/4，未触发运行前声明的“至少 3/4 则撤销分组”规则，但这不是性能验收通过。**

ON 的 QPS / P99 配对比值中位数为 **0.985949 / 1.015615**。`02-B` ON 最大延迟为 **283.851464 ms**；`07-A` ON 也有 P99 **7.435304 ms**、最大延迟 **108.544997 ms**，全部保留。捕获状态锁每次平均仍约 **57–68 ms**。减少写调用和局部阶段耗时，没有证明状态复制暂停被消除，也不能抵消端到端和极端尾延迟风险。

## 预声明、版本与固定条件

运行前冻结 **A B B A B A A B**。每个阶段一次 throughput OFF→5,000 ms ON，共 16 轮；相邻配对为 `(1,2)`、`(3,4)`、`(5,6)`、`(7,8)`，在相同快照条件下始终取 B/A。两对 AB、两对 BA 不是随机化，固定 OFF→ON 顺序也未消除缓存、频率、其他主机负载和墙钟漂移。

两个 Engine 都基于 `3179b64` 的七项计量，源代码只在 `install_snapshot` 是否分组写出上不同：A 相对 B 为 `engine.cpp` 5 行新增 / 23 行删除，保留 counted `write_all` 及其他实现。固定 Gateway 来自 `3179b64`，固定 Benchmark 来自 `7d29382`。前一批 d2 / bfed 使用旧 Gateway 且没有这些计数，不能把两批合并成同一个 A/B 分布。

| 文件 | 实际运行来源 | SHA-256 |
| --- | --- | --- |
| A Engine | 1470c5fdd5f93cf85e099dc1e69dd1c24573f8b2 | 3edddd501c6dc3b16901ed2b9ce2754b9c3fe955b199ed2dd0d2f74250946f83 |
| B Engine | 3179b64f9a1b37c9d50a294ca8141b73a75b557b | 9741b61c986aafdcdf9cf99d499c41e08c085f1f6390133d3db09185edbd5f5c |
| 固定 Gateway | 3179b64 | ab05a47cecffc90d7c5e307ca850869fcd23f3f473ba4a98818c6a8de62f6f85 |
| 固定 Benchmark | 7d29382 | 2ef55eb18244b0e0e8b4b9d94318300c6090b59dda7423df529ac370f6bca4fc |

全部 24 份 stage 执行文件副本结束后重新核对哈希，八阶段原位置均为 `copies_verified`；所有报告的客户端版本为 clean `7d29382`。采集 driver 为 clean `3179b64`，它不能替代 A/B 的实际运行来源。plan SHA-256 为 `4b2a9808aa2f94be56b6e09b11ce2ac4cca90a7251a0f1648fc7f8ccf6094d10`。

每轮新进程、新数据目录，先顺序预置 100,000 个 key，再测量 1,500,000 请求；40 个闭环客户端 worker、mixed PUT 20% / DELETE 0% / 其余 GET、1,024 字节 value、seed 1、请求超时 2 s。Engine 数据线程 20、队列容量 128、连接上限 256、RPC 池 64；WAL batch 64、flush 2 ms、未同步字节限额 16 MiB。两个 Go 进程均 GOMAXPROCS=4、GOGC=100、GOMEMLIMIT=off。

资源每 100 ms、状态每 250 ms 采样；压测进程预算 900 s（含预置和报告生成），启动 10 s、关闭 60 s、WAL 排空 60 s。没有额外稳态预热或清空系统缓存。主机为 Xeon E3-1270 v3、8 逻辑 CPU、Linux 6.8，数据盘为 NTFS3 的 `/dev/nvme0n1p2`，构建背景为 RelWithDebInfo。八阶段开始时的 1 分钟 load 为 8.60、16.06、8.00、15.33、15.49、16.48、12.86、18.74，原始主机记录完整保留。测量期间没有并行项目构建、测试、分析、归档或其他矩阵；共享主机的其余影响没有隔离，也没有据此解释掉慢轮次。

每轮测量要求 ≥30 s，ON 内部完成快照 ≥3；阶段及七项计量的共同 idle 窗口还须完成 ≥3 次且无失败。覆盖不足保留客户端结果、标记受影响指标不可用并继续预声明顺序；实验失败或中断则停止，不替换轮次。本次 16 轮全部满足，测量为 **47.129–57.985 s**。

## 原始客户端值与工程处置规则

规则在任何本轮测量前写入 plan：每个有效 ON 配对以原始整数判断 `B.elapsed_ns > A.elapsed_ns && B.latency_ns.p99 > A.latency_ns.p99`，相等不算负向，不使用四舍五入后的表格或事后幅度阈值。至少 3/4 负向才自动撤销主分支的 64 KiB 分组，保留计量、测试和全部实验；判定等整套序列终止后进行。

不足四个有效配对不能算通过；缺失、失败、覆盖不足不是有利票。新计数不可用不能过滤有效的客户端负向票。该规则是保守工程决策，不是统计显著性或因果检验；未触发也不能证明等价或无回退，P99.9、max 和正确性异常仍须单独审视。

| 阶段 | 自动快照 | 成功 QPS | P99，ms | P99.9，ms | max，ms |
| --- | --- | --- | --- | --- | --- |
| 01-A | off | 31,587.230 | 2.506955 | 3.470398 | 13.303829 |
| 01-A | on | 30,611.118 | 2.672815 | 4.076490 | 62.318580 |
| 02-B | off | 31,433.514 | 2.512418 | 3.485813 | 10.653707 |
| 02-B | on | 30,325.110 | 2.657099 | 5.548470 | 283.851464 |
| 03-B | off | 31,082.374 | 2.589341 | 3.710464 | 15.067674 |
| 03-B | on | 28,328.674 | 2.997667 | 4.461551 | 79.976920 |
| 04-A | off | 31,348.394 | 2.478884 | 3.263946 | 9.979601 |
| 04-A | on | 30,213.316 | 2.746866 | 7.764105 | 64.168008 |
| 05-B | off | 30,597.604 | 2.636100 | 3.766771 | 10.796517 |
| 05-B | on | 29,408.263 | 2.956313 | 8.222065 | 71.906000 |
| 06-A | off | 31,827.525 | 2.440285 | 3.241954 | 19.393992 |
| 06-A | on | 29,970.484 | 2.850530 | 7.966432 | 64.635662 |
| 07-A | off | 30,172.094 | 2.739554 | 4.735142 | 19.856306 |
| 07-A | on | 25,868.570 | 7.435304 | 13.831864 | 108.544997 |
| 08-B | off | 31,337.389 | 2.513998 | 5.608147 | 11.491792 |
| 08-B | on | 30,159.285 | 2.802498 | 8.050747 | 66.644080 |

各组四轮为**中位数 [最小值，最大值]**，是逐轮分位数的分布，不是合并 600 万请求后的总体 P99 或 P99.9。

| 自动快照 | Engine | 成功 QPS | P99，ms | P99.9，ms |
| --- | --- | --- | --- | --- |
| off | A | 31,468 [30,172，31,828] | 2.493 [2.440，2.740] | 3.367 [3.242，4.735] |
| off | B | 31,210 [30,598，31,434] | 2.552 [2.512，2.636] | 3.739 [3.486，5.608] |
| on | A | 30,092 [25,869，30,611] | 2.799 [2.673，7.435] | 7.865 [4.076，13.832] |
| on | B | 29,784 [28,329，30,325] | 2.879 [2.657，2.998] | 6.800 [4.462，8.222] |

先逐对计算 B/A，再汇总四个比值，不能用版本组中位数相除代替配对。QPS 比值大于 1 表示 B 较高，延迟比值小于 1 表示 B 较低。

| 自动快照 | 分子 / 分母 | QPS B/A | P99 B/A | P99.9 B/A | max B/A |
| --- | --- | --- | --- | --- | --- |
| off | 02-B / 01-A | 0.995134 | 1.002179 | 1.004442 | 0.800800 |
| off | 03-B / 04-A | 0.991514 | 1.044559 | 1.136803 | 1.509847 |
| off | 05-B / 06-A | 0.961357 | 1.080243 | 1.161883 | 0.556694 |
| off | 08-B / 07-A | 1.038622 | 0.917667 | 1.184367 | 0.578748 |
| on | 02-B / 01-A | 0.990657 | 0.994120 | 1.361090 | 4.554845 |
| on | 03-B / 04-A | 0.937622 | 1.091304 | 0.574638 | 1.246368 |
| on | 05-B / 06-A | 0.981241 | 1.037110 | 1.032089 | 1.112482 |
| on | 08-B / 07-A | 1.165866 | 0.376918 | 0.582044 | 0.613977 |

| 四对比值中位数 | QPS B/A | P99 B/A | P99.9 B/A | max B/A |
| --- | --- | --- | --- | --- |
| off | 0.993324 | 1.023369 | 1.149343 | 0.689774 |
| on | 0.985949 | 1.015615 | 0.807066 | 1.179425 |

原始 ON 判票数据如下，单位均为 ns：

| 配对 B / A | A elapsed | B elapsed | A P99 | B P99 | 同时负向 |
| --- | --- | --- | --- | --- | --- |
| 02-B / 01-A | 49001804461 | 49463958982 | 2672815 | 2657099 | 否 |
| 03-B / 04-A | 49646984130 | 52949884326 | 2746866 | 2997667 | 是 |
| 05-B / 06-A | 50049242455 | 51006073157 | 2850530 | 2956313 | 是 |
| 08-B / 07-A | 57985424228 | 49735927896 | 7435304 | 2802498 | 否 |

第 2、3 对满足负向规则，共 **2/4**，自动撤销条件未触发。第 1 对虽然 P99 略低而不计负向票，其 P99.9 和 max 都更高，max B/A 为 **4.554845**；不能称该对通过。第 4 对 B 较好，但 A 是本批最慢的 ON 轮次；保留它，并不将该对自动解释为分组造成的收益。

OFF 前三对 QPS 较低、P99 较高，第四对相反。这是本批方向波动的一部分，既不能忽略，也不能自动归因于在 OFF 测量内未运行的周期快照。ON−OFF 差额不是快照的因果成本。四对都是描述性观测，不提供尾延迟上界或容量保证。

P99.9 与 max 直接来自每轮 report 的 1,500,000 个延迟样本。闭环客户端等待响应时不会维持固定外部到达率；这些分位数只描述已经发出的请求，不能表示暂停期间未发出的排队需求。max 是单轮极值，不能证明或否定更长暂停存在。

## 共同 idle 窗口与七项计量

只使用查询开始、结束都在客户端测量区间内的原始状态样本，检查累计字段为 uint64、没有倒退及失败。确定性选取最早、最晚的 `snapshot_in_progress=false` 内部样本作为共同端点，不按成本重选窗口。各字段来自同一 Engine 状态锁视图，但查询与客户端仍经墙钟近似对齐。

以下为 2026-09-14 UTC 的查询结束时间；内部 / idle 的完成数分别保留：

| ON 阶段 | 首 idle 查询结束 | 末 idle 查询结束 | 窗口，s | 内部 / idle 样本 | 内部 / idle 完成 |
| --- | --- | --- | --- | --- | --- |
| 01-A | 13:16:49.631939 | 13:17:38.283985 | 48.652046 | 188 / 188 | 9 / 9 |
| 02-B | 13:19:29.932417 | 13:20:18.958370 | 49.025953 | 189 / 189 | 9 / 9 |
| 03-B | 13:26:41.489998 | 13:27:34.139310 | 52.649312 | 203 / 203 | 10 / 10 |
| 04-A | 13:29:32.395361 | 13:30:21.872660 | 49.477299 | 191 / 191 | 9 / 9 |
| 05-B | 13:32:32.665739 | 13:33:23.551600 | 50.885861 | 197 / 197 | 9 / 9 |
| 06-A | 13:35:43.053517 | 13:36:32.938300 | 49.884783 | 193 / 193 | 9 / 9 |
| 07-A | 13:38:43.932398 | 13:39:41.071152 | 57.138754 | 223 / 221 | 10 / 9 |
| 08-B | 13:41:31.032988 | 13:42:20.392282 | 49.359294 | 191 / 191 | 9 / 9 |

**`07-A` ON 首两条内部样本仍忙，必须按预声明规则裁掉。** 原流 223 条、内部完成 10 次；共同 idle 窗口始于 13:38:43.932398、止于 13:39:41.071152，保留 221 条、57.138754 s、9 次完整快照。第一次内部样本位于一次已经开始的快照中，不能把其完成和不完整的阶段增量当作同一完整集合。其余七个 ON 轮次首末内部样本已空闲。本次窗口裁剪不删除客户端请求、慢值或原始流。

七个字段均在所有内部样本存在、合法且不降。设共同窗口完成数为 N，验证 `Δcapture_lock_acquisitions=N`、`Δhold_ns≤Δcapture_phase_ns`、`Δinstalled_bytes=Δwritten_bytes=N×105488918`。这个文件大小包含 28 字节文件头、100,000 个记录头及 CRC（每条合计 25 字节）、1,024 字节 value 和全部 key：`28 + 100000×(25+1024) + 588890 = 105488918 B`。预置、初始化的累计值经端点相减排除。

| ON 阶段 | Δhold 次数 | Δhold，ns | Δwrite 调用 | Δwritten = Δinstalled，B | Δcompact written，B |
| --- | --- | --- | --- | --- | --- |
| 01-A | 9 | 513,504,944 | 900,009 | 949,400,262 | 31,972,623 |
| 02-B | 9 | 536,990,430 | 14,517 | 949,400,262 | 23,962,840 |
| 03-B | 10 | 601,863,727 | 16,130 | 1,054,889,180 | 24,995,588 |
| 04-A | 9 | 518,994,502 | 900,009 | 949,400,262 | 33,296,531 |
| 05-B | 9 | 530,851,811 | 14,517 | 949,400,262 | 24,220,362 |
| 06-A | 9 | 513,472,211 | 900,009 | 949,400,262 | 32,879,859 |
| 07-A | 9 | 608,242,408 | 900,009 | 949,400,262 | 28,319,582 |
| 08-B | 9 | 522,728,268 | 14,517 | 949,400,262 | 23,532,434 |

三阶段也使用相同端点和相同 N，原始累计增量如下：

| ON 阶段 | Δcapture，ns | Δwrite，ns | Δcompact，ns |
| --- | --- | --- | --- |
| 01-A | 549,403,890 | 5,711,987,187 | 191,811,602 |
| 02-B | 570,439,071 | 4,372,644,087 | 174,917,401 |
| 03-B | 635,212,265 | 4,593,180,975 | 193,978,479 |
| 04-A | 551,594,955 | 5,903,465,124 | 202,174,897 |
| 05-B | 584,128,648 | 4,265,778,775 | 177,560,048 |
| 06-A | 561,991,013 | 6,436,002,092 | 192,165,867 |
| 07-A | 674,819,533 | 6,763,563,385 | 187,057,503 |
| 08-B | 557,031,815 | 4,056,351,916 | 184,394,193 |

阶段均值为 `Δphase_ns/N/1e6`，捕获锁均值为 `Δhold_ns/Δhold_count/1e6`；calls/N、written/calls 与 compact bytes/N 分别计算，不能把各段时间加成暂停：

| ON 阶段 | capture / 次，ms | hold / 次，ms | write / 次，ms | compact / 次，ms | write 调用 / 次 | compact B / 次 |
| --- | --- | --- | --- | --- | --- | --- |
| 01-A | 61.044877 | 57.056105 | 634.665243 | 21.312400 | 100,001 | 3,552,513.667 |
| 02-B | 63.382119 | 59.665603 | 485.849343 | 19.435267 | 1,613 | 2,662,537.778 |
| 03-B | 63.521226 | 60.186373 | 459.318098 | 19.397848 | 1,613 | 2,499,558.800 |
| 04-A | 61.288328 | 57.666056 | 655.940569 | 22.463877 | 100,001 | 3,699,614.556 |
| 05-B | 64.903183 | 58.983535 | 473.975419 | 19.728894 | 1,613 | 2,691,151.333 |
| 06-A | 62.443446 | 57.052468 | 715.111344 | 21.351763 | 100,001 | 3,653,317.667 |
| 07-A | 74.979948 | 67.582490 | 751.507043 | 20.784167 | 100,001 | 3,146,620.222 |
| 08-B | 61.892424 | 58.080919 | 450.705768 | 20.488244 | 1,613 | 2,614,714.889 |

| ON 分子 / 分母 | capture B/A | hold B/A | write B/A | compact B/A |
| --- | --- | --- | --- | --- |
| 02-B / 01-A | 1.038287 | 1.045736 | 0.765521 | 0.911923 |
| 03-B / 04-A | 1.036433 | 1.043705 | 0.700243 | 0.863513 |
| 05-B / 06-A | 1.039391 | 1.033847 | 0.662799 | 0.923994 |
| 08-B / 07-A | 0.825453 | 0.859408 | 0.599736 | 0.985762 |
| 四对中位数 | 1.037360 | 1.038776 | 0.681521 | 0.917958 |

同一完整快照，A / B 均写入并安装 **105,488,918 B**，A 平均每次 write 返回 **1,054.879 B**，B 为 **65,399.205 B**。实测每快照调用均为 100,001 / 1,613，计数反映每次 `write` 尝试，包括短写、EINTR 重试以及零或错误返回；字节只累计正返回。这里没有把调用数硬校验为理论常数，也不把“没有快照失败”理解为所有 syscall 都无重试。

`hold` 只计获取状态锁之后、复制 map / 读取序列 / 分离 pending 的临界区，不含获取该锁或 I/O 锁的等待、WAL 同步及后续阶段。它包含真实墙钟经过时间，不能直接解释为纯复制 CPU 时间。`capture` 还包括 I/O 锁等待、WAL 提交和边界定位；`write` 包括编码、CRC、写入、文件同步、rename、目录同步；`compact` 包括锁等待和 WAL 后缀回收。capture 与 WAL 提交计时重叠，阶段均值不是 P99，也不能相加成客户端暂停。

四对 write 均低于 1，但 hold 前三对较高、第四对较低，中位比值 **1.038776**。状态完整副本仍存在，不能将写分组描述为已经缩短状态锁暂停。compact bytes/N 的 B/A 为 0.675627–0.830960，工作量确实不同；后台周期在一次执行结束后再等 5,000 ms，write 时长改变完成频率和 WAL 后缀。因此 compact 阶段差异不能只当同等字节处理速度。

`snapshot_capture_state_lock_duration_ns_max` 是进程生命周期最大值，以下保留绝对 ns，不相减为窗口最大值：

| ON 阶段 | 首内部 max，ns | 末内部 max，ns | after max，ns | settled max，ns |
| --- | --- | --- | --- | --- |
| 01-A | 59256130 | 59888400 | 59888400 | 59888400 |
| 02-B | 136899372 | 136899372 | 136899372 | 136899372 |
| 03-B | 54761556 | 77311226 | 77311226 | 77311226 |
| 04-A | 59022184 | 62561764 | 62561764 | 62561764 |
| 05-B | 68298723 | 70014838 | 70014838 | 70014838 |
| 06-A | 63194773 | 63194773 | 63194773 | 63194773 |
| 07-A | 61569982 | 83839722 | 83839722 | 83839722 |
| 08-B | 57911324 | 63955521 | 63955521 | 63955521 |

`02-B` 的 **136.899372 ms** hold 最大值在首个内部样本已存在；不能用它定位测量中 **283.851464 ms** 的某个请求，也不能做两者相减来分解慢请求。OFF 的六项累计增量均为 0，次数均值为 null；其绝对 hold max 仍含 fresh directory 恢复时创建空快照的启动记录，不能硬改为 0。

## 结果完整性、CPU、内存与积压

16 轮均有效，失败、未命中、中断、缺失、invalid、未完成和覆盖不足均为 0。每轮 PUT **300,879**、GET **1,199,121**、DELETE **0**，共 1,500,000 次 HTTP 200，另有 100,000 次预置；内部 keys 始终为 100,000。after / settled 均为 applied=durable=**400,879**、WAL pending=0，48 个进程均正常退出，未强制终止。

throughput 在内存更新和 WAL 入队后即可确认，因此成功 QPS 不是已持久化吞吐。预置、百分位计算、报告生成和排空不计入客户端测量；after 位于压测退出之后，不能反推最后一个响应返回时已落盘。

以下每格为四轮中位数 [最小值，最大值]；排队均值先在各轮计算，再汇总：

| 指标 | A off | B off | A on | B on |
| --- | --- | --- | --- | --- |
| engine CPU，核 | 1.261 [1.225，1.262] | 1.248 [1.238，1.253] | 1.335 [1.171，1.348] | 1.278 [1.235，1.304] |
| gateway CPU，核 | 2.778 [2.678，2.799] | 2.752 [2.720，2.775] | 2.667 [2.308，2.702] | 2.631 [2.517，2.678] |
| bench CPU，核 | 1.947 [1.884，1.963] | 1.932 [1.908，1.939] | 1.866 [1.616，1.891] | 1.842 [1.760，1.878] |
| 平均排队，ms | 0.056208 [0.055338，0.057268] | 0.056463 [0.055065，0.057296] | 0.064554 [0.063292，0.069578] | 0.064557 [0.063085，0.065752] |
| 队列深度样本均值 | 0.986 [0.885，1.576] | 1.086 [0.799，1.126] | 1.240 [0.969，1.393] | 1.260 [0.877，1.333] |
| 活动数据线程样本均值 | 1.348 [1.152，1.500] | 1.349 [1.091，1.608] | 1.367 [1.277，1.637] | 1.360 [1.233，1.487] |
| 逐轮 pending 样本最大值，B | 69,095 [65,409，80,173] | 64,878 [59,077，84,394] | 215,194 [193,050，285,869] | 280,071 [218,349，367,098] |

CPU 按各进程自身 monotonic 窗口、固定 PID / starttime，以 `(Δutime+Δstime)/100/窗口秒数` 得到，1 核表示一个逻辑 CPU 的平均计算时间。三个进程的 CPU 值不能用于区分代码与主机原因；闭环完成速率变慢本身也会减少单位时间工作量。

排队为 `Δrequest_queue_wait_duration_ns_total/Δrequests_started_total`，只覆盖已开始执行的任务；gauge 均值是未加权采样算术均值，样本最大值不是真实峰值。WAL 容量等待、可靠确认等待的次数 / 时长增量和等待者样本均为 0，等待均值保留 null。没有请求拒绝、RPC 错误、重试或异步回调错误。这些观测没有解释尾延迟，也不表示没有其他锁等待。

内存表保留每轮测量 RSS 样本最大值的**中位数 [最小值，最大值]**，单位 MiB：

| 进程 RSS | A off | B off | A on | B on |
| --- | --- | --- | --- | --- |
| engine | 115.666 [115.141，116.039] | 115.686 [115.129，116.031] | 226.154 [226.039，226.258] | 225.887 [225.633，226.371] |
| gateway | 14.316 [14.312，14.324] | 14.176 [14.125，14.402] | 14.336 [14.297，14.625] | 14.316 [14.305，14.398] |
| bench | 35.162 [34.953，35.371] | 34.926 [34.840，35.445] | 35.213 [34.914，35.504] | 35.150 [34.926，42.625] |

HWM 自进程启动累计，可能覆盖预置或报告生成阶段；各轮已观察 HWM 的中位数 [最小值，最大值]如下，不能与测量 RSS 混为一个时间窗口：

| 进程 HWM，MiB | A off | B off | A on | B on |
| --- | --- | --- | --- | --- |
| engine | 115.666 [115.141，116.039] | 115.686 [115.129，116.031] | 226.154 [226.039，226.258] | 225.887 [225.633，226.371] |
| gateway | 14.316 [14.312，14.324] | 14.176 [14.125，14.402] | 14.336 [14.297，14.625] | 14.316 [14.305，14.473] |
| bench | 35.170 [34.961，35.387] | 34.936 [34.852，35.453] | 35.217 [34.930，35.516] | 35.158 [34.938，44.324] |

ON 的 Engine 仍约 **225.6–226.4 MiB**，分组没有取消完整 map 副本。`05-B` ON Benchmark 的 RSS 为 **44,695,552 B（42.625 MiB）**，HWM 为 **46,477,312 B（44.324219 MiB）**，高于其余 ON 轮次，完整保留。Benchmark 精确保存 150 万个延迟样本，内存随请求数增长；离散 RSS 也可能漏峰。

Engine 的完整测量资源窗口 I/O 增量如下，每格为四轮中位数 [最小值，最大值]，单位 B：

| Engine 进程 I/O | A off | B off | A on | B on |
| --- | --- | --- | --- | --- |
| read_bytes | 8,192 [0，8,192] | 10,240 [4,096，12,288] | 20,480 [16,384，24,576] | 8,192 [0，24,576] |
| write_bytes | 358,838,272 [358,047,744，362,381,312] | 359,561,216 [358,961,152，360,681,472] | 1,342,021,632 [1,341,161,472，1,390,120,960] | 1,333,878,784 [1,332,002,816，1,442,557,952] |
| cancelled_write_bytes | 0 [0，0] | 0 [0，0] | 0 [0，0] | 0 [0，0] |

这些是 `/proc/<pid>/io` 的进程存储计数，包含 WAL、快照及回收，不是应用 `write` 返回字节或纯快照写放大；不将 ON−OFF 当设备写放大，也不擅自扣掉 cancelled。Gateway / Benchmark 的 write 与 cancelled 增量均为 0，read 原始计数保留。

例如 `07-A` ON 的 Engine 资源窗口是 **57.875951391 s**，与共同 idle 的 **57.138754 s** 不同，包含首个已在进行中的快照活动；其 write_bytes 增量 **1,390,120,960 B** 不能除以 9 解释为九次固定工作量的快照成本。不同角色和状态查询各有窗口，不能互换端点或把累计时间相加。

## 测量后的文件检查与证据边界

全部 16 轮及子进程结束后，另外读取每轮 `data/snapshot.v1` 的 stat 大小和前 28 字节，校验 magic、header CRC、count、size、sequence 范围。全部通过。独立附件 [POST_RUN_CHECKS.json](../benmark/baselines/2026-09-14-snapshot-accounting/POST_RUN_CHECKS.json) 保留 16 个文件的原始 header hex、解析值及各项布尔检查，明确标为**测量后派生记录**，不混进原始归档。

OFF 的最终文件均为 28 B、count=0、sequence=0：fresh directory 初始化创建空快照，`close()` 只排空 WAL，不主动生成最终全量快照。ON 均为 **105,488,918 B / 100,000 records**，最终序列如下：

| ON 阶段 | 最终 snapshot sequence | 最后观测完成 sequence | 最终 applied |
| --- | --- | --- | --- |
| 01-A | 393174 | 393174 | 400879 |
| 02-B | 383214 | 383214 | 400879 |
| 03-B | 393589 | 393589 | 400879 |
| 04-A | 392509 | 392509 | 400879 |
| 05-B | 385905 | 385905 | 400879 |
| 06-A | 387496 | 387496 | 400879 |
| 07-A | 370902 | 370902 | 400879 |
| 08-B | 380458 | 380458 | 400879 |

本次最终文件序列都等于最后观测的完成序列；一般情况下，已经开始的快照可在关闭 join 期间完成，因此只要求不低于最后已完成观测、且不高于最终 applied。最终 checkpoint 不必等于 400,879，更不是某个客户端测量端点。

**本附件未读取和校验完整记录 CRC，也不是新增恢复测试。** 原始 data 文件未归档，读者只能复核保留的 header 解析 / CRC 与记录的 stat 结果，不能从附件重验已省略的数据内容。七项计量、无错误状态和文件头检查各有边界，不拼成超出实际执行范围的正确性声明。

## 当前处置与完整归档

本轮自动撤销分组规则未触发，结论仅为“不按该方向规则自动撤销”。局部 write 改善成立，捕获状态锁每次均值仍约 57–68 ms，端到端未获得整体验收，**283.851464 ms 极值和两个负向 ON 配对仍需处理**。下一步应针对状态捕获临界区和端到端尾延迟继续形成可核对证据，不用更短 write 阶段覆盖这些风险，也不以共享主机为既定原因。

[snapshot-accounting-1470-3179.tar.gz](../benmark/baselines/2026-09-14-snapshot-accounting/snapshot-accounting-1470-3179.tar.gz) 保存整个父目录：plan、8 份 host-before、8 份 stage host-context / manifest / index 及全部 16 轮 report / commands / result / 状态 / 资源 / 日志。只排除 binaries / data；所有慢轮次和窗口外采样仍保留，summary / details / audit 不在原始父目录内。

归档为 **209 文件、22,161,893 原始字节、1,583,687 压缩字节**，SHA-256 为 `5f4f7bbc365b64250c62a3744ef258063420a50ecb1f2456fb8864c400998fb4`。所有成员均为安全、唯一的普通文件，逐成员字节及排除项之外的完整源文件集合已核对。完整版本、plan、规则和附件哈希见 [MANIFEST.json](../benmark/baselines/2026-09-14-snapshot-accounting/MANIFEST.json)。

```sh
(cd benmark/baselines/2026-09-14-snapshot-accounting && sha256sum -c SHA256SUMS)
snapshot_accounting_extract_dir="$(mktemp -d)"
tar -xzf benmark/baselines/2026-09-14-snapshot-accounting/snapshot-accounting-1470-3179.tar.gz \
  -C "$snapshot_accounting_extract_dir"
python3 benmark/summarize.py \
  "$snapshot_accounting_extract_dir/snapshot-accounting-1470-3179"/[0-9][0-9]-[AB] --format json
```

实际搬移汇总退出 0，16 valid，其余失败 / 中断 / 缺失 / incomplete / invalid / unfinished / 快照证据不足均为 0；所有 per-run groups 与原位置相同。八阶段只出现省略二进制后的 `recorded_hashes_only` 提示，绝对原路径不需存在。记录的哈希不能重验省略的执行文件；CLI 分别汇总 stage，不自动合并 A/B 或执行本次配对判票，也不代替共同 idle / ≥3 完成的专项核对。

共同 idle 窗口、阶段与七指标的端点及差值还可使用仓库内的[离线分析工具](benchmark-experiments.md#离线复查快照阶段与持锁)重算：

```sh
python3 benmark/snapshot_report.py \
  "$snapshot_accounting_extract_dir/snapshot-accounting-1470-3179"/[0-9][0-9]-[AB] \
  --min-completed 3 --min-measurement-seconds 30 --format json
```

该命令分别呈现客户端有效性、观测覆盖和字段可用性。它计算通用逐轮差值；本页的固定文件字节数对账、B/A 配对及预声明判票仍需按上述公式核对。其最大值端点跟随共同 idle 窗口，本页最大值表另外保留完整内部流与 after / settled 观测，二者都不把最大值相减。

## 重建 A 源码与完整复现

A 的 `1470c5f` 位于独立 control 分支，不假设只获取主分支也能找到该提交。[unbuffered-control.patch](../benmark/baselines/2026-09-14-snapshot-accounting/unbuffered-control.patch) 是精确的 `git diff 3179b64 1470c5f -- cpp_engine/engine.cpp`，可在 3179 工作树中重建 A 的源代码差异。补丁与 POST_RUN_CHECKS 是单独附件，不混入 raw archive；SHA256SUMS 覆盖三者。

以下先完成构建、冻结和来源记录，再开始顺序测量。应用补丁后 A 是 **3179 + dirty patch**，不把新构建伪称原始 frozen A 文件；保存补丁差异和新 SHA。driver 与 B / Gateway 来自 clean 3179，Benchmark 来自 7d。所有新路径须尚不存在。

```sh
(
set -eu
snapshot_accounting_repo="$PWD"
snapshot_accounting_output_root="/tmp/minikv-snapshot-accounting"
snapshot_accounting_frozen_dir="/tmp/minikv-snapshot-accounting-binaries"
git worktree add --detach /tmp/minikv-snapshot-accounting-B 3179b64
git worktree add --detach /tmp/minikv-snapshot-accounting-A 3179b64
git worktree add --detach /tmp/minikv-snapshot-accounting-go7d 7d29382
git -C /tmp/minikv-snapshot-accounting-A apply \
  "$snapshot_accounting_repo/benmark/baselines/2026-09-14-snapshot-accounting/unbuffered-control.patch"
make -C /tmp/minikv-snapshot-accounting-B engine go JOBS=4
make -C /tmp/minikv-snapshot-accounting-A engine JOBS=4
make -C /tmp/minikv-snapshot-accounting-go7d go
mkdir "$snapshot_accounting_output_root" "$snapshot_accounting_frozen_dir"
cp /tmp/minikv-snapshot-accounting-A/build/engine "$snapshot_accounting_frozen_dir/engine-A"
cp /tmp/minikv-snapshot-accounting-B/build/engine "$snapshot_accounting_frozen_dir/engine-B"
cp /tmp/minikv-snapshot-accounting-B/bin/minikv-go "$snapshot_accounting_frozen_dir/gateway"
cp /tmp/minikv-snapshot-accounting-go7d/bin/minikv-bench "$snapshot_accounting_frozen_dir/bench"
chmod 0555 "$snapshot_accounting_frozen_dir"/*
sha256sum "$snapshot_accounting_frozen_dir"/* > "$snapshot_accounting_output_root/binaries.sha256"
git -C /tmp/minikv-snapshot-accounting-A rev-parse HEAD > "$snapshot_accounting_output_root/source-A-base.txt"
git -C /tmp/minikv-snapshot-accounting-A diff --binary -- cpp_engine/engine.cpp \
  > "$snapshot_accounting_output_root/source-A.diff"
git -C /tmp/minikv-snapshot-accounting-A status --short > "$snapshot_accounting_output_root/source-A-status.txt"
printf '%s\n' 01-A 02-B 03-B 04-A 05-B 06-A 07-A 08-B \
  > "$snapshot_accounting_output_root/stage-order.txt"
printf '%s\n' 'Measurement >=30s; ON internal/shared-idle completions >=3; earliest/latest idle endpoints.' \
  'ON raw B.elapsed>A.elapsed AND B.p99>A.p99 is negative; equal is not negative.' \
  'After all stages: >=3/4 negative triggers withdrawal of grouping only; non-trigger is not acceptance.' \
  'Keep all cases; insufficient counters do not erase client votes; stop on any failure/interruption.' \
  > "$snapshot_accounting_output_root/decision-plan.txt"

for snapshot_accounting_stage in 01-A 02-B 03-B 04-A 05-B 06-A 07-A 08-B; do
  snapshot_accounting_version="${snapshot_accounting_stage#*-}"
  if python3 /tmp/minikv-snapshot-accounting-B/benmark/experiment.py \
    --output "$snapshot_accounting_output_root/$snapshot_accounting_stage" \
    --engine "$snapshot_accounting_frozen_dir/engine-$snapshot_accounting_version" \
    --gateway "$snapshot_accounting_frozen_dir/gateway" \
    --bench "$snapshot_accounting_frozen_dir/bench" \
    --modes throughput --repeats 1 --requests 1500000 --workers 40 --keyspace 100000 \
    --op mixed --write-ratio 20 --delete-ratio 0 --value-size 1024 --seed 1 \
    --engine-workers 20 --rpc-pool 64 --gomaxprocs 4 \
    --wal-batch 64 --wal-flush-ms 2 --snapshot-ms 5000 \
    --sample-ms 100 --stats-ms 250 --run-timeout 900 \
    --startup-timeout 10 --shutdown-timeout 60 --settle-timeout 60; then
    :
  else
    printf 'Stage %s failed or was interrupted; preserve artifacts and stop.\n' \
      "$snapshot_accounting_stage" >&2
    break
  fi
done

set --
for snapshot_accounting_stage in 01-A 02-B 03-B 04-A 05-B 06-A 07-A 08-B; do
  if [ -d "$snapshot_accounting_output_root/$snapshot_accounting_stage" ]; then
    set -- "$@" "$snapshot_accounting_output_root/$snapshot_accounting_stage"
  else
    printf 'Planned stage %s was not executed.\n' "$snapshot_accounting_stage" >&2
  fi
done
if [ "$#" -gt 0 ]; then
  python3 "$snapshot_accounting_repo/benmark/summarize.py" "$@" --format json
else
  printf 'No stage artifacts exist; the declared plan is incomplete.\n' >&2
fi
)
```

数据位于输出目录所在文件系统，可将父路径改到待测盘的新目录并记录环境。归档中的 JSON plan / host-before / host-context 是另行采集，experiment 不会自动产生这层父计划；复现时也应冻结并保留。上述文本规则只记录判定，实际 ≥3 完成、共同 idle、七指标和 raw 判票按本页公式另核对。

失败或中断均可能返回 1，脚本停止后续阶段；已创建的不完整目录仍汇总，未创建阶段按预声明名单显示。部分目录汇总退出 0 不代表整个 8 阶段 / 16 轮计划完成。新构建要重新记录源码差异及执行文件哈希，不能保证字节或性能与本次相同。
