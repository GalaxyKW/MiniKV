# 隔离原型：共享不可变 value 的快照成本与写入代价

[返回 README](../README.md) · [前一轮同口径计量](performance-snapshot-accounting.md) · [实验与离线工具](benchmark-experiments.md) · [指标定义](observability.md)

A = `3179b64` 使用独立的 string 值，B = `df6a64e` 使用 `shared_ptr<const string>`；两者保留同一 64 KiB 写缓冲及七项快照指标。**本轮仅作描述性实验，原型保留在隔离分支，未预设采用或撤销原型的性能阈值，也不因局部阶段变快自动并入主线。**

外层执行状态为 **cutoff**；全部 **24** 个计划 case 均保留，实际状态为 `{"not_executed": 4, "valid": 20}`。未执行、部分完成和覆盖不足不得当作有利结果，也不替换轮次。

主场景 P（primary）中，各轮捕获持锁均值的中位数从 **61.929→24.545 ms**，四对持锁耗时 B/A 的中位数为 **0.381417**；开启快照（ON）时，各轮 Engine 采样 RSS 最大值的中位数从 **225.664→153.951 MiB**。同时，关闭快照（OFF）时，该 RSS 中位数从 **115.959→141.422 MiB**，ON 的 write 阶段耗时 B/A 的配对中位数为 **1.046775**，局部收益有相应代价。

端到端不能只看中位数：P 的 `05-B / 06-A` ON 配对中，QPS 比值为 **0.834332**，P99 比值为 **2.268920**；`02-B` OFF 还出现 **210.533585 ms** 的最大延迟。写入压力场景 S（stress）每种快照配置只完成一个有效配对，ON 捕获持锁均值为 **27.432→26.392 ms**；OFF 的 QPS / P99 比值为 **0.989790** / **1.014768**。这些负向与不足证据均保留，原型不采用，不能以共享主机解释掉异常。

## 版本、门控与两个场景

固定 Gateway 为 3179、Benchmark 为 7d；运行前冻结文件，driver 为 `8027097cce1d2d2c2608f13e589313895f1b3801`。A/B 的修改限于 Engine value 表示，不改变 WAL / snapshot 编码、确认语义、写缓冲或计量发布位置。源码补丁单独保留，主线仅收录证据和文档。

| 文件 | 实际来源 | SHA-256 |
| --- | --- | --- |
| A Engine | 3179b64f9a1b37c9d50a294ca8141b73a75b557b | 9741b61c986aafdcdf9cf99d499c41e08c085f1f6390133d3db09185edbd5f5c |
| B Engine | df6a64e5cc7c663deb6269bfcd1d8e243c8b77fc | 5dd8f5f61e1b4f3fdbcb23aa06c61f8ff4ffa14e6a51b2f6eae3114e40ca5515 |
| 固定 Gateway | 3179b64 | ab05a47cecffc90d7c5e307ca850869fcd23f3f473ba4a98818c6a8de62f6f85 |
| 固定 Benchmark | 7d29382 | 2ef55eb18244b0e0e8b4b9d94318300c6090b59dda7423df529ac370f6bca4fc |

原型在测量前通过普通回归的 4 个 C++ 测试程序与 15 个 E2E 用例（4.16 s / 18.159 s）、ASAN/UBSAN 的 4 个 C++ 测试程序与 15 个 E2E 用例，以及 TSan 的 4 个 C++ 测试程序（29.06 s），上述检查的退出码均为 0。这些是功能与并发检查，不能代替性能验收；测量使用冻结的执行文件，driver HEAD 不能替代执行文件的来源记录。

P：100k key / 1,024 B value / PUT 20%，顺序 ABBABAAB；每阶段先运行 OFF，再运行间隔为 5 s 的 ON，共 16 case、每条件四对。S：100k key / 128 B value / PUT 80%，预声明顺序 ABBA、共 8 case、每条件两对；本次仅完成前两个阶段、每条件一对，证据更弱。外层先 P 后 S，两个场景分别分组，各自按相邻阶段配对并计算 B/A，不与过去 150 万请求或旧 Gateway 批次合并。

每轮使用全新进程与数据目录，先预置 100k key，再测量 **1,000,000** 请求；均为 throughput / mixed / DELETE 0% / seed 1，40 个客户端、20 个 Engine worker、RPC 池 64、GOMAXPROCS 4、WAL batch 64 / flush 2 ms。资源每 100 ms 采样，Stats 每 250 ms 采样；压测预算为 900 s（含预置与报告）、启动 10 s、关闭 60 s、排空 60 s。其余队列、连接与 Go 设置同冻结的 manifest 一致。

覆盖要求为测量时长 ≥20 s，ON 的内部及共同 idle 窗口中均完成 ≥3 次快照，且无快照失败；覆盖不足时保留原始值，相关均值为 null。**2026-09-14 14:40 UTC 后不启动新 stage**，已启动的 stage 可完成；失败或中断后停止两个场景的后续阶段，不补跑。原 runner 遇到非中断的 OFF 失败时，可能完成本 stage 的 ON 后再返回非零；停止边界以外层 plan 为准。

环境为共享 Xeon E3-1270 v3 / 8 个逻辑 CPU、Linux 6.8、NTFS3 数据盘，未清缓存、未额外稳态预热或隔离外部负载。各阶段的 host-before / host-context 原样保留；测量期间没有项目构建、测试、分析或并行矩阵。共享主机不构成忽略负向轮次的理由。

捕获仍在 state 锁内复制 key 与 map 节点，只共享 const string 值；它不是 O(1) 捕获。每次 PUT 用 make_shared 创建值对象及控制块（两者合并分配），超出小字符串优化（SSO）范围的内容还需分配字符缓冲，失去既有 string 容量复用；GET 仍在锁内复制独立的 Response value 并捕获目标 LSN。快照引用的旧值保留到 image 释放。128 B 不覆盖 SSO，100k key 的均匀访问不覆盖热点或慢快照的最坏驻留情况，throughput 的结果不能外推到 reliable。

## 全部客户端与相邻配对

下表延迟单位为 ms，QPS 来自各轮完整测量；`—` 不是零。缺失与未执行行保持其计划位置。

| 场景/阶段 | 快照 | 状态 | 成功 QPS | P99 | P99.9 | max |
| --- | --- | --- | --- | --- | --- | --- |
| P/01-A | off | valid | 29,867.256 | 2.854684 | 4.532857 | 12.792852 |
| P/01-A | on | valid | 29,338.842 | 2.856700 | 4.295505 | 82.822138 |
| P/02-B | off | valid | 29,883.061 | 2.807015 | 4.751064 | 210.533585 |
| P/02-B | on | valid | 30,477.953 | 2.616972 | 3.609053 | 27.722461 |
| P/03-B | off | valid | 29,993.849 | 2.875857 | 7.429819 | 24.335393 |
| P/03-B | on | valid | 30,074.195 | 2.735969 | 7.898000 | 31.890640 |
| P/04-A | off | valid | 30,513.359 | 2.715403 | 7.872514 | 10.780690 |
| P/04-A | on | valid | 30,186.538 | 2.703380 | 7.997904 | 64.214429 |
| P/05-B | off | valid | 29,853.564 | 2.953503 | 8.086846 | 19.694194 |
| P/05-B | on | valid | 23,740.886 | 8.612109 | 18.395585 | 35.829986 |
| P/06-A | off | valid | 29,727.124 | 3.242733 | 8.143406 | 11.384198 |
| P/06-A | on | valid | 28,454.979 | 3.795687 | 9.391768 | 68.375372 |
| P/07-A | off | valid | 23,564.750 | 7.953121 | 9.520367 | 21.698505 |
| P/07-A | on | valid | 22,591.073 | 8.137167 | 9.943142 | 80.142536 |
| P/08-B | off | valid | 24,138.810 | 7.934841 | 9.264735 | 12.826121 |
| P/08-B | on | valid | 23,525.279 | 8.010053 | 9.732932 | 36.973421 |
| S/01-A | off | valid | 23,448.534 | 8.032582 | 10.536968 | 25.933119 |
| S/01-A | on | valid | 22,527.490 | 8.248726 | 11.319401 | 38.769582 |
| S/02-B | off | valid | 23,209.128 | 8.151205 | 9.828475 | 17.493273 |
| S/02-B | on | valid | 23,399.156 | 8.033896 | 9.641950 | 36.252278 |
| S/03-B | off | not_executed | — | — | — | — |
| S/03-B | on | not_executed | — | — | — | — |
| S/04-A | off | not_executed | — | — | — | — |
| S/04-A | on | not_executed | — | — | — | — |

先逐对计算 B/A，再概括比值，不能用两组中位数相除替代配对；QPS 比值大于 1、延迟比值小于 1 表示 B 在该项上较好。配对缺一方则不计算，不调整顺序。

| 场景/快照 | B / A | QPS B/A | P99 B/A | P99.9 B/A | max B/A |
| --- | --- | --- | --- | --- | --- |
| P/off | 02-B / 01-A | 1.000529 | 0.983301 | 1.048139 | 16.457127 |
| P/off | 03-B / 04-A | 0.982974 | 1.059090 | 0.943767 | 2.257313 |
| P/off | 05-B / 06-A | 1.004253 | 0.910807 | 0.993055 | 1.729959 |
| P/off | 08-B / 07-A | 1.024361 | 0.997702 | 0.973149 | 0.591106 |
| P/on | 02-B / 01-A | 1.038826 | 0.916082 | 0.840193 | 0.334723 |
| P/on | 03-B / 04-A | 0.996278 | 1.012055 | 0.987509 | 0.496627 |
| P/on | 05-B / 06-A | 0.834332 | 2.268920 | 1.958692 | 0.524019 |
| P/on | 08-B / 07-A | 1.041353 | 0.984379 | 0.978859 | 0.461346 |
| S/off | 02-B / 01-A | 0.989790 | 1.014768 | 0.932761 | 0.674553 |
| S/off | 03-B / 04-A | — | — | — | — |
| S/on | 02-B / 01-A | 1.038693 | 0.973956 | 0.851807 | 0.935070 |
| S/on | 03-B / 04-A | — | — | — | — |

P：ON 有效配对为 4，其中 B 的 QPS 较低且 P99 较高的有 2 对；QPS / P99 比值中位数为 1.017552 / 0.998217。B 的 ON 最大延迟最高轮次为 08-B / 36.973421 ms。方向计数只描述本批，不构成采用或撤销规则。

S：ON 有效配对为 1，其中 B 的 QPS 较低且 P99 较高的有 0 对；QPS / P99 比值中位数为 1.038693 / 0.973956。B 的 ON 最大延迟最高轮次为 02-B / 36.252278 ms。方向计数只描述本批，不构成采用或撤销规则。

所有慢轮均保留，包括 A 的慢轮。CPU/RSS 变化不能单独归因于分配器，ON−OFF 的差值也不是快照的因果成本。闭环 worker 等待时不会维持固定到达率，P99/P99.9 仅描述已发出的请求，max 不是尾延迟上界；stress 即使跑满也仅两对，本次以实际有效配对数为准，不能证明没有 PUT 回退。

## 共同 idle 与七项指标

查询开始、结束都在客户端测量区间内；先验证全部内部成功样本与计数，再固定最早/最晚的 `snapshot_in_progress=false` 端点。三阶段与七指标使用同一窗口，禁止按结果换端点；两类失败增量都须为 0。以下 UTC 窗口和均值仅用于对应轮次。

| ON场景/阶段 | idle首末结束 UTC | 内部/idle N | 头/尾裁剪 | hold ms/次 | capture ms/次 | write ms/次 | compact ms/次 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| P/01-A | 14:23:08.802140–14:23:42.688882 | 6/6 | 0/0 | 64.394408 | 68.212698 | 475.053836 | 19.702945 |
| P/02-B | 14:24:55.828334–14:25:28.347189 | 6/6 | 0/0 | 22.930190 | 26.304288 | 492.935381 | 20.193648 |
| P/03-B | 14:26:41.563724–14:27:14.537954 | 6/6 | 0/0 | 22.677944 | 28.062004 | 503.080414 | 20.009688 |
| P/04-A | 14:28:27.752621–14:29:00.454472 | 6/6 | 0/0 | 58.244153 | 62.063173 | 488.971140 | 19.836357 |
| P/05-B | 14:30:18.381198–14:31:00.397549 | 7/7 | 0/0 | 26.159145 | 29.233211 | 589.720104 | 19.414465 |
| P/06-A | 14:32:13.768862–14:32:48.548262 | 6/6 | 0/0 | 59.462813 | 66.916485 | 483.618397 | 20.562947 |
| P/07-A | 14:34:14.008113–14:34:57.951066 | 8/8 | 0/0 | 70.313442 | 81.483008 | 516.187020 | 19.363257 |
| P/08-B | 14:36:19.604108–14:37:01.888347 | 8/8 | 0/0 | 26.260236 | 30.869455 | 545.046898 | 19.829331 |
| S/01-A | 14:38:22.828924–14:39:06.998537 | 9/9 | 0/0 | 27.432056 | 34.204756 | 110.533757 | 17.811871 |
| S/02-B | 14:40:27.985996–14:41:10.561961 | 8/8 | 0/0 | 26.392312 | 32.437725 | 129.297845 | 17.509836 |
| S/03-B | —–— | —/— | —/— | — | — | — | — |
| S/04-A | —–— | —/— | —/— | — | — | — | — |

P 的完整 snapshot 为 **105,488,918 B**，S 为 **15,888,918 B**：`28 + 100000×(25+value_size) + 588890`。检查同一窗口内 capture 获取次数 = N、hold 总量 ≤ capture 总量、written = installed = N × 文件大小；计数缺失、倒退或不等式失败时单独报告，保留客户端结果。下表的 hold、调用数与字节为原始增量，最后一列为末内部样本的生命周期最大值（绝对值，单位 ns）。

| ON场景/阶段 | Δ获取次数 | Δhold ns | Δwrite调用 | Δwritten / installed B | Δcompact B | hold lifetime max ns |
| --- | --- | --- | --- | --- | --- | --- |
| P/01-A | 6 | 386,366,450 | 9,678 | 632,933,508 / 632,933,508 | 15,348,636 | 82,080,366 |
| P/02-B | 6 | 137,581,138 | 9,678 | 632,933,508 / 632,933,508 | 17,570,262 | 24,268,245 |
| P/03-B | 6 | 136,067,663 | 9,678 | 632,933,508 / 632,933,508 | 17,774,922 | 25,221,736 |
| P/04-A | 6 | 349,464,920 | 9,678 | 632,933,508 / 632,933,508 | 17,185,208 | 62,229,791 |
| P/05-B | 7 | 183,114,014 | 11,291 | 738,422,426 / 738,422,426 | 16,685,177 | 51,030,225 |
| P/06-A | 6 | 356,776,879 | 9,678 | 632,933,508 / 632,933,508 | 16,869,739 | 65,524,434 |
| P/07-A | 8 | 562,507,535 | 12,904 | 843,911,344 / 843,911,344 | 17,302,247 | 77,284,597 |
| P/08-B | 8 | 210,081,888 | 12,904 | 843,911,344 / 843,911,344 | 19,068,172 | 34,400,728 |
| S/01-A | 9 | 246,888,505 | 2,187 | 143,000,262 / 143,000,262 | 2,717,912 | 35,328,253 |
| S/02-B | 8 | 211,138,494 | 1,944 | 127,111,344 / 127,111,344 | 3,071,995 | 33,207,076 |
| S/03-B | — | — | — | — / — | — | — |
| S/04-A | — | — | — | — / — | — | — |

hold 均值按累计时长增量 / 获取次数增量计算，阶段均值按累计时长增量 / N 计算；write 调用包含短写、EINTR、零返回或错误尝试，字节只计正返回。按首内部→末内部→after→settled 的顺序检查 max 不下降，绝不相减成窗口 max；它可能来自预置，不能据其数值定位某个慢请求。capture 含锁等待与 WAL 同步，write 含编码、CRC、sync 和 rename，阶段不能相加成纯复制暂停。

快照完成后再等 5 s，持续时长与值长度会改变完成频率和 WAL 后缀工作量；各轮的 compact bytes/N 分别保留，不将整轮 CPU/IO 除以 N 当作固定工作量。OFF 的完成增量为 0 时，均值为 null，不是快照成本为 0。各场景的专项观测问题数 / 覆盖不足数为：P 0 / 0；S 0 / 0；完整原始流与离线 CLI 可以重算。

## 资源、正确性与缺失证据

每格为各轮中位数 [最小值，最大值]。CPU 使用各角色自己的 monotonic 窗口，并校验 PID/starttime，按 `(Δutime+Δstime)/clk_tck/Δseconds` 计算。RSS 先取每轮测量期内的采样最大值，再汇总这些值的中位数与范围；HWM 同样按每轮已观察到的生命周期峰值汇总。内存单位为 MiB。

| 场景/快照/版本 | Engine CPU核 | Gateway CPU核 | Bench CPU核 | Engine RSS MiB | Engine HWM MiB | 排队均值ms |
| --- | --- | --- | --- | --- | --- | --- |
| P/off/A | 1.182 [0.980，1.215] | 2.626 [2.108，2.701] | 1.851 [1.486，1.908] | 115.959 [115.645，116.047] | 115.959 [115.645，116.047] | 0.057390 [0.055595，0.063803] |
| P/off/B | 1.208 [1.011，1.231] | 2.652 [2.154，2.664] | 1.873 [1.514，1.889] | 141.422 [121.219，163.840] | 141.434 [121.219，163.875] | 0.055453 [0.055030，0.065977] |
| P/on/A | 1.252 [1.023，1.294] | 2.560 [2.026，2.661] | 1.810 [1.429，1.878] | 225.664 [225.492，225.961] | 225.664 [225.492，225.961] | 0.063537 [0.062294，0.078196] |
| P/on/B | 1.193 [1.061，1.335] | 2.397 [2.093，2.702] | 1.695 [1.475，1.908] | 153.951 [133.230，172.199] | 153.979 [133.230，172.199] | 0.062841 [0.057110，0.069167] |
| S/off/A | 0.961 [0.961，0.961] | 2.058 [2.058，2.058] | 1.481 [1.481，1.481] | 29.625 [29.625，29.625] | 29.625 [29.625，29.625] | 0.069107 [0.069107，0.069107] |
| S/off/B | 0.976 [0.976，0.976] | 2.049 [2.049，2.049] | 1.477 [1.477，1.477] | 35.000 [35.000，35.000] | 35.000 [35.000，35.000] | 0.069095 [0.069095，0.069095] |
| S/on/A | 0.959 [0.959，0.959] | 1.980 [1.980，1.980] | 1.425 [1.425，1.425] | 54.051 [54.051，54.051] | 54.051 [54.051，54.051] | 0.076409 [0.076409，0.076409] |
| S/on/B | 1.011 [1.011，1.011] | 2.049 [2.049，2.049] | 1.474 [1.474，1.474] | 44.891 [44.891，44.891] | 44.891 [44.891，44.891] | 0.075032 [0.075032，0.075032] |

| 场景/快照/版本 | Gateway RSS MiB | Gateway HWM MiB | Bench RSS MiB | Bench HWM MiB |
| --- | --- | --- | --- | --- |
| P/off/A | 14.230 [14.098，14.316] | 14.230 [14.098，14.316] | 27.318 [27.289，27.766] | 27.330 [27.305，27.766] |
| P/off/B | 14.307 [14.051，14.320] | 14.311 [14.051，14.324] | 27.762 [27.324，27.926] | 27.762 [27.336，33.594] |
| P/on/A | 14.287 [14.246，14.340] | 14.287 [14.246，14.340] | 27.762 [27.273，27.832] | 27.762 [27.281，27.832] |
| P/on/B | 14.193 [14.113，14.246] | 14.193 [14.113，14.246] | 27.344 [27.309，27.637] | 27.355 [27.309，27.637] |
| S/off/A | 14.371 [14.371，14.371] | 14.371 [14.371，14.371] | 27.367 [27.367，27.367] | 27.387 [27.387，27.387] |
| S/off/B | 14.250 [14.250，14.250] | 14.250 [14.250，14.250] | 27.633 [27.633，27.633] | 27.633 [27.633，27.633] |
| S/on/A | 14.320 [14.320，14.320] | 14.320 [14.320，14.320] | 27.301 [27.301，27.301] | 27.309 [27.309，27.309] |
| S/on/B | 14.188 [14.188，14.188] | 14.188 [14.188，14.188] | 27.352 [27.352，27.352] | 27.363 [27.363，27.363] |

下表为 Engine 自身资源采样窗口内 `/proc/PID/io` 的计数增量，单位为 MiB；各组仍为每轮中位数 [最小值，最大值]。它使用 PID/starttime 一致的资源窗口，与上方共同 idle 快照窗口不同，也不能用 ON−OFF 推导写放大。

| 场景/快照/版本 | read_bytes Δ MiB | write_bytes Δ MiB | cancelled_write_bytes Δ MiB |
| --- | --- | --- | --- |
| P/off/A | 0.004 [0.004，0.008] | 228.980 [227.430，232.918] | 0.000 [0.000，0.000] |
| P/off/B | 0.004 [0.000，0.008] | 228.217 [228.051，231.535] | 0.000 [0.000，0.000] |
| P/on/A | 0.012 [0.008，0.027] | 848.143 [847.238，1,055.281] | 0.000 [0.000，0.000] |
| P/on/B | 0.006 [0.000，0.023] | 900.930 [847.395，1,055.492] | 0.000 [0.000，0.000] |
| S/off/A | 0.008 [0.008，0.008] | 167.266 [167.266，167.266] | 0.000 [0.000，0.000] |
| S/off/B | 0.000 [0.000，0.000] | 166.945 [166.945，166.945] | 0.000 [0.000，0.000] |
| S/on/A | 0.027 [0.027，0.027] | 306.641 [306.641，306.641] | 0.000 [0.000，0.000] |
| S/on/B | 0.016 [0.016，0.016] | 291.000 [291.000，291.000] | 0.000 [0.000，0.000] |

排队均值按 `Δwait_ns/Δstarted` 计算，分母是窗口内开始执行的数据请求数。gauge 均值是未加权的样本算术均值，采样 max 不是真实峰值。非零的等待、拒绝、RPC 错误与 retry 计数，以及各类在途数量相对自身 capacity 的检查均单独保留，不能因端点差值为 0 就忽略已有错误。进程的 /proc 存储 IO 包含 WAL、snapshot 与回收，不是应用 write 字节或设备写放大；共享旧值的驻留和控制块代价必须结合 OFF/ON 资源观察。

P 已执行轮次中：pending 的采样最大值为 396,623 B；单轮容量 / 确认等待完成数的最大值为 0 / 0；内部样本中 RPC error/retry 累计计数的最大值为 0 / 0。这些是观测计数，不解释尾延迟原因。

S 已执行轮次中：pending 的采样最大值为 37,976 B；单轮容量 / 确认等待完成数的最大值为 0 / 0；内部样本中 RPC error/retry 累计计数的最大值为 0 / 0。这些是观测计数，不解释尾延迟原因。

每个有效 case 均为 100 万请求、10 万预置，DELETE 为 0，miss/failure 均为零，keys 为 100k。P 为 PUT 200,791 / GET 799,209、最终 LSN 300,791；S 为 PUT 800,098 / GET 199,902、最终 LSN 900,098，均按 `100000+实际PUT数` 核对。settled 必须满足 applied = durable 且 pending 为 0，20 轮的 60 个子进程均正常退出，未强制终止；throughput 成功不等于响应时已落盘。客户端、观测不足和未执行状态分开，以下原始计划完整保留。

## 原始归档、文件头与可搬移核验

[snapshot-shared-values.tar.gz](../benmark/baselines/2026-09-14-snapshot-shared-values/snapshot-shared-values.tar.gz) 保存外层 plan/execution、两个场景的计划、所有已产生的 stage 与 host 记录；只排除 binaries/data，未删慢轮或窗口外采样。归档含 **264 个文件 / 原始大小 22,814,455 B / 压缩大小 1,675,471 B**，SHA 为 `1c9779361478da1ddb25588501d1a7d5bc7a9a9214a118ff45d0d976627c777b`。详情见 [MANIFEST.json](../benmark/baselines/2026-09-14-snapshot-shared-values/MANIFEST.json)。

[POST_RUN_CHECKS.json](../benmark/baselines/2026-09-14-snapshot-shared-values/POST_RUN_CHECKS.json) 是测量后派生附件，保留 24 个计划位置，以及实际检查的 stat、28 B 文件头十六进制内容、sequence、count 和 CRC；20 轮的文件头检查通过，4 个未执行的 case 明确标为未检查。**未校验完整 record CRC 或重跑恢复**。OFF 应留有 28 B 的空 checkpoint；ON 大小按场景核对，最终 checkpoint 的序列不必等于最终 applied，可在 close 期间完成。原始 data 未归档，读者不能从附件重验省略内容。

```sh
(cd benmark/baselines/2026-09-14-snapshot-shared-values && sha256sum -c SHA256SUMS)
shared_extract="$(mktemp -d)"
tar -xzf benmark/baselines/2026-09-14-snapshot-shared-values/snapshot-shared-values.tar.gz -C "$shared_extract"
# 仅已创建且具有有效manifest的stage能交给CLI；未执行计划仍须核对外层plan/execution。
python3 benmark/summarize.py "$shared_extract/snapshot-shared-values"/*/[0-9][0-9]-[AB] --format json
python3 benmark/snapshot_report.py "$shared_extract/snapshot-shared-values"/*/[0-9][0-9]-[AB] \
  --min-completed 3 --min-measurement-seconds 20 --format json
```

本次搬移复查记录：外层计划 case 的状态为 `{"not_executed": 4, "valid": 20}`；现有可汇总 stage 为 10，CLI 退出码为 0，其计数只覆盖这些 stage：`{"failed": 0, "incomplete": 0, "interrupted": 0, "invalid": 0, "missing": 0, "planned": 20, "snapshot_evidence_insufficient": 0, "unfinished": 0, "valid": 20}`。每轮 groups 在归一化输入根路径后相等，省略 binary 后的验证级别为 recorded_hashes_only。部分 stage 汇总退出 0 不代表 24 个计划 case 完整，损坏的 manifest 也必须保留在外层记录中。

## 重建隔离原型与新一次复现

[prototype.patch](../benmark/baselines/2026-09-14-snapshot-shared-values/prototype.patch) 是 3179→df6a64e 的 engine.h/cpp 精确差异，附带应用后的源码一致性检查；无需假设隔离分支在主线可达。补丁与 POST 是独立附件，不混入原始归档。新构建必须记录 **3179+dirty patch** 与新的 binary SHA，不能声称自动重现了原冻结文件。以下新路径须不存在；先构建再计时，并设置自己的预声明截止时刻。

```sh
(
set -eu
shared_repo="$PWD"; shared_out=/tmp/minikv-shared-values; shared_bin=/tmp/minikv-shared-binaries
git worktree add --detach /tmp/minikv-shared-A 3179b64
git worktree add --detach /tmp/minikv-shared-B 3179b64
git worktree add --detach /tmp/minikv-shared-go7d 7d29382
git -C /tmp/minikv-shared-B apply "$shared_repo/benmark/baselines/2026-09-14-snapshot-shared-values/prototype.patch"
make -C /tmp/minikv-shared-A engine go JOBS=4
make -C /tmp/minikv-shared-B engine JOBS=4
make -C /tmp/minikv-shared-go7d go
mkdir "$shared_out" "$shared_bin" "$shared_out/primary" "$shared_out/stress"
cp /tmp/minikv-shared-A/build/engine "$shared_bin/engine-A"
cp /tmp/minikv-shared-B/build/engine "$shared_bin/engine-B"
cp /tmp/minikv-shared-A/bin/minikv-go "$shared_bin/gateway"
cp /tmp/minikv-shared-go7d/bin/minikv-bench "$shared_bin/bench"
chmod 0555 "$shared_bin"/*
sha256sum "$shared_bin"/* > "$shared_out/binaries.sha256"
git -C /tmp/minikv-shared-B diff --binary > "$shared_out/prototype-source.diff"
shared_stages='primary/01-A primary/02-B primary/03-B primary/04-A primary/05-B primary/06-A primary/07-A primary/08-B stress/01-A stress/02-B stress/03-B stress/04-A'
printf '%s\n' "$shared_stages" > "$shared_out/stage-plan.txt"
shared_cutoff="$(date -u -d '+25 minutes' +%s)"
printf '%s\n' "$shared_cutoff" > "$shared_out/start-cutoff-epoch.txt"
for shared_stage in $shared_stages; do
  if [ "$(date -u +%s)" -ge "$shared_cutoff" ]; then break; fi
  case "$shared_stage" in primary/*) shared_size=1024; shared_put=20;; *) shared_size=128; shared_put=80;; esac
  shared_version="${shared_stage##*-}"
  if python3 /tmp/minikv-shared-A/benmark/experiment.py \
    --output "$shared_out/$shared_stage" --engine "$shared_bin/engine-$shared_version" \
    --gateway "$shared_bin/gateway" --bench "$shared_bin/bench" --modes throughput --repeats 1 \
    --requests 1000000 --workers 40 --keyspace 100000 --op mixed --write-ratio "$shared_put" \
    --delete-ratio 0 --value-size "$shared_size" --seed 1 --engine-workers 20 --rpc-pool 64 \
    --gomaxprocs 4 --wal-batch 64 --wal-flush-ms 2 --snapshot-ms 5000 --sample-ms 100 --stats-ms 250 \
    --run-timeout 900 --startup-timeout 10 --shutdown-timeout 60 --settle-timeout 60; then :
  else printf 'Failed/interrupted stage %s; preserve artifacts and stop.\n' "$shared_stage" >&2; break; fi
done
set --
for shared_stage in $shared_stages; do
  if [ -d "$shared_out/$shared_stage" ]; then set -- "$@" "$shared_out/$shared_stage"
  else printf 'Planned stage not executed: %s\n' "$shared_stage" >&2; fi
done
if [ "$#" -gt 0 ]; then python3 "$shared_repo/benmark/summarize.py" "$@" --format json; fi
)
```

该示例在启动序列前，将 stage 启动截止设为 25 分钟后，并不改写原始 14:40 计划。还应冻结完整参数与覆盖条件，并采集 host 背景；实验程序不会自动生成本文的外层 JSON 计划。数据盘取决于输出目录所在文件系统。先列明计划再运行，保留中断和未执行状态；新环境不保证相同字节、速度或观测覆盖。
