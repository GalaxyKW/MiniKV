# 控制实验：异步可靠确认与版本返回

[返回 README](../README.md) · [数据线程数对照](performance-worker-controls.md) · [异步设计](design.md#异步可靠确认与请求生命周期) · [运行指标](observability.md)

相同 20 个数据线程下，异步版本将可靠确认等待移出了数据线程；关闭快照时，排队均值从约 3.27 ms 降到 0.068 ms。重新运行旧版冻结执行文件后，排队、吞吐与尾延迟回到接近首次参照的水平。本文保留完整 18 轮结果，用于检查这一变化及其资源代价。

## 版本、顺序与固定条件

| 阶段 | 运行版本 | 请求处理方式 | 原始记录 |
| --- | --- | --- | --- |
| A：首次参照 | `25cec55` | 数据线程同步等待可靠确认 | [此前线程数对照中的 workers20](../benmark/baselines/2026-09-14-worker-control/) |
| B：异步实现 | `7d29382` | 保留响应和目标序列，由完成线程等待可靠确认 | [async7d.tar.gz](../benmark/baselines/2026-09-14-async-control/async7d.tar.gz) |
| A：返回参照 | 首次 A 保存的 `25cec55` 执行文件 | 同步等待可靠确认 | [sync25-return.tar.gz](../benmark/baselines/2026-09-14-async-control/sync25-return.tar.gz) |

这是 A→B→A 的版本返回安排，但不是紧邻、随机交错的纯 A/B/A 实验。首次 A 之后还运行过 40 线程对照，并完成异步实现的构建与测试；B 与返回 A 是时间上更近的对照。主机共享，其他负载、CPU 频率与系统缓存未完全受控，各矩阵按顺序执行；本项目测量期间没有并行构建或测试。

每阶段都只运行 reliable 模式，自动快照关闭 / 1,000 ms 各三轮。每轮新进程、新数据目录，先顺序预置全部 5,000 个 key，再测量 100,000 请求；客户端 40 个闭环 worker、mixed 20% PUT / 5% DELETE / 其余 GET、128 字节 value、seed 1、请求超时 2 s。预置不计入测量；没有另加稳态预热或清空系统缓存。

引擎数据线程固定 20，数据任务队列容量 128、连接上限 256；WAL batch 64、flush 2 ms、字节限额 16 MiB，RPC 池 64。两个 Go 进程均 GOMAXPROCS=4、GOGC=100、GOMEMLIMIT=off。资源采样 100 ms、状态采样 250 ms；采样配置固定，采样本身仍有开销。

三阶段在同一主机运行，设备记录均为 `/dev/nvme0n1p2`、NTFS3。首 A 的 `host-context.json` 在沙箱中采集，B 与返回 A 在启动实验的同一执行环境中采集；挂载条目、uid/gid 等记录视图不同，不能据此推断实际发生重挂载，也不能宣称主机记录逐项相同。归档保留原始视图和采集背景。

### 执行文件的来源

运行版本以每阶段 manifest 中三个执行文件的 SHA-256、每轮 `commands.json` 和客户端报告为依据。实验程序所在工作区的 HEAD 记录的是采集环境，尤其不能用它把旧版返回运行标为新版。

| 进程 | 同步版本 `25cec55` SHA-256 | 异步版本 `7d29382` SHA-256 |
| --- | --- | --- |
| Engine | `e2cc7bf1c5a68581bef1c79b69a47fcc0697f2ea047c6dc88abfb35d78fdcea7` | `5bf67c7eb3ddf2ce0c665c269d488fd2a811e53bb2ec284778261dcc68c390e8` |
| Gateway | `692a9bf5da02b8d240d01236911236335f2e2ae512bdd42d88c60741d378c831` | `6fed313c6f924c21871cc05cf5589a5a6189cde1663492d682ec5b5ea3981882` |
| Benchmark | `2dbc85c5e1bd64ee919b68c5678a29c41dca23b783548edbe9ad316488ae6bbd` | `2ef55eb18244b0e0e8b4b9d94318300c6090b59dda7423df529ac370f6bca4fc` |

压测客户端源码在这两个版本之间没有变化，但 Go 执行文件携带的 VCS 元数据不同，不能因此声称二进制相同。新版网关增加了可选 async 观测字段；本次比较的是整组程序版本，不是只替换某一行代码的成本测量。三个阶段的实际执行文件均已重新计算哈希；返回参照逐份与首次 A 一致。上述完整哈希也保存在每阶段原始 manifest 中，归档位置见[归档清单](../benmark/baselines/2026-09-14-async-control/MANIFEST.json)。

## 客户端结果

下表列出各阶段、快照开关组三轮的**中位数 [最小值，最大值]**。成功 QPS 包括正常未命中；P99 是各轮分位数的汇总，不把多轮请求合并为一个总体 P99。

| 阶段 | 自动快照 | 成功 QPS | 每轮 P99，ms |
| --- | --- | ---: | ---: |
| 首次 A：同步 | 关闭 | 5,111 [5,110，5,121] | 10.011 [9.982，10.056] |
| B：异步 | 关闭 | 9,616 [9,613，9,624] | 5.907 [5.484，6.844] |
| 返回 A：同步 | 关闭 | 5,128 [5,118，5,136] | 10.026 [9.934，10.027] |
| 首次 A：同步 | 1,000 ms | 5,101 [5,090，5,105] | 11.048 [11.016，11.379] |
| B：异步 | 1,000 ms | 9,541 [9,515，9,547] | 7.253 [7.205，7.276] |
| 返回 A：同步 | 1,000 ms | 5,103 [5,097，5,115] | 11.251 [11.045，11.294] |

18 轮均有效，全部完成 100,000 测量请求，系统失败为 0；没有失败、缺失、中断、invalid 或未完成轮次。各轮结束后的已应用与已持久化序列相等，未同步 WAL 为 0，三个子进程均正常退出。

九轮开启快照均有测量内部活动证据：首次 A 和返回 A 每轮在内部首尾状态样本之间完成 19 次快照，B 每轮完成 10 次。固定请求数下，新版运行更快、测量时间更短，经历的快照次数不同；不能据此给出相同快照工作量下的成本结论。

## 等待位置与请求名额

只使用查询开始与结束都在测量区间内的状态样本：两次同步阶段每轮 76–77 个，新版每轮 40–41 个。状态与客户端测量通过墙钟时间戳近似对齐；gauge 均值为未加权的采样算术均值。以下逐轮计算后取三轮中位数；off 表示关闭自动快照，on 表示 1,000 ms 周期。

| 指标 | 首 A off | B off | 返 A off | 首 A on | B on | 返 A on |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 已出队请求平均排队时间，ms | 3.272 | 0.068 | 3.268 | 3.274 | 0.066 | 3.287 |
| 请求队列深度的样本均值 | 16.84 | 0.65 | 16.64 | 17.84 | 0.83 | 17.18 |
| 活动 worker 的样本均值，容量 20 | 19.75 | 0.55 | 19.74 | 19.86 | 0.20 | 19.65 |
| 可靠确认等待者的样本均值 | 19.58 | 31.07 | 19.47 | 19.74 | 30.32 | 19.57 |
| 已结束可靠确认等待平均耗时，ms | 4.395 | 3.322 | 4.388 | 4.408 | 3.352 | 4.409 |
| Engine async 在途样本均值 | 不可用 | 31.07 | 不可用 | 不可用 | 30.32 | 不可用 |
| Server 请求在途样本均值 | 不可用 | 32.44 | 不可用 | 不可用 | 31.98 | 不可用 |

新版关闭快照组三轮的中位数中，已出队请求平均排队时间为 0.068 ms，活动 worker 的样本均值为 0.55；与此同时，可靠确认等待者的样本均值为 31.07，server 在途请求样本均值为 32.44。这些观测把“仍有请求等待持久化”与“数据线程仍被占住”区分开了。

两个版本的 `wal_durable_waiters` 都统计等待可靠确认的请求；同步版本的等待会占用数据线程，新版由完成线程处理。新版等待 episode 在完成线程取出回复时结束，包含发布持久化序列后被调度、以及等待前面回调的时间。它不是纯 `fdatasync` 耗时，也不能直接当作 worker 阻塞时间。

`engine.async_requests_inflight` 包括提交预留和仍持有回调资源的请求；`server.requests_inflight` 还覆盖执行排队及等待 reactor 消费或丢弃的结果，两者的释放边界不同，不应相加。引擎和调度器分别取样，整份状态也不是跨组件的原子快照。旧版没有这些字段，必须保留为不可用，不能用 0 或其他 gauge 补出。

新版两层容量均为 20 + 128 = 148，六轮各自的在途样本最大值均为 40，没有样本超过容量。`async_callback_failures_total` 在初始、测量采样及结束 / 排空状态中均为 0，排空后的两层在途数均为 0。样本最大值只代表已观察值，不代表连续时间中的真实峰值。本次只有 40 个闭环客户端，也不能据此证明名额耗尽时的容量或尾延迟。

全部 18 轮内部窗口的 WAL 容量等待完成次数增量、累计耗时增量，以及等待者样本均值和最大值均为 0，平均耗时为 null。这表明本次窗口未记录容量等待；它不代表大 value、更多写入或慢盘下仍不会等待。

各类等待均值只计算已结束 episode。分母为零时用 null，不称为测得 0 ms；不同请求集合、窗口边界和可能重叠的耗时不能相加为端到端延迟。已有统计公式见[等待数字复核](performance-worker-controls.md#如何复核等待数字)。

## 提交、CPU 与内存

以下同样是逐轮计算后取三轮中位数。

| 指标 | 首 A off | B off | 返 A off | 首 A on | B on | 返 A on |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 已结束提交平均耗时，ms | 2.410 | 2.384 | 2.406 | 2.415 | 2.393 | 2.413 |
| 提交周期近似值，ms | 4.519 | 4.445 | 4.515 | 4.531 | 4.466 | 4.530 |
| 每次提交对应的记录近似值 | 5.759 | 10.655 | 5.766 | 5.752 | 10.630 | 5.762 |
| Engine CPU，核 | 0.302 | 0.453 | 0.305 | 0.317 | 0.456 | 0.311 |
| Gateway CPU，核 | 0.590 | 0.874 | 0.595 | 0.600 | 0.864 | 0.586 |
| Benchmark CPU，核 | 0.434 | 0.647 | 0.436 | 0.440 | 0.640 | 0.431 |

提交耗时和周期没有随 QPS 成比例缩短，每次提交对应的写记录近似值却从约 5.76 增到 10.65，再随旧版返回。它与更多请求在不占住数据线程的情况下等待可靠确认这一机制相符。新版各进程每秒消耗的 CPU 时间也增加；处理更多请求的成本与新增分配、引用计数等开销混在一起，这些核数不能单独证明每请求成本变高或变低。

提交周期使用内部首尾查询结束时间之差除以已结束提交次数；记录近似值使用 `Δ applied_sequence / Δ wal_commits_total`。它们有窗口边界误差，不能当作逐批记录，也不能把“周期减提交耗时”精确称作定时器成本。

CPU 使用同一 PID / starttime 的用户与系统 tick 增量，分别除以各进程自身的 monotonic 采样区间和 manifest 记录的 tick 频率。1 核表示平均消耗一个逻辑 CPU 的计算时间；它不表示主机其余 CPU 都空闲，也不能揭示某个请求的调度延迟。

内存表的每个单元格为“测量区间 RSS 样本最大值 / 已观察到的进程 HWM”，两者分别取三轮中位数，单位 MiB。

| 进程 | 首 A off | B off | 返 A off | 首 A on | B on | 返 A on |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Engine | 6.01 / 6.01 | 5.90 / 5.90 | 6.02 / 6.02 | 7.32 / 7.32 | 7.20 / 7.20 | 7.34 / 7.34 |
| Gateway | 14.33 / 14.33 | 14.27 / 14.27 | 14.31 / 14.31 | 14.26 / 14.26 | 14.31 / 14.31 | 14.29 / 14.29 |
| Benchmark | 12.91 / 12.91 | 12.91 / 12.91 | 13.00 / 13.00 | 12.95 / 12.95 | 12.93 / 12.93 | 13.04 / 13.04 |

这里两种观测的轮次中位数相同，逐轮值仍可能不同。RSS 离散采样可能漏掉瞬时峰值；HWM 自进程启动累计，可能包含预置或测量后的阶段，但最后一次采样也不保证覆盖进程完整生命周期。不能将这些值称作工作负载的真实内存峰值，也不能从本次小数据集的细小差异推出异步实现降低了内存成本。

## 原始归档与复现

首次 A 使用[已有 worker-control 归档](../benmark/baselines/2026-09-14-worker-control/)，不重复打包。[新增归档](../benmark/baselines/2026-09-14-async-control/)保留 B 和返回 A 的全部报告、配置、状态、资源、进程退出结果、日志与主机背景，省略实际执行文件和生成的数据目录。每份新增归档包含 6 轮、69 个文件，文件数、字节数、SHA-256 和运行版本记在 [MANIFEST.json](../benmark/baselines/2026-09-14-async-control/MANIFEST.json)。

在当前仓库根目录检查并搬移复查完整 18 轮：

```sh
(cd benmark/baselines/2026-09-14-worker-control && sha256sum -c SHA256SUMS)
(cd benmark/baselines/2026-09-14-async-control && sha256sum -c SHA256SUMS)
async_control_extract_dir="$(mktemp -d)"
tar -xzf benmark/baselines/2026-09-14-worker-control/workers20.tar.gz -C "$async_control_extract_dir"
tar -xzf benmark/baselines/2026-09-14-async-control/async7d.tar.gz -C "$async_control_extract_dir"
tar -xzf benmark/baselines/2026-09-14-async-control/sync25-return.tar.gz -C "$async_control_extract_dir"
python3 benmark/summarize.py \
  "$async_control_extract_dir/workers20" \
  "$async_control_extract_dir/async7d" \
  "$async_control_extract_dir/sync25-return"
```

预期为 18 valid，失败、缺失、中断、未完成、invalid 和快照证据不足均为 0。三个目录分别统计，不跨版本合并。省略二进制的归档显示 `recorded_hashes_only`；原运行的完整哈希仍保留，但不能在归档内重验被省略的文件。等待细分需要按原始 JSONL 公式复算，仓库的汇总 CLI 不直接输出这些等待均值。

以下从两个明确版本的新工作树各构建一次，重新执行 A→B→A。它复现配置与版本顺序，没有重演历史上首次 A 与 B 之间的开发和线程数实验。所有构建先于测量，旧版返回使用第一批 A 保存的副本。实验程序统一从 B 工作树运行，记录的 driver HEAD 因此与旧版执行文件的版本不同。

```sh
async_control_repo="$PWD"
async_control_output_root="/tmp/minikv-version-controls"
git worktree add --detach /tmp/minikv-sync25-control 25cec55
git worktree add --detach /tmp/minikv-async7d-control 7d29382
make -C /tmp/minikv-sync25-control JOBS=4
make -C /tmp/minikv-async7d-control JOBS=4

run_async_control() {
  python3 /tmp/minikv-async7d-control/benmark/experiment.py \
    --output "$1" --engine "$2" --gateway "$3" --bench "$4" \
    --modes reliable --repeats 3 --requests 100000 --workers 40 --keyspace 5000 \
    --op mixed --write-ratio 20 --delete-ratio 5 --value-size 128 --seed 1 \
    --engine-workers 20 --rpc-pool 64 --gomaxprocs 4 \
    --wal-batch 64 --wal-flush-ms 2 --snapshot-ms 1000 \
    --sample-ms 100 --stats-ms 250 --run-timeout 600 \
    --startup-timeout 10 --shutdown-timeout 15 --settle-timeout 10
}

run_async_control "$async_control_output_root/sync-first" \
  /tmp/minikv-sync25-control/build/engine \
  /tmp/minikv-sync25-control/bin/minikv-go \
  /tmp/minikv-sync25-control/bin/minikv-bench &&
run_async_control "$async_control_output_root/async" \
  /tmp/minikv-async7d-control/build/engine \
  /tmp/minikv-async7d-control/bin/minikv-go \
  /tmp/minikv-async7d-control/bin/minikv-bench &&
run_async_control "$async_control_output_root/sync-return" \
  "$async_control_output_root/sync-first/binaries/engine" \
  "$async_control_output_root/sync-first/binaries/gateway" \
  "$async_control_output_root/sync-first/binaries/bench"

python3 "$async_control_repo/benmark/summarize.py" \
  "$async_control_output_root/sync-first" \
  "$async_control_output_root/async" \
  "$async_control_output_root/sync-return"
```

工作树和输出目录必须尚不存在；输出目录所在文件系统就是数据所在文件系统。将 `async_control_output_root` 改到待评估的数据盘上的新目录，并记录该环境。本次的额外 `host-context.json` 是手动采集，实验命令不会自动生成它。即使流程和配置一致，构建环境、主机与文件系统状态也可能影响哈希和性能。

## 可以回答什么，仍需验证什么

这组负载中，可靠确认异步化后，等待请求数量可以超过数据线程数，而大部分数据线程已不再被持久化等待占住；排队减少、近似提交批量增大，客户端 QPS 与尾延迟相应改善。旧版返回时这些指标回到接近首次参照的水平，提供了比单独前后各测一次更强的机制证据。共享主机、版本整体变化和非随机顺序仍限制精确归因，不能把所有差异都分配给某一处实现。

throughput 模式下新增名额分配、引用计数、完成处理等公共请求路径成本仍待独立验证。下一阶段保持工具要求的完整矩阵，新旧版本各运行快照 off/on × 三轮、每轮 500,000 请求；不会为了只看快照关闭而剪裁计划。本轮性能实验也没有覆盖慢盘、过载、长时间运行或大数据集，不能据本页改变默认值或作容量承诺。
