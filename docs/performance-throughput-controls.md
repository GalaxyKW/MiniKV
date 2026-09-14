# 控制实验：throughput 公共请求路径

[返回 README](../README.md) · [可靠模式版本对照](performance-async-controls.md) · [实验程序](benchmark-experiments.md) · [运行指标](observability.md)

异步确认在可靠模式下释放了数据线程，但请求名额、引用计数和完成处理也影响无需等待可靠确认的请求。本次用 throughput 模式做旧版→新版→旧版返回对照，保留全部 18 轮结果。**新版部分轮次吞吐下降、尾延迟扩大，尚不能证明公共请求路径没有性能回退，也不能把波动直接归因于代码。**

本页仅覆盖冻结执行文件 `25cec55 → 7d29382 → 25cec55`，不包含测量结束后开展的进一步 throughput 路径优化，也不代表仓库后续版本的性能。

## 固定条件与版本来源

| 阶段 | 运行版本 | 原始归档 |
| --- | --- | --- |
| A：首次参照 | `25cec55`，异步确认改造前 | [sync25-reference.tar.gz](../benmark/baselines/2026-09-14-throughput-control/sync25-reference.tar.gz) |
| B：异步改造后 | `7d29382` | [async7d.tar.gz](../benmark/baselines/2026-09-14-throughput-control/async7d.tar.gz) |
| A：返回参照 | 与首次 A 相同的冻结执行文件 | [sync25-return.tar.gz](../benmark/baselines/2026-09-14-throughput-control/sync25-return.tar.gz) |

每阶段都运行完整矩阵：throughput 模式 × 自动快照关闭 / 1,000 ms × 三次重复。旧版返回预先列入计划；三个阶段依次运行，没有随机交错版本或筛选轮次。每轮新进程、新数据目录，先顺序预置全部 5,000 个 key，再测量 500,000 请求；40 个客户端闭环 worker，mixed 20% PUT / 5% DELETE / 其余 GET，value 128 字节、seed 1、请求超时 2 s。预置时间不计入 QPS 和延迟，没有另加稳态预热或清空系统缓存。

引擎数据线程 20、任务队列容量 128、连接上限 256；WAL batch 64、flush 2 ms、未同步字节限额 16 MiB，RPC 池 64。两个 Go 进程均 GOMAXPROCS=4、GOGC=100、GOMEMLIMIT=off。资源每 100 ms、状态每 250 ms 采样；压测进程预算 600 s（含预置与报告生成），启动 10 s、关闭 15 s、WAL 排空 10 s。除执行文件、路径和临时端口外，三阶段运行参数与显式运行环境一致。

主机为 Xeon E3-1270 v3、8 个逻辑 CPU、Linux 6.8，数据位于 `/dev/nvme0n1p2` 的 NTFS3 文件系统；构建背景为 RelWithDebInfo。三个阶段的 `host-context.json` 在同一执行环境中采集，保留挂载、CPU、内存和负载记录。本项目测量期间没有并行构建、测试或其他矩阵，但其他主机负载、CPU 频率和缓存未隔离。阶段开始时的 1 分钟 load 分别为 8.68、12.30、11.53；这只是背景快照，不能据此确定慢轮次的原因。

三个阶段均从已保存的副本启动，实际文件重新计算 SHA-256；返回 A 与首次 A 逐份一致：

| 进程 | `25cec55` SHA-256 | `7d29382` SHA-256 |
| --- | --- | --- |
| Engine | `e2cc7bf1c5a68581bef1c79b69a47fcc0697f2ea047c6dc88abfb35d78fdcea7` | `5bf67c7eb3ddf2ce0c665c269d488fd2a811e53bb2ec284778261dcc68c390e8` |
| Gateway | `692a9bf5da02b8d240d01236911236335f2e2ae512bdd42d88c60741d378c831` | `6fed313c6f924c21871cc05cf5589a5a6189cde1663492d682ec5b5ea3981882` |
| Benchmark | `2dbc85c5e1bd64ee919b68c5678a29c41dca23b783548edbe9ad316488ae6bbd` | `2ef55eb18244b0e0e8b4b9d94318300c6090b59dda7423df529ac370f6bca4fc` |

运行版本由各阶段 manifest 的执行文件哈希、每轮 `commands.json` 和客户端报告共同核对。采集程序工作区的 HEAD 为 `930c97e`，不能拿它代替旧版或新版的运行版本。压测功能源码在 `25cec55` 与 `7d29382` 间没有变化，但 Go 二进制携带的 VCS 元数据不同；新版网关也增加了 async 状态字段。本次比较的是整组程序版本，不是单独测量一次 ticket 分配的成本。

## 客户端结果与正确性

下表为每组三轮的**中位数 [最小值，最大值]**。成功 QPS 包含正常未命中；P99 是各轮自己的分位数，再汇总轮次值，不是合并全部请求后的总体 P99。

| 阶段 | 自动快照 | 成功 QPS | 每轮 P99，ms |
| --- | --- | ---: | ---: |
| 首次 A：25cec55 | 关闭 | 32,780 [32,645，33,254] | 2.329 [2.311，2.365] |
| B：7d29382 | 关闭 | 32,026 [26,857，33,224] | 2.520 [2.245，4.958] |
| 返回 A：25cec55 | 关闭 | 32,511 [31,752，32,729] | 2.412 [2.336，2.916] |
| 首次 A：25cec55 | 1,000 ms | 32,696 [32,413，33,436] | 2.337 [2.265，2.409] |
| B：7d29382 | 1,000 ms | 30,414 [28,065，32,527] | 2.844 [2.425，4.607] |
| 返回 A：25cec55 | 1,000 ms | 32,816 [32,363，32,947] | 2.347 [2.300，2.484] |

新版 `r02-throughput-snapshot-off` 为 26,857 QPS / P99 4.958 ms，`r01-throughput-snapshot-on` 为 28,065 QPS / P99 4.607 ms。没有将这些慢轮次排除；旧版返回关闭快照的 P99 也有 2.336–2.916 ms 的变化。样本显示新版波动更大，需要进一步定位，不能仅凭接近旧版的最快轮次宣称没有额外成本。

18 轮全部有效，均完成 500,000 测量请求，系统失败为 0，没有失败、缺失、中断、invalid 或未完成轮次。各轮操作数完全一致：PUT 100,217、DELETE 24,755、GET 375,028。包含 5,000 次预置写入后，最终已应用和已持久化序列均为 **129,972**，未同步 WAL 为 0；54 个子进程均正常退出。DELETE 未命中也产生 WAL 记录，正常未命中不计系统失败。

## 测量区间与 WAL 排空

throughput 在内存更新且 WAL 入队后即可确认成功，**客户端 QPS 不等于已持久化写入吞吐**。`report.json` 的 `measurement_started_at` 与 `elapsed_ns` 定义测量区间；预置、报告生成和随后 WAL 排空均不计入该 QPS。

状态分析只保留查询起止时间都在测量区间内的样本。首次 A 每轮 57–59 个，新版每轮 58–71 个，返回 A 每轮 58–60 个；通过墙钟时间戳近似对齐。累计量取内部末值减首值，先检查计数不倒退；gauge 采用未加权采样算术均值和样本最大值，不当作连续时间均值或真实峰值。

本次 18 轮的内部 WAL pending 样本最大值分别落在 8,291–10,992 字节之间，未见容量等待。所有 `stats-after.json` 的 WAL 已排空，但该查询发生在客户端退出之后，已经经过百分位计算与 JSON 报告生成；它不证明最后一个响应返回时数据就已持久化。

`wal_drain_elapsed_ns` 为 479–1,083 ns，反映本次排空检查几乎都没有进入等待循环。这个计时在读取 after 状态和校验报告后才开始，不能当作“最后确认→持久化”的耗时，也不能用这些细小差值比较版本性能。工具允许 throughput 的 after 状态仍有尾部 WAL，但最终 settled 必须核对正确的序列并排空；这项正确性要求保留，不将排空阶段的 I/O 混入客户端测量窗口。

九轮开启快照均有内部活动证据。首次和返回 A 每轮在内部首尾样本间完成 14 次快照；B 按重复编号分别完成 17、15、14 次。固定请求数下，慢轮次运行更久，也经历更多快照，不能把这些结果视作相同快照工作量下的成本比较。

## 排队、CPU 与公共请求路径

以下先逐轮计算，再取各组三轮的中位数；off 表示自动快照关闭，on 表示 1,000 ms 周期。

| 指标 | 首 A off | B off | 返 A off | 首 A on | B on | 返 A on |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 已出队请求平均排队时间，ms | 0.057 | 0.055 | 0.058 | 0.057 | 0.058 | 0.057 |
| 请求队列深度的样本均值 | 0.97 | 0.55 | 1.07 | 1.61 | 1.19 | 1.45 |
| 活动数据线程的样本均值，容量 20 | 1.44 | 1.14 | 1.47 | 1.32 | 1.22 | 1.40 |
| Engine CPU，核 | 1.244 | 1.214 | 1.213 | 1.247 | 1.170 | 1.233 |
| Gateway CPU，核 | 2.702 | 2.644 | 2.688 | 2.700 | 2.530 | 2.694 |
| Benchmark CPU，核 | 1.956 | 1.919 | 1.940 | 1.963 | 1.837 | 1.950 |
| 已结束 WAL 提交平均耗时，ms | 2.666 | 2.636 | 2.614 | 2.665 | 2.664 | 2.628 |
| 提交周期近似值，ms | 4.815 | 4.786 | 4.759 | 4.817 | 4.858 | 4.778 |
| 每次提交对应的记录近似值 | 39.577 | 38.252 | 38.779 | 39.511 | 36.594 | 39.235 |

新版逐轮排队平均耗时为 0.0543–0.0590 ms，与两个旧版阶段的范围重叠。全部内部窗口的 WAL 容量等待和可靠确认等待完成次数、累计耗时增量与等待者样本最大值均为 0，等待均值因此为 null。各轮没有数据请求拒绝、RPC 错误或重试。平均排队和这些等待计数没有直接解释新版慢轮次，不能把端到端 P99 当成某一阶段的 P99。

throughput 不需要可靠确认回调：新版 `engine.async_requests_inflight` 样本均为 0，回调失败计数为 0。但 server 的整请求名额仍被使用，其逐轮样本均值为 12.86–14.28、样本最大值为 32–40，上限为 148。旧版缺少这些 async 字段，保留不可用，不补成 0。只有 40 个闭环客户端的实验也不能验证名额耗尽时的性能。

CPU 核数由每个进程自身的 monotonic 区间、用户与系统 tick 增量计算，校验 PID 与 starttime 一致，tick 频率为 manifest 中记录的 100。1 核表示平均消耗一个逻辑 CPU 的计算时间，不表示主机其余 CPU 空闲。新版慢轮次三个进程的 CPU 核数也低于其快轮次，但吞吐、CPU 时间、调度和等待共同变化，不能由此确定代码或外部负载谁是根因，也不能直接推算每请求分配成本。

排队均值为 `Δ request_queue_wait_duration_ns_total / Δ requests_started_total`；提交平均耗时为 `Δ wal_commit_duration_ns_total / Δ wal_commits_total`，本次提交失败为 0。提交周期使用内部查询结束时间之差除以提交次数；记录近似值使用 `Δ applied_sequence / Δ wal_commits_total`。完成量可能包含窗口前开始的工作，记录近似值不是逐批精确均值。不同区间、不同请求集合的耗时不能相加为端到端延迟，CPU 区间也不能用状态窗口长度替代。

## 内存观测

每格为“测量区间 RSS 样本最大值 / 已观察 HWM”，分别取三轮中位数，单位 MiB。

| 进程 | 首 A off | B off | 返 A off | 首 A on | B on | 返 A on |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Engine | 6.47 / 6.47 | 6.25 / 6.25 | 6.48 / 6.49 | 7.78 / 7.78 | 7.54 / 7.54 | 7.77 / 7.77 |
| Gateway | 14.03 / 14.03 | 14.21 / 14.27 | 14.28 / 14.28 | 14.26 / 14.26 | 14.22 / 14.22 | 14.23 / 14.23 |
| Benchmark | 19.64 / 19.64 | 19.58 / 19.58 | 19.73 / 19.73 | 19.72 / 19.72 | 19.68 / 19.68 | 19.61 / 19.61 |

RSS 离散采样可能漏掉瞬时峰值；HWM 自进程启动累计，可能包含预置及测量后的阶段，最后一个样本也不保证覆盖整个进程生命周期。因此二者不是工作负载的真实峰值，细小差异不能证明内存优化。资源流校验同一 PID / starttime；缺失字段不替换为零。

本次每轮 500,000 请求，压测器保留精确延迟样本，内存成本随请求数增长；其 RSS 样本最大值整体为 19.16–20.06 MiB。不能将这些值与[可靠模式对照](performance-async-controls.md)的 100,000 请求直接比较，并称作版本带来的内存回退。

## 原始归档与复现

[归档目录](../benmark/baselines/2026-09-14-throughput-control/)包含三个阶段全部原始报告、命令、状态、资源、日志、进程退出结果与主机背景。每份 6 轮、69 文件；只省略实际二进制副本与生成的 `data/`。文件数、原始 / 压缩字节数、运行版本与 SHA-256 见 [MANIFEST.json](../benmark/baselines/2026-09-14-throughput-control/MANIFEST.json)。

从当前仓库根目录校验并搬移复查：

```sh
(cd benmark/baselines/2026-09-14-throughput-control && sha256sum -c SHA256SUMS)
throughput_extract_dir="$(mktemp -d)"
tar -xzf benmark/baselines/2026-09-14-throughput-control/sync25-reference.tar.gz -C "$throughput_extract_dir"
tar -xzf benmark/baselines/2026-09-14-throughput-control/async7d.tar.gz -C "$throughput_extract_dir"
tar -xzf benmark/baselines/2026-09-14-throughput-control/sync25-return.tar.gz -C "$throughput_extract_dir"
python3 benmark/summarize.py \
  "$throughput_extract_dir/sync25-reference" \
  "$throughput_extract_dir/async7d" \
  "$throughput_extract_dir/sync25-return"
```

预期为 18 valid，失败、缺失、中断、未完成、invalid 和快照证据不足均为 0。三个目录分别统计，不跨版本合并。省略执行文件后显示 `recorded_hashes_only`；哈希仍可追溯，但无法从归档重验被省略的文件，原绝对路径也无需存在。等待细分按上文公式从原始 JSONL 复算，仓库汇总 CLI 不直接输出这些等待均值。

重跑时从两个明确版本各构建一次，先完成全部构建，再按 A→B→A 顺序运行；旧版返回使用第一批保存的执行文件。以下统一用 `7d29382` 的实验程序复现负载与版本安排，采集程序所在工作树的 HEAD 仍与旧版运行版本不同。

```sh
throughput_control_repo="$PWD"
throughput_output_root="/tmp/minikv-throughput-controls"
git worktree add --detach /tmp/minikv-throughput-sync25 25cec55
git worktree add --detach /tmp/minikv-throughput-async7d 7d29382
make -C /tmp/minikv-throughput-sync25 JOBS=4
make -C /tmp/minikv-throughput-async7d JOBS=4

run_throughput_control() {
  python3 /tmp/minikv-throughput-async7d/benmark/experiment.py \
    --output "$1" --engine "$2" --gateway "$3" --bench "$4" \
    --modes throughput --repeats 3 --requests 500000 --workers 40 --keyspace 5000 \
    --op mixed --write-ratio 20 --delete-ratio 5 --value-size 128 --seed 1 \
    --engine-workers 20 --rpc-pool 64 --gomaxprocs 4 \
    --wal-batch 64 --wal-flush-ms 2 --snapshot-ms 1000 \
    --sample-ms 100 --stats-ms 250 --run-timeout 600 \
    --startup-timeout 10 --shutdown-timeout 15 --settle-timeout 10
}

run_throughput_control "$throughput_output_root/sync25-reference" \
  /tmp/minikv-throughput-sync25/build/engine \
  /tmp/minikv-throughput-sync25/bin/minikv-go \
  /tmp/minikv-throughput-sync25/bin/minikv-bench &&
run_throughput_control "$throughput_output_root/async7d" \
  /tmp/minikv-throughput-async7d/build/engine \
  /tmp/minikv-throughput-async7d/bin/minikv-go \
  /tmp/minikv-throughput-async7d/bin/minikv-bench &&
run_throughput_control "$throughput_output_root/sync25-return" \
  "$throughput_output_root/sync25-reference/binaries/engine" \
  "$throughput_output_root/sync25-reference/binaries/gateway" \
  "$throughput_output_root/sync25-reference/binaries/bench"

python3 "$throughput_control_repo/benmark/summarize.py" \
  "$throughput_output_root/sync25-reference" \
  "$throughput_output_root/async7d" \
  "$throughput_output_root/sync25-return"
```

工作树与输出目录必须尚不存在。输出目录所在文件系统就是数据所在文件系统，可把 `throughput_output_root` 改到待评估数据盘的新目录，并记录环境。本次 `host-context.json` 是额外手动采集，实验命令不会自动生成。相同源码与配置不保证在不同构建环境中生成同一哈希，也不保证不同主机条件下性能相同。

## 后续如何判断成本

这组结果保留了需要解释的回退信号，而不是给出“throughput 无额外成本”的结论。下一步应在更受控的环境中交错运行固定版本，结合 CPU、分配与调度分析复现慢轮次；优化后也需重新冻结执行文件，复查相同负载和错误边界。只从源码识别可减少的分配，不等于已经证明该分配造成了这里的慢轮次。

本轮性能实验未覆盖慢盘、过载、大数据集或长时间稳定性；已有故障与恢复测试另行验证相应功能边界。这里的 QPS 和尾延迟不能作为容量承诺，也不能代替后续版本的测量。
