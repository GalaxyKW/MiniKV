# 控制实验：数据线程数与等待位置

[返回 README](../README.md) · [刷新间隔控制](performance-controls.md) · [实验程序](benchmark-experiments.md) · [等待指标](observability.md#区分三种等待)

将数据线程从 20 个增加到 40 个后，这组负载的请求排队明显减少，更多线程同时等待可靠确认，每次 WAL 提交对应的写记录近似值也增加。这支持继续研究如何释放等待持久化的工作线程。

**本页全部结果来自 `25cec55` 的同步等待实现。** 它已包含请求排队、WAL 容量等待和可靠确认等待指标；可靠请求仍在数据线程内等待持久化。这些结果不代表后续异步完成实现的性能。

## 固定条件与比较范围

先完整运行 20 线程的六轮参照，再用参照保存的三个执行文件运行 40 线程六轮；两批执行文件的 SHA-256 完全一致，客户端报告均为干净提交 `25cec555f70e338baaf8dad1eec579589f79bf5f`。每批分别关闭自动快照或设置为 1,000 ms，各重复三次；轮次在 `manifest.plan` 中保留，未筛选较快的轮次。

每轮新进程、新数据目录，先顺序预置 5,000 个 key，再测量 100,000 请求；40 个客户端闭环 worker，mixed 20% PUT / 5% DELETE / 其余 GET，128 字节 value，seed 1，单请求超时 2 s。预置时间不计入 QPS 与延迟；没有另加稳态预热阶段，也没有清空系统缓存，因此这里不是冷缓存实验。

两批均为 reliable 模式，WAL batch 64、flush 2 ms、队列 16 MiB；数据请求队列容量 128、最大连接数 256、RPC 池 64。两个 Go 进程均 GOMAXPROCS=4、GOGC=100、GOMEMLIMIT=off，资源采样 100 ms、状态采样 250 ms。除数据线程数和每轮路径、端口外，运行配置相同；固定采样配置不等于采样开销为零。

主机为 Xeon E3-1270 v3、8 个逻辑 CPU、Linux 6.8，数据目录位于 NTFS3 文件系统，C++ 使用 RelWithDebInfo（`-O2 -g -DNDEBUG`）。归档中两份 `host-context.json` 保留主机、挂载与负载背景。20→40 顺序没有随机交错或返回参照，未隔离其他主机负载，也未控制缓存和 CPU 频率；本项目在测量期间没有并行编译或测试。结果用于检查这台共享主机上这一负载的等待机制，不提供跨环境的性能保证。

## 客户端结果

以下均为每组三轮的**中位数 [最小值，最大值]**。成功 QPS 包括正常未命中；P99 是每轮各自算出的分位数，再汇总三轮数值，不是将请求合并后的总体 P99。

| 数据线程 | 自动快照 | 成功 QPS | 每轮 P99，ms |
| --- | --- | ---: | ---: |
| 20 | 关闭 | 5,111 [5,110，5,121] | 10.011 [9.982，10.056] |
| 40 | 关闭 | 9,399 [9,199，9,437] | 5.836 [5.682，8.218] |
| 20 | 1,000 ms | 5,101 [5,090，5,105] | 11.048 [11.016，11.379] |
| 40 | 1,000 ms | 9,382 [9,370，9,394] | 6.386 [6.200，6.898] |

全部 12 轮有效，均完成 100,000 测量请求，系统失败为 0；没有失败、缺失、中断或未完成轮次。40 线程关闭快照的首轮 P99 为 8.218 ms，高于后两轮的 5.682 和 5.836 ms，表中保留该波动。

六轮开启快照均有测量内部的实际活动证据：内部首尾状态样本间，20 线程每轮完成 19 次快照，40 线程每轮完成 10 次。固定请求数下，40 线程测量区间更短，经历的快照次数不同，不能把两组差异解释为相同快照工作量下的成本差异。

## 等待与提交的证据

只选择查询开始、结束都落在客户端测量区间内的状态样本：20 线程每轮 76–77 个，40 线程每轮 41–42 个。先逐轮计算，再取三轮的中位数。

| 指标 | 20，快照关闭 | 40，快照关闭 | 20，快照 1,000 ms | 40，快照 1,000 ms |
| --- | ---: | ---: | ---: | ---: |
| 已出队请求平均排队时间，ms | 3.272 | 0.094 | 3.274 | 0.095 |
| 请求队列深度的样本均值 | 16.84 | 0.61 | 17.84 | 1.12 |
| 活动数据线程数的样本均值 | 19.75 | 35.12 | 19.86 | 35.36 |
| 已结束可靠确认等待平均耗时，ms | 4.395 | 3.585 | 4.408 | 3.598 |
| 可靠确认等待者数量的样本均值 | 19.58 | 35.00 | 19.74 | 35.00 |
| 已结束 WAL 提交平均耗时，ms | 2.410 | 2.394 | 2.415 | 2.393 |
| 提交周期近似值，ms | 4.519 | 4.557 | 4.531 | 4.560 |
| 每次提交对应的写记录近似值 | 5.759 | 10.669 | 5.752 | 10.656 |
| RPC 池使用量的样本均值，容量 64 | 38.38 | 38.10 | 38.97 | 38.50 |
| RPC 池获取平均耗时近似值，µs | 0.986 | 0.825 | 0.982 | 0.825 |

12 轮内部窗口的 WAL 容量等待完成次数增量、累计等待耗时增量以及等待者样本均值和最大值均为 0。其**平均等待耗时不可计算（null）**，因为分母为零；它表示此窗口没有记录已结束的容量等待，不应写成测得平均等待为 0 ms，也不证明其他负载不会触发容量限制。

排队时间下降、近似批量增大，而提交耗时和周期仍分别接近 2.4 ms、4.5 ms。20 线程组几乎全部活动线程在等待可靠确认，40 线程组能同时容纳更多等待者。这与同步确认占住线程、限制下一批请求进入的机制相符；仅增加线程已能改变这一限制，不能将观察到的吞吐变化全部归因于某一种等待。

CPU 使用量也支持区分“线程活动”与“占满 CPU”。以下是每轮各进程消耗的 CPU 时间除以其采样区间，单位为核，再取三轮中位数；1 表示平均占用一个逻辑 CPU 的计算时间。

| 进程 | 20，快照关闭 | 40，快照关闭 | 20，快照 1,000 ms | 40，快照 1,000 ms |
| --- | ---: | ---: | ---: | ---: |
| Engine | 0.302 | 0.501 | 0.317 | 0.512 |
| Gateway | 0.590 | 0.855 | 0.600 | 0.854 |
| Benchmark | 0.434 | 0.639 | 0.440 | 0.637 |

这些是三个进程各自的 CPU 时间，不代表整台主机的空闲程度。平均值不能解释某一个尾延迟请求，也不能用来排除短时调度干扰。

## 如何复核等待数字

原始 `report.json` 给出 `measurement_started_at` 与 `elapsed_ns`，定义测量区间。`stats.jsonl` 每次查询的 `started_at`、`finished_at` 必须都在其中。对筛选后的序列先检查所需字段存在、各累计计数不降序，再取末值减首值；缺失或不合法应记为不可用，不能从旧字段推导新字段。

| 逐轮值 | 计算方式 |
| --- | --- |
| 排队平均耗时 | `Δ server.request_queue_wait_duration_ns_total / Δ server.requests_started_total` |
| 容量等待平均耗时 | `Δ engine.wal_capacity_wait_duration_ns_total / Δ engine.wal_capacity_waits_total` |
| 可靠确认等待平均耗时 | `Δ engine.wal_durable_wait_duration_ns_total / Δ engine.wal_durable_waits_total` |
| 提交平均耗时 | `Δ engine.wal_commit_duration_ns_total / Δ engine.wal_commits_total` |
| 提交周期近似值 | 内部末次与首次查询的结束时间之差 `/ Δ engine.wal_commits_total` |
| 每次提交的记录近似值 | `Δ engine.applied_sequence / Δ engine.wal_commits_total` |
| RPC 池获取耗时近似值 | `Δ gateway.rpc.pool_wait_duration_ns_total / Δ gateway.rpc.pool_acquires_total` |

纳秒除以 1,000,000 转为 ms；分母为零时结果为 null。队列与等待者等 gauge 使用样本的算术均值和最大值，不是时间加权均值。以上状态时间使用墙钟对齐，因此窗口只是近似对齐。

排队累计量在任务出队时计入，WAL 等待累计量在等待结束时计入，包括错误、关闭唤醒；仍在等待的 episode 没有计入这些完成量。窗口内结束的等待可能始于窗口之前。RPC 获取次数在入口增加、耗时在返回时增加，首尾还可能包含不同的请求集合。每批记录的分子是已应用记录增量，而非每次提交的精确批量；提交耗时包含 WAL 写入、同步与调度。不能将这些不同阶段、不同完成集合的平均耗时相加为端到端延迟。

CPU 从 `resources.jsonl` 为每个进程分别选择测量区间内的 `sampled_at_utc`，校验 PID 与 `result.json` 一致、`starttime_ticks` 不变、CPU tick 和 monotonic 时间不倒退。使用 `Δ(cpu_user_ticks + cpu_system_ticks) / clock_ticks_per_second / Δmonotonic_seconds`；tick 频率保存在 manifest，本次为 100。各进程的 monotonic 区间独立，不用状态窗口时长替代。

## 原始归档与复现

[归档目录](../benmark/baselines/2026-09-14-worker-control/)包含两个矩阵的全部原始报告、命令、状态、资源、退出结果、日志和主机背景；[MANIFEST.json](../benmark/baselines/2026-09-14-worker-control/MANIFEST.json) 记录运行版本、每份归档的文件数、原始字节数、压缩字节数和 SHA-256。

在当前仓库根目录离线复查：

```sh
(cd benmark/baselines/2026-09-14-worker-control && sha256sum -c SHA256SUMS)
worker_control_extract_dir="$(mktemp -d)"
tar -xzf benmark/baselines/2026-09-14-worker-control/workers20.tar.gz -C "$worker_control_extract_dir"
tar -xzf benmark/baselines/2026-09-14-worker-control/workers40.tar.gz -C "$worker_control_extract_dir"
python3 benmark/summarize.py "$worker_control_extract_dir/workers20" "$worker_control_extract_dir/workers40"
```

预期为 12 valid，失败、缺失、中断、未完成、invalid 与快照证据不足均为 0。`summarize.py` 复查原始报告和采样；上面的等待分析公式还需从原始 JSONL 计算，汇总工具没有提供这些等待均值。归档省略实际二进制与生成的 `data/`，因此二进制来源显示 `recorded_hashes_only`，不能重新验证被省略的执行文件内容；原绝对路径仅为来源记录，搬移后不必存在。

重跑时从同步实现版本建立新工作树，构建一次，再按 20→40 顺序使用第一批保存的执行文件：

```sh
git worktree add --detach /tmp/minikv-worker-control-25cec55 25cec55
cd /tmp/minikv-worker-control-25cec55
make JOBS=4
worker_control_engine="./build/engine"
worker_control_gateway="./bin/minikv-go"
worker_control_bench="./bin/minikv-bench"
for worker_control_threads in 20 40; do
  python3 benmark/experiment.py \
    --output "./benmark/results/workers-${worker_control_threads}" \
    --engine "$worker_control_engine" --gateway "$worker_control_gateway" --bench "$worker_control_bench" \
    --modes reliable --repeats 3 --requests 100000 --workers 40 --keyspace 5000 \
    --op mixed --write-ratio 20 --delete-ratio 5 --value-size 128 --seed 1 \
    --engine-workers "$worker_control_threads" --rpc-pool 64 --gomaxprocs 4 \
    --wal-batch 64 --wal-flush-ms 2 --snapshot-ms 1000 \
    --sample-ms 100 --stats-ms 250 --run-timeout 600 \
    --startup-timeout 10 --shutdown-timeout 15 --settle-timeout 10 || break
  worker_control_engine="./benmark/results/workers-20/binaries/engine"
  worker_control_gateway="./benmark/results/workers-20/binaries/gateway"
  worker_control_bench="./benmark/results/workers-20/binaries/bench"
done
```

工作树和输出目录必须尚不存在；输出目录所在文件系统就是数据所在文件系统。以上重跑的是配置与流程，构建环境不同不保证二进制哈希相同，主机条件不同也不保证性能相同。实验程序记录配置、构建元数据和进程采样；本次额外保存的 `host-context.json` 是手动采集的主机背景，并非该命令自动生成。

## 后续设计检验

在当前负载中，WAL 容量没有表现为等待来源；可靠确认等待却长期占用数据线程。这提供了一个明确的后续方向：让应用请求与等待持久化完成分离，同时保留可靠确认条件、WAL 字节限额、全程有界的在途请求以及关闭时的完成与错误处理。

这只是异步完成设计的动机。实现后仍需用相同的新二进制分别建立配置参照，保留失败与恢复测试，再独立测量排队、在途请求、提交批量、CPU 和尾延迟。本页没有异步实现的运行结果，也不据此改变默认线程数。
