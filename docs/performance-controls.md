# 控制实验：WAL 刷新间隔

[返回 README](../README.md) · [初始基线](performance-baseline.md) · [实验程序](benchmark-experiments.md) · [等待指标](observability.md#区分三种等待)

初始基线中，可靠模式的数据线程接近满载、请求队列经常非空，RPC 池仍有余量。WAL 平均提交约 2.4 ms，但两次提交之间平均约 4.5 ms。代码在完成提交后重新设置“当前时间 + 刷新间隔”的截止时间，因此先检查刷新间隔是否解释了其中一部分等待。

## 固定条件与比较范围

本次沿用初始基线保存的三个执行文件，逐份核对 SHA-256 相同；客户端报告仍为提交 `7c488b7`。记录程序所在工作区的后续改动没有替换这些执行文件，也没有将新增的等待指标混入本次实验。

每轮 100,000 请求、40 个闭环 worker、5,000 key、128 字节 value；mixed 20% PUT / 5% DELETE / 其余 GET，seed 1，先预置全部 key。引擎 20 个数据线程、RPC 池 64、两个 Go 进程均 GOMAXPROCS=4；WAL batch 64，资源 100 ms、状态 250 ms 采样。每种配置重复三次、每轮使用新数据目录，分别关闭快照或每 1,000 ms 触发快照。

两批控制只选择 reliable 模式，先将 flush 改成 1 ms，再回到 2 ms；初始基线还交替运行了 throughput。三个阶段不同时执行，没有随机交错参数，其他主机负载也没有完全受控。这里检查结果是否随参数改变并返回，不能据此给出跨环境的性能保证。主机、文件系统和构建背景见初始基线。

## 参数改变与返回的结果

下表是每组三轮的**中位数 [最小值，最大值]**。成功 QPS 包括正常未命中，P99 汇总每轮的分位数，不合并请求。

| 配置 | 自动快照 | 成功 QPS | 每轮 P99，ms |
| --- | --- | ---: | ---: |
| 初始 2 ms | 关闭 | 5,132 [4,920，5,141] | 10.023 [10.022，15.523] |
| 改为 1 ms | 关闭 | 6,599 [6,594，6,600] | 7.848 [7.809，7.918] |
| 回到 2 ms | 关闭 | 5,112 [5,102，5,114] | 10.046 [10.044，10.115] |
| 初始 2 ms | 1,000 ms | 5,100 [5,097，5,110] | 11.129 [11.115，11.220] |
| 改为 1 ms | 1,000 ms | 6,539 [6,506，6,548] | 9.790 [9.690，9.854] |
| 回到 2 ms | 1,000 ms | 5,103 [5,098，5,110] | 11.154 [10.806，11.216] |

两批控制共 12 轮，全部完成测量请求，系统失败为 0；六轮开启快照的测量区间内均观察到快照活动。关闭快照时，吞吐量与尾延迟随刷新间隔缩短而改善，改回 2 ms 后回到接近初始基线的水平。开启快照时也观察到这一方向，但三个阶段经历的运行时长与快照次数不同。

## 提交周期提供了什么证据

只使用查询起止时间完全落在测量区间内的状态样本。每轮先由累计量增量计算近似值，以下再取关闭快照三轮的中位数：

| 指标 | 初始 2 ms | 改为 1 ms | 回到 2 ms |
| --- | ---: | ---: | ---: |
| 已结束 WAL 提交平均耗时 | 2.409 ms | 2.391 ms | 2.414 ms |
| 提交周期近似值 | 4.510 ms | 3.487 ms | 4.517 ms |
| 周期减提交耗时 | 2.102 ms | 1.097 ms | 2.103 ms |
| 每次提交对应的写记录近似值 | 5.772 | 5.738 | 5.755 |

平均提交耗时和每批记录数接近，提交周期先减少约 1 ms、再返回约 4.5 ms，与调整刷新等待的机制一致。它支持将刷新时机列为当前负载的等待来源，但不能把所有吞吐量差异都精确归因于定时器。

提交周期为内部首尾样本结束时间之差除以已结束提交次数；每批记录近似值使用 `Δ applied_sequence / Δ wal_commits_total`，并非逐批记录的精确均值。提交耗时包括 write、fdatasync 和期间调度。周期与提交耗时之差还含其他锁、调度与窗口边界误差，不能当作单独的定时器测量。计数窗口首尾可能存在正在进行的请求或提交。

## 原始记录与复现

[控制实验归档](../benmark/baselines/2026-09-14-flush-control/)保留两批控制的全部原始报告、命令、状态、资源、日志与主机背景。[MANIFEST.json](../benmark/baselines/2026-09-14-flush-control/MANIFEST.json) 记录文件数、字节数与 SHA-256。初始 2 ms 数据来自[基线归档](../benmark/baselines/2026-09-14-7c488b7/)，没有重复打包。

在当前仓库根目录复查两批控制：

```sh
(cd benmark/baselines/2026-09-14-flush-control && sha256sum -c SHA256SUMS)
control_extract_dir="$(mktemp -d)"
tar -xzf benmark/baselines/2026-09-14-flush-control/flush1.tar.gz -C "$control_extract_dir"
tar -xzf benmark/baselines/2026-09-14-flush-control/flush2-return.tar.gz -C "$control_extract_dir"
python3 benmark/summarize.py "$control_extract_dir/flush1" "$control_extract_dir/flush2-return"
```

归档省略实际执行文件副本和生成的数据文件，因此便携复查显示 `recorded_hashes_only`；原实验中三份副本的 SHA-256 一致，省略后不能再次校验内容。绝对路径仅记录原运行环境，搬移目录不需要修改命令记录。

在对应版本的新工作树重跑参数比较：

```sh
git worktree add --detach /tmp/minikv-flush-control-7c488b7 7c488b7
cd /tmp/minikv-flush-control-7c488b7
make JOBS=4
for control_flush_ms in 1 2; do
  python3 benmark/experiment.py \
    --output "./benmark/results/flush-${control_flush_ms}" \
    --modes reliable --requests 100000 --workers 40 --keyspace 5000 --repeats 3 \
    --engine-workers 20 --rpc-pool 64 --gomaxprocs 4 \
    --wal-batch 64 --wal-flush-ms "$control_flush_ms" --snapshot-ms 1000 \
    --sample-ms 100 --stats-ms 250 --run-timeout 600
done
```

工作树与输出目录必须尚不存在。原实验使用 NTFS3 数据盘；重跑时可以把 `--output` 改到待评估的数据盘上的新目录，并记录环境。命令复现的是配置和流程，不保证吞吐量相同；旧提交没有汇总工具，复查新结果时从当前版本运行 `summarize.py` 并传入输出目录。

## 下一步

这次使用旧版冻结执行文件，尚不能直接区分数据线程内的容量等待与可靠确认等待。新增指标现已能分别记录请求排队、WAL 容量等待和可靠确认等待；下一轮先固定新版本建立 20 个数据线程的参照，再只把数据线程改为 40，检查排队是否减少、等待是否只是转入可靠确认阶段，以及近似批量如何改变。不能直接把旧版 20 线程作为新版 40 线程的唯一对照。

当前结果还不足以统一修改程序默认值。刷新更频繁可能改变其他写比例、数据量和存储设备下的提交成本，需要继续比较，并保留现有故障与恢复保证。
