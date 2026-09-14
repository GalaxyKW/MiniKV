# 预实验：扩大数据集后的快照开销

[返回 README](../README.md) · [实验程序](benchmark-experiments.md) · [快照与运行指标](observability.md) · [此前 Engine 交错对照](performance-throughput-interleaved.md)

本次先在 **20,000 与 100,000 个 key、每个 value 1,024 字节**下测量未使用写出缓冲的 `d2d5eb8` Engine，确认较大快照是否有足够的观测覆盖，再决定下一步优化。两个数据集各做一次 throughput OFF/ON，共四轮。**这是一组单轮预实验，不是优化前后的性能对照，也不包含随后实现的快照缓冲。**

四轮均通过正确性校验和预声明覆盖门槛。100k ON 的写出阶段平均约 635.674 ms，长于捕获和回收阶段；源码逐条写出快照记录，提供了尝试有界写缓冲的依据。但阶段耗时同时包含编码、校验、写入和同步，不能由此认定系统调用是唯一瓶颈，也不能承诺缓冲会改善吞吐或尾延迟。

## 冻结条件与 pilot 门槛

先执行 20k，再执行 100k；每个阶段先关闭自动快照，再开启 5,000 ms 周期，各一次。每轮新进程、新数据目录，顺序预置整个 keyspace 后测量 1,500,000 请求，40 个闭环客户端 worker。mixed 为 20% PUT、0% DELETE、其余 GET，seed 1、value 1,024 字节、请求超时 2 s。DELETE 为 0，使全部预置 key 在测量期保持存在；标称 value 总量为 19.531 / 97.656 MiB，不等于进程内存占用。

Engine 数据线程 20、请求队列容量 128、连接上限 256，RPC 池 64。WAL batch 64、flush 2 ms、未同步字节限额 16 MiB。两个 Go 进程均 GOMAXPROCS=4、GOGC=100、GOMEMLIMIT=off；资源每 100 ms、状态每 250 ms 采样。压测进程预算 900 s，包含预置与报告生成；启动预算 10 s、关闭 60 s、WAL 排空 60 s。

运行前冻结两项覆盖标准：每轮客户端测量至少 **30 s**；ON 轮次在测量内部首尾状态样本间至少完成 **3 次快照**。若覆盖不足，只据此调整未来另行声明的正式实验，保留当前全部结果，不悄悄替换。本次四轮测量为 47.691–50.113 s，两个 ON 内部完成数为 10 / 9，均满足标准。

主机为 Xeon E3-1270 v3、8 个逻辑 CPU、Linux 6.8，数据位于 `/dev/nvme0n1p2` 的 NTFS3 文件系统，构建背景为 RelWithDebInfo。两阶段开始时 1 分钟 load 为 7.71 / 15.78，MemAvailable 为 2,696,908 / 2,728,916 KiB，均随主机背景记录保存。测量期间没有并行的项目构建、测试或其他矩阵；其他主机负载、CPU 频率、缓存和阶段间隔没有隔离。没有随机化、返回对照、额外稳态预热或清空系统缓存。

两个阶段使用相同冻结文件，结束后重算全部六份副本 SHA-256，并核对四轮实际命令：

| 文件 | 运行来源 | SHA-256 |
| --- | --- | --- |
| Engine | `d2d5eb862ee1cf11fcbeb68c04c66d740613338e` | `efabc96b11ae376fa020b052215f57af286729d7c5b76722b9ddaba6d9f022a0` |
| Gateway | `7d293825a6bc6963ef9856b63cd08f932ee91f76` | `6fed313c6f924c21871cc05cf5589a5a6189cde1663492d682ec5b5ea3981882` |
| Benchmark | `7d293825a6bc6963ef9856b63cd08f932ee91f76` | `2ef55eb18244b0e0e8b4b9d94318300c6090b59dda7423df529ac370f6bca4fc` |

实际采集工作区 HEAD 为 clean `aac9c1b`，它不是 Engine 的运行版本。客户端报告均记录 `7d29382`，与固定 Benchmark 文件一致。原始 plan 的 SHA-256 为 `a4872fc41fc49bb16f20f0c9a3d23e07a0d837d0d27862f033960a4f9c43c3cb`。

## 全部客户端结果

每格是对应单轮的值，没有把一次结果称作重复实验的中位数，也没有合并四轮的分位数。

| keyspace | 自动快照 | 预置，s | 测量，s | 成功 QPS | P99，ms | P99.9，ms | max，ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 20,000 | 关闭 | 3.801 | 48.655 | 30,829.264 | 2.631856 | 3.754426 | 18.155781 |
| 20,000 | 5,000 ms | 3.808 | 47.996 | 31,252.785 | 2.550417 | 3.675185 | 23.699435 |
| 100,000 | 关闭 | 19.002 | 47.691 | 31,452.587 | 2.535097 | 3.503661 | 18.361464 |
| 100,000 | 5,000 ms | 18.978 | 50.113 | 29,932.641 | 2.825283 | 7.563305 | 66.915110 |

100k ON 的 P99.9 和最大值高于其 OFF 轮次；20k ON 的 QPS、P99 与 P99.9 反而略好，最大值略高。各条件只有一次，顺序固定且共享主机未隔离，不能把这些差值直接归因为快照，也不能据此计算稳定的性能损失比例。

P99.9 与 max 来自原始 `report.json` 的 `latency_ns.p99_9` / `max`，不是从只保存 P99 的 `result.metrics` 推断。压测器记录每轮全部 1,500,000 个请求的精确延迟；max 是一次测量中的极值，不是上界保证。闭环 worker 在等待响应时不会继续按固定频率发新请求，因此这些分位数只描述已发出的请求，不能代替固定到达率下的排队尾延迟或过载结果。本次也没有逐请求时间线，无法将某一个慢请求与一次快照阶段精确对应。

四轮均为 1,500,000 次 HTTP 200，PUT **300,879**、GET **1,199,121**、DELETE **0**；没有未命中、系统失败、丢失或未完成请求。包含预置后，20k 最终 LSN 为 **320,879**，100k 为 **400,879**；`stats-after.json` 与 `stats-settled.json` 的 applied / durable 均达到对应值，pending 为 0。12 个子进程均正常退出。

throughput 在内存更新和 WAL 入队后即可确认；这里的 QPS 不是已持久化写入吞吐。预置、百分位计算、报告生成和随后 WAL 排空不计入客户端测量区间。after 查询发生在 Benchmark 退出之后，不能反推最后一个响应返回时数据已经落盘。

## 快照阶段：共同窗口与准确口径

先按 `measurement_started_at` 与 `elapsed_ns` 定义客户端测量区间，再选取查询开始和结束都在该区间内的状态样本。四轮内部样本数按上表顺序为 187、185、183、193。状态查询与客户端通过墙钟近似对齐；计数取末值减首值前检查所有内部样本没有倒退。

计算每次快照阶段均值时，选择**两端均为 `snapshot_in_progress=false` 的同一内部子窗口**，三阶段使用相同边界和完成数，且检查快照失败为 0。本次两个 ON 的首末内部样本恰好都空闲，无需缩窗。以下时间为 2026-09-14 UTC 的查询结束时间：

| keyspace，ON | 共同窗口起点 | 共同窗口终点 | 内部长度，s | 样本数 | 首 / 末完成计数 | 完成增量 |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| 20,000 | 11:58:19.149850 | 11:59:07.022131 | 47.872281 | 185 | 1 / 11 | 10 |
| 100,000 | 12:00:58.117316 | 12:01:48.009910 | 49.892594 | 193 | 4 / 13 | 9 |

100k ON 在测量前已发生三次额外快照，首个内部完成计数为 4；没有将预置期活动混入内部的 9 次完成。自动快照线程在上一次执行结束后再等待周期，5,000 ms 不代表固定墙钟频率。OFF 的内部完成增量和三阶段增量均为 0；本次每轮使用新目录，`recover()` 创建 WAL 后先执行一次初始快照，因而启动完成计数为 1，它不充当测量期快照活动。已有 v1 数据目录的恢复路径不会因此自动增加一次完成计数。

| keyspace，ON | capture 总增量，ns | write 总增量，ns | compact 总增量，ns |
| --- | ---: | ---: | ---: |
| 20,000，10 次 | 140,456,004 | 1,361,967,727 | 171,261,284 |
| 100,000，9 次 | 552,092,096 | 5,721,065,427 | 189,395,129 |

| keyspace，ON | capture / 次，ms | write / 次，ms | compact / 次，ms |
| --- | ---: | ---: | ---: |
| 20,000 | 14.045600 | 136.196773 | 17.126128 |
| 100,000 | 61.343566 | 635.673936 | 21.043903 |

每项均值按 `Δ snapshot_*_duration_ns_total / Δ snapshot_successes_total / 1e6` 计算。这里窗口两端空闲、失败为 0，使阶段完成量与同一组完整快照对应；若边界仍在进行中，就不能随意用阶段增量除以快照完成数称作精确均值。OFF 分母为 0，阶段均值不可用，不补成 0 ms。

三个阶段的含义分别是：

- **capture**：等待 I/O 锁、复制状态、同步捕获的 WAL 批次、确定 WAL 边界。它不是纯状态复制耗时，更不是已经测出的全局暂停时间。
- **write**：快照编码与 CRC、文件写入、同步、rename 和目录同步。这段在状态锁、WAL I/O 锁外进行，其总时长不是所有请求被阻塞的时长。
- **compact**：等待 I/O 锁、复制 WAL 后缀并持久化安装，也不是纯内存复制。

阶段计时在阶段结束时累计，包含该阶段内的失败；等待前一个快照和最终销毁内存副本不计入三个阶段。capture 还可能包含一次 WAL 提交，与 WAL 提交计时重叠。不能把这些累计量与 WAL、请求排队或客户端延迟相加为总暂停，也不能把上述阶段平均值当作阶段 P99。

## 内存、CPU、排队和进程 I/O

内存每格为“测量区间 RSS 样本最大值 / 已观察 HWM”，单位 MiB：

| 进程 | 20k off | 20k on | 100k off | 100k on |
| --- | ---: | ---: | ---: | ---: |
| Engine | 27.367 / 27.367 | 49.258 / 49.258 | 115.855 / 115.855 | 225.418 / 225.418 |
| Gateway | 14.422 / 14.422 | 14.059 / 14.078 | 14.414 / 14.414 | 14.395 / 14.395 |
| Benchmark | 34.848 / 34.855 | 34.930 / 34.941 | 35.371 / 35.379 | 34.918 / 34.926 |

100k Engine 的 RSS 样本最大值精确为 OFF **121,483,264 B**、ON **236,367,872 B**。源码捕获一份完整的内存状态副本，与 ON 更高内存的观测相符，但 RSS 还包括容器、字符串分配与分配器保留，不能把差值当成精确的副本字节数。离散采样可能漏掉峰值；HWM 自进程启动累计，可能包含预置或测量后阶段，最后一个样本不保证覆盖整个生命周期。所有资源观测均校验 PID / starttime，缺失字段不补成零。

Benchmark 为精确分位数保留 1,500,000 条延迟，单个 `time.Duration` 切片本身约 12,000,000 B；进程 RSS 还含 worker、HTTP 和运行时开销。不能把这组数据与 100k 或 500k 请求实验直接比较，并称为版本内存回退。

| 指标 | 20k off | 20k on | 100k off | 100k on |
| --- | ---: | ---: | ---: | ---: |
| Engine CPU，核 | 1.232 | 1.282 | 1.242 | 1.319 |
| Gateway CPU，核 | 2.718 | 2.748 | 2.762 | 2.642 |
| Benchmark CPU，核 | 1.910 | 1.937 | 1.932 | 1.850 |
| 已出队请求平均排队时间，ms | 0.057798 | 0.057484 | 0.057204 | 0.064948 |
| 队列深度样本均值 / 最大值 | 1.086 / 15 | 0.859 / 11 | 1.022 / 24 | 1.145 / 14 |
| 活动数据线程样本均值 / 最大值，容量 20 | 1.390 / 10 | 1.449 / 11 | 1.142 / 9 | 1.342 / 13 |
| WAL pending 样本最大值，B | 84,349 | 105,437 | 67,517 | 260,557 |
| applied−durable 样本最大值 | 80 | 100 | 64 | 247 |

队列均值使用 `Δ request_queue_wait_duration_ns_total / Δ requests_started_total`。gauge 均值为未加权样本算术均值，样本最大值不是连续时间真峰值。全部内部窗口没有 WAL 容量等待、可靠确认等待、请求拒绝、RPC 错误或重试；等待完成数为 0，等待均值为 null。这些平均值不能直接解释端到端 P99.9。

CPU 以同一进程的用户与系统 tick 增量，除以记录的 100 tick/s 和该进程自身 monotonic 区间；1 核是一个逻辑 CPU 的平均计算时间。I/O 也使用该进程测量内部的首末样本差值并检查计数不倒退，不借用状态窗口长度。Engine 窗口与存储 I/O 增量为：

| 条件 | Engine 自有窗口，s | Δ read_bytes，B | Δ write_bytes，B | Δ cancelled_write_bytes，B |
| --- | ---: | ---: | ---: | ---: |
| 20k off | 48.480207460 | 8,192 | 359,477,248 | 0 |
| 20k on | 47.881716640 | 8,192 | 578,793,472 | 0 |
| 100k off | 47.571481826 | 8,192 | 359,931,904 | 0 |
| 100k on | 50.000286402 | 32,768 | 1,344,163,840 | 0 |

这些是 `/proc/<pid>/io` 的**整个进程存储 I/O 计数**，不是 write 系统调用的 payload 总数。Engine 同时执行 WAL 写入、快照安装和 WAL 后缀回收；不能把 ON−OFF 的差额命名为“纯快照写放大”，不能由此推导设备真实写放大，也不擅自用 cancelled 值相减。Gateway 与 Benchmark 的 write / cancelled 增量均为 0；100k ON 的 Gateway read 增量为 61,440 B，其余 Gateway / Benchmark read 增量为 0。保留这些观测，不将一个字段的非零值直接归因为业务读盘。

## 为什么先尝试有界写出缓冲

`d2d5eb8` 的 `Engine::install_snapshot` 对文件头调用一次 `write_all`，随后对每条 `codec::record(...)` 再调用一次。100k 数据集每次快照对应 **100,001 次 `write_all` 调用**；短写或 EINTR 重试时，底层 write 系统调用可以更多。write 阶段在两个规模下都是三阶段中耗时最长的部分，因此合并小块写出是一个范围明确、可独立验证的候选改动。

下一步可以只在快照文件写出时使用固定容量缓冲，保留逐条编码、CRC、文件格式和 sync / rename / 目录同步顺序，不额外拼接整个快照。它主要尝试减少小块写出次数；不会取消捕获时的完整状态副本，也不会直接解决状态复制、WAL 后缀回收或所有尾延迟来源。需要先验证跨缓冲边界、短写和真实恢复，再冻结新旧 Engine 做同负载对照。**本页只发布缓冲前的 d2 pilot；功能测试通过或源码减少调用，都不等于已经证明性能提升。**

## 原始归档与搬移验证

[d2-size-pilot.tar.gz](../benmark/baselines/2026-09-14-snapshot-pilot/d2-size-pilot.tar.gz) 保留原始父目录中的 plan、两份 host-before，以及两个 stage 的全部 manifest、index、host-context、report、commands、result、状态 JSON/JSONL、资源和日志。只省略实际 `binaries/` 与生成的 `data/`，派生分析 JSON 位于原父目录之外。未丢弃任何 pilot 轮次。

归档共 **53 文件、4,477,658 原始字节、342,022 压缩字节**，SHA-256 为 `32c86433e8ce37029d943a5723a76b00f0faf66514f2b18815d86b12670f2a78`。完整信息见 [MANIFEST.json](../benmark/baselines/2026-09-14-snapshot-pilot/MANIFEST.json)；成员均为安全、唯一的普通文件，已逐成员核对原始字节与完整文件集合。

从仓库根目录解包到任意新位置，再将两个 stage 根目录传给汇总 CLI：

```sh
(cd benmark/baselines/2026-09-14-snapshot-pilot && sha256sum -c SHA256SUMS)
snapshot_pilot_extract_dir="$(mktemp -d)"
tar -xzf benmark/baselines/2026-09-14-snapshot-pilot/d2-size-pilot.tar.gz \
  -C "$snapshot_pilot_extract_dir"
python3 benmark/summarize.py \
  "$snapshot_pilot_extract_dir/d2-size-pilot/01-keys20000" \
  "$snapshot_pilot_extract_dir/d2-size-pilot/02-keys100000" --format json
```

实际搬移验证退出码为 0，**4 valid**，失败、缺失、中断、invalid、未完成、快照证据不足均为 0，逐轮汇总与原位置完全一致。两个 stage 各有一个预期的 `recorded_hashes_only` 提示：归档可核对已记录的哈希，不能重验被省略的实际执行文件。原绝对路径仅作来源信息，无需存在；父目录中的 pilot plan 也不是单份标准实验 manifest。

汇总 CLI 不自动输出阶段均值、P99.9 或 I/O 分解。它们可分别由原始 `report.json`、上述共同窗口公式与 `resources.jsonl` 复算，不依赖归档外的临时分析脚本。

## 复现相同条件

先在明确版本构建并冻结文件，全部构建完成后再测量；两个阶段使用同一组副本。以下使用 `7d29382` 工作树中的实验程序，其实验 / 汇总源码与本次采集工作区 `aac9c1b` 相同。

```sh
snapshot_pilot_repo="$PWD"
snapshot_pilot_output_root="/tmp/minikv-snapshot-pilot"
snapshot_pilot_frozen_dir="/tmp/minikv-snapshot-pilot-binaries"
git worktree add --detach /tmp/minikv-snapshot-go7d 7d29382
git worktree add --detach /tmp/minikv-snapshot-engined2 d2d5eb8
make -C /tmp/minikv-snapshot-go7d go
make -C /tmp/minikv-snapshot-engined2 engine JOBS=4
mkdir "$snapshot_pilot_frozen_dir" "$snapshot_pilot_output_root"
cp /tmp/minikv-snapshot-engined2/build/engine "$snapshot_pilot_frozen_dir/engine"
cp /tmp/minikv-snapshot-go7d/bin/minikv-go "$snapshot_pilot_frozen_dir/gateway"
cp /tmp/minikv-snapshot-go7d/bin/minikv-bench "$snapshot_pilot_frozen_dir/bench"
chmod 0555 "$snapshot_pilot_frozen_dir"/*
sha256sum "$snapshot_pilot_frozen_dir"/* > "$snapshot_pilot_output_root/binaries.sha256"
printf '%s\n' '01-keys20000 OFF/5000ms; 02-keys100000 OFF/5000ms' \
  'Each measurement >=30s; ON internal completed snapshots >=3; keep all pilot runs.' \
  > "$snapshot_pilot_output_root/coverage-plan.txt"

for snapshot_pilot_case in 01-keys20000 02-keys100000; do
  snapshot_pilot_keys="${snapshot_pilot_case#*-keys}"
  if python3 /tmp/minikv-snapshot-go7d/benmark/experiment.py \
    --output "$snapshot_pilot_output_root/$snapshot_pilot_case" \
    --engine "$snapshot_pilot_frozen_dir/engine" \
    --gateway "$snapshot_pilot_frozen_dir/gateway" \
    --bench "$snapshot_pilot_frozen_dir/bench" \
    --modes throughput --repeats 1 --requests 1500000 --workers 40 \
    --keyspace "$snapshot_pilot_keys" --op mixed --write-ratio 20 --delete-ratio 0 \
    --value-size 1024 --seed 1 --engine-workers 20 --rpc-pool 64 --gomaxprocs 4 \
    --wal-batch 64 --wal-flush-ms 2 --snapshot-ms 5000 \
    --sample-ms 100 --stats-ms 250 --run-timeout 900 \
    --startup-timeout 10 --shutdown-timeout 60 --settle-timeout 60; then
    :
  else
    printf 'Pilot %s failed or was interrupted; retain artifacts and stop.\n' \
      "$snapshot_pilot_case" >&2
    break
  fi
done

set --
for snapshot_pilot_case in 01-keys20000 02-keys100000; do
  if [ -d "$snapshot_pilot_output_root/$snapshot_pilot_case" ]; then
    set -- "$@" "$snapshot_pilot_output_root/$snapshot_pilot_case"
  else
    printf 'Planned stage %s was not executed.\n' "$snapshot_pilot_case" >&2
  fi
done
if [ "$#" -gt 0 ]; then
  python3 "$snapshot_pilot_repo/benmark/summarize.py" "$@" --format json
else
  printf 'No stage artifacts exist; the pilot plan is incomplete.\n' >&2
fi
```

工作树、冻结目录和输出父目录必须尚不存在；输出所在文件系统就是数据盘，可改到待评估盘的新路径并记录主机背景。原归档的 JSON plan 与 host-before / host-context 是手工补充采集，以上实验程序不会自动生成。覆盖门槛也需按测量时长与内部完成计数另行核对，不能仅凭汇总 CLI 的“存在快照活动”判断已经完成三次。

实验程序失败或中断都可能返回 1；脚本遇到非零退出后保留产物并停止，不启动下一阶段。随后只汇总已创建的阶段目录，同时对照预声明顺序显示未执行阶段。汇总 CLI 的计数仅涵盖传入目录，即使部分汇总退出 0，也不能称整个四轮 pilot 已完整完成；需要另外核对两阶段均存在、四轮状态及覆盖门槛。

不同构建环境不保证生成相同哈希；不同主机状态也不保证复现相同性能。后续正式对照应保留新旧版本、重复次数与失败规则的预声明，当前四轮继续作为原始 pilot 保存。
