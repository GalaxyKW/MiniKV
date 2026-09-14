# 控制实验：throughput 的 Engine 交错对照

[返回 README](../README.md) · [此前整组版本对照](performance-throughput-controls.md) · [实验程序](benchmark-experiments.md) · [指标口径](observability.md)

`d2d5eb8` 精简了 throughput 请求路径：服务器直接调用同步执行接口投递即时结果，跳过可靠确认回调的构造；引擎也不再启动该模式不使用的可靠确认完成线程。请求名额、完成队列和 WAL 确认语义保留。本次固定 Gateway 与 Benchmark 的实际文件，只交错更换 Engine，检查这项改动在相同负载下的表现。

**16 轮均有效，但相邻配对没有显示稳定的一致收益。** 关闭快照时四对 QPS 比值的中位数接近 1，单对方向不同；开启快照时差异较小，仍有反向结果。这些观测不能证明零成本、统计显著提升，或已经解释和修复[此前 25→7d 整组版本对照](performance-throughput-controls.md)中的波动。

## 预声明顺序与固定条件

A 为 Engine `7d29382`，B 为 Engine `d2d5eb8`。测量前冻结 `plan.json`，顺序为 **A B B A B A A B**；每阶段运行 throughput × 自动快照关闭 / 1,000 ms，各一次，共 8 阶段、16 轮。相邻阶段按 `(1,2)`、`(3,4)`、`(5,6)`、`(7,8)` 配对，分别在快照关闭和开启条件下计算 **B/A**，不因某一对先跑 B 而倒算 A/B。没有删除慢轮次，也没有失败后追加替代配对。

各轮使用新进程、新数据目录，顺序预置 5,000 个 key 后，测量 500,000 请求。40 个客户端闭环 worker，keyspace 5,000，mixed 20% PUT / 5% DELETE / 其余 GET，value 128 字节、seed 1，请求超时 2 s。预置不计入 QPS 与延迟，没有另加稳态预热或清空系统缓存。

Engine 数据线程 20、请求队列容量 128、连接上限 256；WAL batch 64、flush 2 ms、未同步字节限额 16 MiB。RPC 池 64，两个 Go 进程均 GOMAXPROCS=4、GOGC=100、GOMEMLIMIT=off。资源每 100 ms、状态每 250 ms 采样；压测进程预算 600 s（含预置与报告生成），启动 10 s、关闭 15 s、WAL 排空 10 s。除 Engine 文件、路径、临时端口与分层比较的快照开关外，运行参数和显式运行环境一致。

主机为 Xeon E3-1270 v3、8 个逻辑 CPU、Linux 6.8，数据位于 `/dev/nvme0n1p2` 的 NTFS3 文件系统，构建背景为 RelWithDebInfo。每阶段开始前采集主机背景，1 分钟 load 按阶段为 7.55、9.67、11.32、11.12、11.46、12.19、12.42、11.18。测量期间没有并行的项目构建、测试或其他矩阵，但共享主机的其他负载、CPU 频率、缓存和阶段间隔没有隔离。

ABBABAAB 使四个相邻配对含两对 AB、两对 BA；这有助于保留不同先后顺序下的证据，**不等于随机化或消除了时间漂移**。各阶段始终先 off 再 on，也没有平衡快照执行顺序。配对中的两个同条件轮次之间仍隔着一次另一快照条件的测量。

## 实际执行文件

每阶段从冻结来源复制三份执行文件后启动，测量结束后重新计算全部 24 份副本的 SHA-256，并核对它们与 16 轮命令的关系：

| 文件 | 运行来源 | SHA-256 |
| --- | --- | --- |
| A Engine | `7d293825a6bc6963ef9856b63cd08f932ee91f76` | `5bf67c7eb3ddf2ce0c665c269d488fd2a811e53bb2ec284778261dcc68c390e8` |
| B Engine | `d2d5eb862ee1cf11fcbeb68c04c66d740613338e` | `efabc96b11ae376fa020b052215f57af286729d7c5b76722b9ddaba6d9f022a0` |
| 固定 Gateway | `7d29382`，两组使用相同文件 | `6fed313c6f924c21871cc05cf5589a5a6189cde1663492d682ec5b5ea3981882` |
| 固定 Benchmark | `7d29382`，两组使用相同文件 | `2ef55eb18244b0e0e8b4b9d94318300c6090b59dda7423df529ac370f6bca4fc` |

Gateway、Benchmark 的 Go 源码及模块文件在 `7d29382 → d2d5eb8` 间没有变化；本次直接固定了相同二进制字节，不依靠重新构建后的等价假设。全部客户端报告的 `client_build.vcs_revision` 都是 `7d29382`，包括运行 B Engine 的轮次。

实际采集程序工作区 HEAD 为 `3f7eb97`，8 份 manifest 均记录 clean。它表示采集代码背景，不能代替 A/B Engine 的版本。预声明 plan 的 SHA-256 为 `96ff0713d7425b41146c2c159af6006811493667f73a8d2e586ecdb556fd1317`，归档保留其原始字节。此次比较只覆盖上述冻结文件，既不代表后续提交，也不等同于先前 Gateway 与 Benchmark 文件也随版本变化的 25→7d 对照。

## 全部轮次与相邻配对

以下保留执行顺序中的每轮成功 QPS 与 P99。成功请求包含正常未命中；各轮 P99 来自该轮的 500,000 个延迟样本。

| 阶段 | Engine | off QPS | off P99，ms | on QPS | on P99，ms |
| --- | --- | ---: | ---: | ---: | ---: |
| 01 | A | 33,118.717 | 2.337194 | 32,702.923 | 2.326490 |
| 02 | B | 31,707.517 | 2.689060 | 32,931.360 | 2.341860 |
| 03 | B | 33,139.333 | 2.320524 | 33,096.871 | 2.313709 |
| 04 | A | 31,369.190 | 2.746350 | 33,303.111 | 2.290882 |
| 05 | B | 32,367.987 | 2.443910 | 32,639.722 | 2.415644 |
| 06 | A | 32,911.460 | 2.368396 | 32,362.838 | 2.459461 |
| 07 | A | 32,799.831 | 2.330481 | 32,944.289 | 2.351973 |
| 08 | B | 33,093.619 | 2.304245 | 33,200.906 | 2.307762 |

每个快照条件下各版本有四轮，下表为**中位数 [最小值，最大值]**。P99 的中位数只是四个轮次值的汇总，不是合并请求后的总体 P99。

| 自动快照 | Engine | 成功 QPS | 每轮 P99，ms |
| --- | --- | ---: | ---: |
| 关闭 | A：7d29382 | 32,856 [31,369，33,119] | 2.353 [2.330，2.746] |
| 关闭 | B：d2d5eb8 | 32,731 [31,708，33,139] | 2.382 [2.304，2.689] |
| 1,000 ms | A：7d29382 | 32,824 [32,363，33,303] | 2.339 [2.291，2.459] |
| 1,000 ms | B：d2d5eb8 | 33,014 [32,640，33,201] | 2.328 [2.308，2.416] |

逐对先取对应轮次的 B/A，再汇总四个比值；不使用两组中位数之比替代配对统计。QPS 比值大于 1 表示该对 B 较高，P99 比值小于 1 表示该对 B 较低。

| 配对及分子 / 分母 | off QPS B/A | off P99 B/A | on QPS B/A | on P99 B/A |
| --- | ---: | ---: | ---: | ---: |
| 1：02-B / 01-A | 0.957390 | 1.150551 | 1.006985 | 1.006607 |
| 2：03-B / 04-A | 1.056429 | 0.844948 | 0.993807 | 1.009964 |
| 3：05-B / 06-A | 0.983487 | 1.031884 | 1.008556 | 0.982184 |
| 4：08-B / 07-A | 1.008957 | 0.988742 | 1.007789 | 0.981203 |
| 四对中位数 | **0.996222** | **1.010313** | **1.007387** | **0.994395** |
| 四对范围 | 0.957390–1.056429 | 0.844948–1.150551 | 0.993807–1.008556 | 0.981203–1.009964 |

关闭快照时，B 的 QPS 与 P99 各有两对较好、两对较差；开启快照时，QPS 三对较高、一对较低，P99 仍各有两对较好、两对较差。较慢的 off 轮次同时出现在 `02-B` 与 `04-A`。这些结果没有识别出稳定的一致提升，也不能单独把某轮差异归因于代码或主机负载。

## 正确性、测量窗口与 WAL

16 轮均完成 500,000 测量请求，失败、缺失、中断、invalid、未完成和快照证据不足轮次均为 0。每轮操作数完全一致：PUT **100,217**、DELETE **24,755**、GET **375,028**。包括 5,000 次预置写入，最终已应用和已持久化序列均为 **129,972**；`stats-after.json` 与 `stats-settled.json` 均记录 pending 为 0，48 个子进程全部正常退出。DELETE 未命中也产生 WAL 记录，逻辑未命中不计系统失败。

throughput 在内存更新、WAL 入队后即可确认，客户端 QPS 不等于已持久化写入吞吐。测量区间由 `measurement_started_at + elapsed_ns` 定义，预置、报告生成和随后排空不计入。after 查询发生在客户端退出之后，不能由 after 已排空反推最后一个响应返回时就已经持久化。本轮 `wal_drain_elapsed_ns` 为 671–1,097 ns，仅表示检查几乎没有进入等待循环，不能把它当作“最后确认→持久化”的耗时，或把它加到客户端计时中比较版本。

状态统计只选取查询起止时间都在客户端测量区间内的样本，每轮 58–61 个。通过墙钟时间戳近似对齐；累计计数检查全段不倒退后取末值减首值，gauge 使用未加权采样算术均值。样本最大值不是连续时间的真实峰值。内部 WAL pending 的逐轮样本最大值为 8,522–12,648 字节，没有容量等待。

8 轮开启快照都有内部活动证据，首尾样本间均完成 14 次快照，关闭组增量均为 0。预置阶段发生的快照不充当测量期证据。相同完成次数仍不等于相同快照数据量或 I/O 耗时，也不意味着 off/on 顺序已得到控制。

## 排队、CPU 与内存

以下先逐轮计算，再取各版本 / 快照条件的四轮中位数。

| 指标 | A off | B off | A on | B on |
| --- | ---: | ---: | ---: | ---: |
| 已出队请求平均排队时间，ms | 0.05675 | 0.05681 | 0.05649 | 0.05766 |
| 请求队列深度的样本均值 | 1.391 | 0.982 | 0.923 | 1.284 |
| 活动数据线程的样本均值，容量 20 | 0.992 | 1.142 | 1.043 | 1.233 |
| 整请求在途名额的样本均值，容量 148 | 13.911 | 14.416 | 13.949 | 13.733 |
| Engine CPU，核 | 1.215 | 1.203 | 1.244 | 1.240 |
| Gateway CPU，核 | 2.684 | 2.676 | 2.698 | 2.707 |
| Benchmark CPU，核 | 1.944 | 1.931 | 1.956 | 1.955 |
| 已结束 WAL 提交平均耗时，ms | 2.614 | 2.611 | 2.624 | 2.632 |
| 提交周期近似值，ms | 4.765 | 4.762 | 4.783 | 4.777 |
| 每次提交对应的记录近似值 | 39.063 | 38.974 | 39.421 | 39.611 |

所有内部窗口的 WAL 容量等待、可靠确认等待完成次数和累计耗时增量、等待者样本最大值均为 0；等待均值分母为 0，保留 null，不说成“完成了一次耗时为零的等待”。两版本的 Engine async inflight 样本均为 0、回调失败为 0；server 整请求在途名额仍被使用，逐轮样本均值为 12.638–16.483，样本最大值为 29–40。没有请求拒绝、RPC 错误或重试；40 个闭环客户端也未覆盖名额耗尽时的性能。

队列均值为 `Δ request_queue_wait_duration_ns_total / Δ requests_started_total`；WAL 等待均值按已结束等待 episode 计算。提交平均耗时为 `Δ wal_commit_duration_ns_total / Δ wal_commits_total`，本次提交失败为 0。提交周期近似值为内部首末查询结束时间之差除以提交次数，记录近似值为 `Δ applied_sequence / Δ wal_commits_total`。窗口内结束的工作可能在窗口前开始，后者也不是逐批精确记录数。不同计数器的时间不能相加为端到端延迟，平均排队值更不能解释成端到端 P99。

CPU 使用每个进程自身的 monotonic 采样区间，校验同一 PID / starttime 后，以用户与系统 tick 增量除以 100 tick/s 和区间秒数。1 核表示平均消耗一个逻辑 CPU 的计算时间，不表示其他 CPU 空闲。CPU 窗口与状态窗口不同，不用一个区间替代另一个；当前采样也没有给出每请求分配或调度成本的独立归因。

内存每格为“测量区间 RSS 样本最大值 / 已观察 HWM”的四轮中位数，单位 MiB：

| 进程 | A off | B off | A on | B on |
| --- | ---: | ---: | ---: | ---: |
| Engine | 6.22 / 6.22 | 6.24 / 6.25 | 7.54 / 7.54 | 7.52 / 7.53 |
| Gateway | 14.21 / 14.21 | 14.28 / 14.28 | 14.22 / 14.22 | 14.18 / 14.20 |
| Benchmark | 19.61 / 19.61 | 19.58 / 19.58 | 19.68 / 19.68 | 19.59 / 19.59 |

RSS 离散采样可能漏掉瞬时峰值；HWM 自进程启动累计，可能包含预置或测量之后的阶段，最后一个样本也不保证覆盖整个进程生命周期。资源流校验同一 PID / starttime。`07-A` off 的 Benchmark 在测量结束后有一条 RSS/HWM 不可用的样本，汇总保留 warning 与此前确认的内存观测，不把缺值变成 0，也不因此删除该轮。

全部 Benchmark 的测量区间 RSS 样本最大值为 19.17–20.03 MiB。压测器保留 500,000 个精确延迟样本，内存随请求数增长；不能与 100,000 请求实验直接比较，并称作版本的内存回退。这些细小的同请求数差异也不足以证明内存优化。

## 完整归档与搬移复查

[engine7d-d2.tar.gz](../benmark/baselines/2026-09-14-throughput-interleaved/engine7d-d2.tar.gz) 保留整个实验父目录：冻结 `plan.json`、8 份原始 `*-host-before.json` 和 8 个 stage。每个 stage 保留 manifest、index、host-context、两轮 report / commands / result、before / after / settled 状态、原始状态和资源 JSONL、进程日志。只省略实际 `binaries/` 副本和生成的 `data/`，没有裁剪失败或慢轮次；派生 summary / analysis 位于原实验父目录外，不混入原始归档。

归档共 **209 个文件、5,096,413 原始字节、462,773 压缩字节**，SHA-256 为 `8d504b8822bb87c0c89ef8ceeac7bae9393717a2fec3bf59c0c8226ad6cc087c`。文件数、版本、配对与计划哈希见 [MANIFEST.json](../benmark/baselines/2026-09-14-throughput-interleaved/MANIFEST.json)。所有成员均为路径安全、无重复的普通文件，已逐成员与原始文件核对字节及完整文件集合。

从仓库根目录验证、解包到新位置后，传入八个 stage 根目录；父目录是本次交错安排，不能直接作为单份标准实验 manifest 交给汇总 CLI：

```sh
(cd benmark/baselines/2026-09-14-throughput-interleaved && sha256sum -c SHA256SUMS)
interleaved_extract_dir="$(mktemp -d)"
tar -xzf benmark/baselines/2026-09-14-throughput-interleaved/engine7d-d2.tar.gz \
  -C "$interleaved_extract_dir"
python3 benmark/summarize.py \
  "$interleaved_extract_dir/engine7d-d2/01-A" \
  "$interleaved_extract_dir/engine7d-d2/02-B" \
  "$interleaved_extract_dir/engine7d-d2/03-B" \
  "$interleaved_extract_dir/engine7d-d2/04-A" \
  "$interleaved_extract_dir/engine7d-d2/05-B" \
  "$interleaved_extract_dir/engine7d-d2/06-A" \
  "$interleaved_extract_dir/engine7d-d2/07-A" \
  "$interleaved_extract_dir/engine7d-d2/08-B" --format json
```

搬移复查退出码为 0，16 valid，其余失败 / 缺失 / 中断 / invalid / 未完成 / 快照证据不足计数均为 0。八个目录分别统计，CLI 不自动跨目录按 A/B 合并或计算相邻配对；上面的配对表按冻结 plan 和原始 report 复算。全部 per-run 值与原位置汇总一致，包括 `07-A` 的正常资源 warning。

由于归档省略二进制，每个 stage 显示 `recorded_hashes_only` 提示；可以核对记录的哈希，不能从归档重验被省略的实际文件。原绝对路径只是来源信息，无需在新主机存在，也不能照原路径寻找文件。等待和 CPU 按上文公式从原始 JSONL 复算；仓库汇总 CLI 不直接输出这些细分均值。

## 复现版本安排与负载

先构建两个明确版本，再冻结文件，全部构建结束后才启动测量。Gateway 和 Benchmark 始终使用同一批 `7d29382` 文件，B 只构建 Engine。以下使用 A 工作树的实验程序，其实验 / 汇总源码与本次采集工作树 `3f7eb97` 相同；运行 B 时客户端构建信息仍应显示 A 的版本。

```sh
interleaved_control_repo="$PWD"
interleaved_output_root="/tmp/minikv-throughput-interleaved"
interleaved_frozen_dir="/tmp/minikv-throughput-interleaved-binaries"
git worktree add --detach /tmp/minikv-throughput-engine7d 7d29382
git worktree add --detach /tmp/minikv-throughput-engined2 d2d5eb8
make -C /tmp/minikv-throughput-engine7d JOBS=4
make -C /tmp/minikv-throughput-engined2 engine JOBS=4
mkdir "$interleaved_frozen_dir" "$interleaved_output_root"
cp /tmp/minikv-throughput-engine7d/build/engine "$interleaved_frozen_dir/engine-A"
cp /tmp/minikv-throughput-engined2/build/engine "$interleaved_frozen_dir/engine-B"
cp /tmp/minikv-throughput-engine7d/bin/minikv-go "$interleaved_frozen_dir/gateway"
cp /tmp/minikv-throughput-engine7d/bin/minikv-bench "$interleaved_frozen_dir/bench"
chmod 0555 "$interleaved_frozen_dir"/*
sha256sum "$interleaved_frozen_dir"/* > "$interleaved_output_root/binaries.sha256"
printf '%s\n' 01-A 02-B 03-B 04-A 05-B 06-A 07-A 08-B \
  > "$interleaved_output_root/stage-order.txt"

for interleaved_stage in 01-A 02-B 03-B 04-A 05-B 06-A 07-A 08-B; do
  interleaved_version="${interleaved_stage#*-}"
  if python3 /tmp/minikv-throughput-engine7d/benmark/experiment.py \
    --output "$interleaved_output_root/$interleaved_stage" \
    --engine "$interleaved_frozen_dir/engine-$interleaved_version" \
    --gateway "$interleaved_frozen_dir/gateway" \
    --bench "$interleaved_frozen_dir/bench" \
    --modes throughput --repeats 1 --requests 500000 --workers 40 --keyspace 5000 \
    --op mixed --write-ratio 20 --delete-ratio 5 --value-size 128 --seed 1 \
    --engine-workers 20 --rpc-pool 64 --gomaxprocs 4 \
    --wal-batch 64 --wal-flush-ms 2 --snapshot-ms 1000 \
    --sample-ms 100 --stats-ms 250 --run-timeout 600 \
    --startup-timeout 10 --shutdown-timeout 15 --settle-timeout 10; then
    :
  else
    printf 'Stage %s failed; stop and keep the complete planned order.\n' \
      "$interleaved_stage" >&2
    break
  fi
done

python3 "$interleaved_control_repo/benmark/summarize.py" \
  "$interleaved_output_root"/[0-9][0-9]-[AB] --format json
```

工作树、冻结目录和输出父目录都必须尚不存在。输出目录所在文件系统就是数据盘，可以改到待测盘的新路径；记录编译器、构建类型和主机背景。相同源码不保证不同构建环境得到相同文件哈希，重跑也不保证相同性能。

这段命令预先保存顺序和实际文件哈希，复现版本安排与负载；本次归档的 `plan.json` 与各阶段 host-before / host-context 是额外手工采集，实验程序不会自动生成它们。命令遇到失败或中断会停止后续阶段。最后的汇总只读取已创建的 stage；必须再按预先保存的顺序标记尚未运行的阶段，不能把部分目录的有效计数当作完整 16 轮完成。某对缺一轮就标记不可用，不从其他阶段挑一个结果补齐。

这次路径精简有明确代码依据，但性能证据只支持当前条件下的观测。后续若要判断较小差异，应增加更受控的重复测量，并用独立 CPU / 分配 / 调度观测定位；本轮没有覆盖过载、慢盘、大数据集或长期稳定性，也不提供容量承诺。
