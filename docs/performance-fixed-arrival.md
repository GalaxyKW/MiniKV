# 固定到达率实验：请求在哪一层被限制

[返回 README](../README.md) · [负载模型与报告](benchmark-report.md) · [实验工具](benchmark-experiments.md) · [异步确认设计](design.md#异步可靠确认与请求生命周期)

在每秒计划到达 500 次 PUT 的条件下，本次实验分别观察到了客户端名额不足导致的发送前丢弃，以及后端可靠确认占用请求名额导致的 HTTP 503。后者的测量内部样本中，数据 worker 和执行队列深度都为 0，但在途请求达到 132 的上限。它说明确认等待移出 worker 后，仍需保留覆盖请求完整生命周期的容量约束。

本文保留全部 18 轮正式测量和 6 轮短试跑。这里改变的是 WAL 刷新策略；没有模拟慢盘，也没有寻找系统最大吞吐量。

## 条件、顺序与预先声明的检查

运行版本为 `286ed8e500175db6ecc5892d077c124be81bd548`。所有执行文件在测量前构建，实验期间没有并行的项目构建或测试。每个阶段使用冻结副本；12 个阶段的源码记录均为同一干净提交，36 份执行文件副本逐一与计划中的 SHA-256 对账。完整身份见[归档清单](../benmark/baselines/2026-09-22-fixed-arrival/MANIFEST.json)。

| 条件 | 客户端 worker | WAL flush | 要检查的行为 |
| --- | ---: | ---: | --- |
| A：低等待参照 | 256 | 2 ms | 没有客户端名额丢弃、后端拒绝和请求失败 |
| B：客户端名额受限 | 20 | 1,000 ms | 客户端丢弃增加，实际尝试仍成功，后端不拒绝 |
| C：后端名额受限 | 256 | 1,000 ms | 后端拒绝增加，客户端名额不先耗尽 |

A/C 只改变刷新间隔；B/C 只改变客户端 worker 数。A/B 同时改变两个条件，不用于单因素归因。三组均使用 reliable、纯 PUT、500 req/s、每轮 5,000 个计划请求，即 10 秒计划窗口。key 空间 1,000、value 128 字节、seed 1；每轮使用新进程和空数据目录，PUT 模式不预置数据，也没有额外稳态预热。

其余配置固定：4 个引擎数据线程、128 个执行队列名额、132 个 server/engine 在途名额、RPC 池 256、连接上限 258；WAL batch 65,536、字节限额 16 MiB。三个条件使用相同的大 batch，避免 A/C 同时改变批量阈值。两个 Go 进程均 GOMAXPROCS=4；HTTP/RPC 超时 2 秒，资源采样 100 ms、状态采样 250 ms。

每次调用实验程序都运行快照关闭 / 1,000 ms 两种配置。正式阶段顺序为 `A1 B1 C1 B2 C2 A2 C3 A3 B3`，每条件各三次、每阶段两轮，共 18 轮。外层轮换顺序，阶段内部始终先 off 后 on，未随机化。先前的短试跑依次为 A/B/C、各 1,000 个计划请求；试跑与正式测量分开保存，没有替换不利结果。

计划在试跑前写入归档中的 `PLAN.json`，哈希保存在 `PLAN.sha256`，不是外部注册的预注册实验。预先声明的检查包括计数守恒、HTTP 503 的来源对账、容量与健康状态、完整排空，以及每轮 `late / planned ≤ 1%` 的客户端调度质量门槛。超过门槛仍保留结果，但不能声称预期限制机制已被充分区分；任何非零丢弃都继续标为 `degraded`。

主机为 Xeon E3-1270 v3、8 个逻辑 CPU、Linux 6.8.0-139-generic、Go 1.22.0，数据位于 `/dev/nvme0n1p2` 的 NTFS3。客户端与服务端共机；未绑定 CPU、固定频率或清空缓存，也未控制共享主机的其他负载。归档保留逐阶段主机/构建信息及测量期间采集的文件系统背景。

## 完整到达与损失

下表为三轮的**中位数 [最小值，最大值]**；相同值省略区间。每轮分母均为 5,000 个计划到达。

| 条件 / 快照 | 成功请求 | 客户端 busy 丢弃 | late 丢弃 | HTTP 503 | 到达成功率 |
| --- | ---: | ---: | ---: | ---: | ---: |
| A / off | 5,000 [4,999，5,000] | 0 | 0 [0，1] | 0 | 100% [99.98，100] |
| A / on | 5,000 | 0 | 0 | 0 | 100% |
| B / off | 200 | 4,800 | 0 | 0 | 4% |
| B / on | 318 [298，318] | 4,682 [4,682，4,702] | 0 | 0 | 6.36% [5.96，6.36] |
| C / off | 1,320 | 0 | 2 [0，9] | 3,678 [3,671，3,680] | 26.4% |
| C / on | 1,469 [1,465，1,474] | 0 | 1 [1，4] | 3,530 [3,525，3,531] | 29.38% [29.30，29.48] |

正式测量共有 5 轮 `ok`、13 轮 `degraded`，没有失败、不完整或无效轮次。A3/off 的一次 late 丢弃也保留为 `degraded`。全体计数为：90,000 个计划到达 = 61,516 次实际尝试 + 28,466 次 busy 丢弃 + 18 次 late 丢弃；实际尝试 = 39,901 次成功 + 21,615 次 HTTP 503。各轮最大 late 比例为 9/5,000 = 0.18%，均低于声明门槛。

B/off 的实际请求成功率为 100%，但全部计划到达的成功率只有 4%。只看实际请求的成功率会漏掉 96% 的发送前丢弃。C/off 没有 busy 丢弃，但约 74% 的计划到达得到 503；这些失败没有从吞吐、延迟或汇总中消失。

## 延迟与名额占用

成功 QPS 的分母包括完整计划窗口和末尾排空。三组延迟只包含实际尝试，且包含快速返回的 503；丢弃没有虚构的延迟样本。下表继续使用三轮中位数 [最小值，最大值]，每轮 P99 的中位数不是合并请求后的 P99。

| 条件 / 快照 | 成功 QPS | 服务 P99，ms | 计划到完成 P99，ms |
| --- | ---: | ---: | ---: |
| A / off | 499.849 [499.714，499.895] | 7.934 [7.658，7.990] | 8.574 [8.265，8.610] |
| A / on | 499.810 [499.803，499.891] | 8.055 [7.968，8.241] | 8.707 [8.677，8.858] |
| B / off | 19.989 [19.987，19.989] | 1,002.834 [1,002.063，1,002.940] | 1,002.973 [1,002.677，1,003.258] |
| B / on | 31.779 [29.749，31.783] | 997.935 [996.467，1,010.958] | 998.439 [997.473，1,011.036] |
| C / off | 131.834 [131.755，131.900] | 993.551 [993.146，993.968] | 994.100 [993.548，994.736] |
| C / on | 146.695 [146.309，147.233] | 971.662 [970.106，972.872] | 972.246 [970.867，973.404] |

正式测量实际耗时为完整 10 秒窗口再加约 2.109–18.626 ms。短试跑中 B/C 的 2 秒计划窗口实际约耗时 2.98 秒，末尾排空占比不同，不能混入正式测量。

状态分析仅采用查询起止均在测量区间内的样本，按墙钟近似对齐。18 轮共有 765 次成功状态查询、0 次失败，其中 692 次完全位于测量区间。先按每轮计算未加权的样本均值 / 最大值，再对三轮取中位数 [范围]：

| 条件 / 快照 | Server 在途样本均值 | Server 在途样本最大值 |
| --- | ---: | ---: |
| A / off | 2.385 [2.359，2.538] | 4 |
| A / on | 2.308 [2.282，2.308] | 4 [3，4] |
| B / off | 20 [19.5，20] | 20 |
| B / on | 19.605 [19.579，20] | 20 |
| C / off | 114.763 [114.308，115.447] | 132 |
| C / on | 111.816 [111.231，112.5] | 132 |

B/C 的 engine async 在途与可靠确认等待者统计同上。这些组的内部样本中，活动数据 worker 和执行队列深度均为 0；观测到的是**确认等待保留请求名额**，不能解释为数据线程饱和或执行队列已排满。

全部成功查询及 before/after/settled 状态中，两层在途计数均未超过 132。RPC 在用样本最大值为 133，低于其独立容量 256；不同组件的生命周期和取样时刻不同，不能要求它与后端在途数逐项相等。WAL 容量等待者均为 0，待同步 WAL 样本最大值为 20,713 字节，远低于 16 MiB 上限。

## 拒绝来源、持久化与内存

纯 PUT 不预置数据，不重试写入，因此每轮都可以核对下列实际等式。分析分别检查 after 和 settled 相对 before 的增量：

```text
HTTP 503 = HTTP failures = Δ server.requests_rejected_total
successes = Δ server.requests_started_total = Δ applied_sequence
started = Δ gateway.rpc.calls_total
settled applied_sequence = settled durable_sequence
```

这 24 轮均满足等式。所有必要状态及成功查询中，存储失败、WAL 提交失败、快照失败、异步回调失败、RPC 传输错误、重试和连接拒绝均为 0；客户端没有超时、网络或协议失败。最后两次静止采样的顺序与最终 settled 内容一致，在途请求与待同步 WAL 都归零。这些证据共同支持本次 503 来自 server 请求名额拒绝；503 本身也可能由其他后端错误产生，不能单独作这种归因。

测量期 RSS 样本最大值如下，单位 MiB，仍为每组三轮中位数 [范围]：

| 条件 / 快照 | Engine | Gateway | Benchmark |
| --- | ---: | ---: | ---: |
| A / off | 4.410 [4.406，4.414] | 13.113 [12.715，13.141] | 15.121 [15.063，15.379] |
| A / on | 4.797 [4.773，4.797] | 12.926 [12.750，12.941] | 15.258 [15.148，15.344] |
| B / off | 4.219 [4.219，4.223] | 10.070 [10.063，10.105] | 8.953 [8.910，8.973] |
| B / on | 4.387 [4.383，4.406] | 11.027 [10.941，11.168] | 9.625 [9.559，9.672] |
| C / off | 4.461 [4.457，4.473] | 16.207 [16.195，16.465] | 18.156 [18.035，18.641] |
| C / on | 4.754 [4.730，4.758] | 16.027 [15.891，16.063] | 18.000 [17.973，18.309] |

正式测量中，各进程记录到的生命周期 HWM 与对应 RSS 样本最大值相同，但两个指标口径仍不同。离散样本会漏掉瞬时峰值，10 秒窗口也不能证明无内存泄漏。实际成功写入的子集不同：A 最终 997 个 key，B/off 176–177、B/on 253–269，C/off 724–725、C/on 756–759。客户端 worker、连接使用量和实际数据集均影响内存，因此上述差异只作描述，不作为每请求内存成本的单因素结论。

九轮开启快照的正式测量均观察到内部窗口完成 9 次快照。快照流程也会推进持久化；在长确认等待下，on 组更多请求得到确认，不能据此宣称快照降低系统成本。各组实际数据集和快照写入量不同，且过载轮次被通用快照成本报告明确排除。

## 归档复查与重新运行

[归档](../benmark/baselines/2026-09-22-fixed-arrival/fixed-arrival.tar.gz)含 310 个文件：全部试跑与正式阶段的报告、配置、状态、资源、退出记录、日志，以及计划、原执行脚本、主机背景和离线计算结果；省略实际执行文件和生成的数据目录。打包前逐一校验原二进制副本；省略后离线验证显示 `recorded_hashes_only`，不声称能在归档中重验已省略文件。

在仓库根目录执行：

```sh
(cd benmark/baselines/2026-09-22-fixed-arrival && sha256sum -c SHA256SUMS)
arrival_extract_dir="$(mktemp -d)"
tar -xzf benmark/baselines/2026-09-22-fixed-arrival/fixed-arrival.tar.gz -C "$arrival_extract_dir"
python3 benmark/baselines/2026-09-22-fixed-arrival/analyze.py \
  "$arrival_extract_dir/fixed-arrival" > /tmp/minikv-arrival-formal.json
python3 benmark/baselines/2026-09-22-fixed-arrival/analyze.py \
  "$arrival_extract_dir/fixed-arrival" --phase pilot > /tmp/minikv-arrival-pilot.json
```

该[专用分析脚本](../benmark/baselines/2026-09-22-fixed-arrival/analyze.py)复用通用汇总工具的身份、配置、报告、WAL、退出和收尾证据校验，再核对本实验的计数与容量。输出保留所有逐轮值和检查；产物或不变量损坏时退出 1，坏数据不进入分组数值。预期正式 / 试跑分别为 18 / 6 个完整测量，`analysis_pass` 与 `supported_isolation` 均为 true。它们表示记录和本实验归因检查通过，**不表示过载请求全部成功**；不符合预期的机制或调度质量会使 `supported_isolation=false`。通用 `summarize.py` 仍会对包含 `degraded` 的阶段退出 1。

保存的 `FORMAL_ANALYSIS.json` / `PILOT_ANALYSIS.json` 可与重新计算结果对比。计划、脚本、归档哈希及完整二进制哈希见 `MANIFEST.json`；各轮状态仍以原始记录为准。重定向会覆盖同名输出文件，重复复查时可选择不同路径。

要重新测量同一版本，先在新工作树构建，再串行运行以下正式顺序。输出目录应位于待评估数据盘，并为新的空路径；这里不重新混入短试跑：

```sh
git worktree add --detach /tmp/minikv-arrival-286ed8e 286ed8e
make -C /tmp/minikv-arrival-286ed8e JOBS=4
arrival_source=/tmp/minikv-arrival-286ed8e
arrival_output=/mnt/nvme/minikv-arrival-rerun

for arrival_stage in A1 B1 C1 B2 C2 A2 C3 A3 B3; do
  case "$arrival_stage" in
    A*) arrival_workers=256; arrival_flush=2 ;;
    B*) arrival_workers=20; arrival_flush=1000 ;;
    C*) arrival_workers=256; arrival_flush=1000 ;;
  esac
  if python3 "$arrival_source/benmark/experiment.py" \
    --engine "$arrival_source/build/engine" \
    --gateway "$arrival_source/bin/minikv-go" --bench "$arrival_source/bin/minikv-bench" \
    --output "$arrival_output/$arrival_stage" --modes reliable --repeats 1 \
    --rate 500 --requests 5000 --op put --keyspace 1000 --value-size 128 --seed 1 \
    --workers "$arrival_workers" --engine-workers 4 --rpc-pool 256 --gomaxprocs 4 \
    --wal-batch 65536 --wal-flush-ms "$arrival_flush" --snapshot-ms 1000 \
    --sample-ms 100 --stats-ms 250 --run-timeout 60 \
    --startup-timeout 10 --shutdown-timeout 15 --settle-timeout 10; then
    arrival_exit=0
  else
    arrival_exit=$?
  fi
  printf '%s runner exit=%s\n' "$arrival_stage" "$arrival_exit"
  if [ "$arrival_exit" -gt 1 ]; then break; fi
done
python3 benmark/summarize.py "$arrival_output"/A* "$arrival_output"/B* "$arrival_output"/C*
```

退出 1 既可能是预期的 `degraded`，也可能是实验失败；复现时必须逐阶段检查 `index.json`、`result.json` 和通用汇总，不能只按退出码认定成功。专用分析脚本用于冻结的历史归档；新运行的机器、执行文件哈希和路径可能不同，应保存自己的计划与背景，不通过修改历史清单让新数据冒充旧记录。

这组结果补上了固定到达率下的名额限制证据。后续仍需独立覆盖实际慢存储、长时间运行、大数据集、持续恢复与新旧实现对照；本次不会据此修改默认容量或作生产容量承诺。
