# 可复现性能实验

[返回 README](../README.md) · [压测指标](testing.md) · [运行状态](observability.md)

一次可复查的实验需要同时保留负载配置、实际执行文件、服务状态、资源样本与压测结果。固定随机种子能够固定请求内容，但吞吐量、尾延迟和恢复行为仍需结合运行环境解释。

## 单次运行与对照实验

需要 Linux、Python 3.8+ 和已经构建的三个执行文件。所有命令从仓库根目录运行：

```sh
make JOBS=4
python3 benmark/experiment.py --repeats 1 --requests 200 --keyspace 32
```

这是一轮流程检查：运行两种持久化模式，各自比较关闭和开启自动快照，共 4 次独立实验。程序自行启动引擎、网关和压测进程，只监听本机回环地址并选择临时端口，无需另外启动服务。默认产物位于 `benmark/results/<UTC时间>/`，实际路径会打印为 `Artifacts: ...`。

上述短负载不一定覆盖一次快照，适合确认构建、启动、采样、报告和停机流程。要按默认参数执行完整矩阵：

```sh
python3 benmark/experiment.py
```

| 设置 | 实验程序默认值 |
| --- | --- |
| 持久化模式 | `throughput,reliable` |
| 快照对照 | 每种模式都运行关闭快照和间隔 1000 ms 两种情况 |
| 重复次数 | 3，共 12 次独立实验；重复之间轮换配置的执行顺序 |
| 每次测量请求 | 20,000；20 个并发 worker |
| 数据与操作 | 1,000 个 key，mixed，20% PUT、5% DELETE，其余 GET |
| value 与 seed | 128 字节；seed 为 1 |
| 引擎数据线程 / 网关数据 RPC 池 | 4 / 32 |
| Go 调度 | 网关和压测进程均使用 `GOMAXPROCS=4` |
| WAL 提交 | 批量阈值 64；刷新间隔 2 ms |
| 资源 / 状态周期采样 | 100 ms / 250 ms |
| 单次压测超时 | 120 秒，包括数据预置和生成报告 |
| 启动 / 关闭 / WAL 排空等待 | 每次启动等待 10 秒；每个进程关闭等待 15 秒；排空等待 10 秒 |

这些是实验程序的受控配置，与直接启动服务时的默认值不同。程序向子进程传递明确的运行环境，不继承 shell 中的 `MINIKV_*`、代理或 Go 调优变量；改动实验应使用对应 CLI 参数，并以 `commands.json` 为准。

GET/mixed 会在测量前预置全部 key；PUT/DELETE 不做数据预置。预置不计入压测工具的测量耗时，但会影响后续缓存和内存状态，也可能触发快照。每次实验都使用新的数据目录，不复用前一次实验的数据。

例如，只比较可靠模式、加长负载并将结果保存到一个明确的新目录：

```sh
python3 benmark/experiment.py \
  --output /tmp/minikv-reliable-experiment \
  --modes reliable --repeats 3 \
  --requests 200000 --workers 20 --keyspace 10000 \
  --snapshot-ms 1000 --value-size 128 --seed 1 --run-timeout 600
```

`--output` 必须尚不存在；重复执行时应选择另一个目录。上述长负载将单次运行预算扩大到 600 秒，预算包含预置耗时。`--snapshot-ms` 指开启快照的对照组间隔，不能用 0 取消对照矩阵，每种模式始终另有关闭快照的一组。增加请求数并不保证运行一定够长，仍需检查快照观测证据。完整参数可查询：

```sh
python3 benmark/experiment.py --help
```

## 产物与复查顺序

完成的产物目录通常包含：

```text
<output>/
  manifest.json
  index.json
  binaries/
    engine
    gateway
    bench
  r01-throughput-snapshot-off/
    commands.json
    result.json
    report.json
    stats-before.json
    stats-after.json
    stats-settled.json
    stats.jsonl
    resources.jsonl
    engine.log
    gateway.log
    benchmark.stderr.log
    data/
  r01-throughput-snapshot-on/
  ...
```

| 文件 | 复查用途 |
| --- | --- |
| `manifest.json` | 整体参数、执行顺序、运行环境、源码与机器元数据，以及原始执行文件和运行副本的身份 |
| `index.json` | 矩阵进度、每次结果及成功/失败次数 |
| `binaries/` | 本次实验实际运行的三个执行文件副本，SHA-256 与初始记录一致 |
| 每次实验的 `commands.json` | 实际命令、工作目录和明确传给子进程的环境 |
| `result.json` | `ok` / `failed` / `interrupted` 状态、错误、进程退出情况和观察摘要 |
| `report.json` | 压测工具原始 JSON：实际操作数、结果分类、延迟与吞吐量 |
| `stats-before/after/settled.json` | 预置前、压测进程结束后、未同步 WAL 排空后的运行状态 |
| `stats.jsonl` | 带查询起止时间的状态样本，包括周期查询和阶段边界查询 |
| `resources.jsonl` | 引擎、网关、压测进程的 `/proc` 资源样本 |
| 日志与 `data/` | 排查启动、运行、停机或持久化问题的现场 |

先检查 `index.json` 中的失败次数与各轮 `result.status`。`index.status: complete` 表示计划中的实验都跑完，**不表示每次都成功**。再检查 `report.json` 的结果分类和实际操作数量，最后结合运行状态、资源与日志解释差异。失败实验只保证保留已经生成的产物，文件可能缺失或不完整。

吞吐模式在压测结束时可能仍有未同步 WAL。程序分别保留立即采样的 `stats-after.json` 和排空后的 `stats-settled.json`，将额外等待记录为 `wal_drain_elapsed_ns`；排空时间不会加进压测报告的测量耗时或 QPS 分母。

## 汇总多轮结果

汇总工具只读取已有产物，不启动服务、不修改输入目录：

```sh
python3 benmark/summarize.py /tmp/minikv-reliable-experiment
python3 benmark/summarize.py /tmp/minikv-reliable-experiment --format json > /tmp/minikv-summary.json
python3 benmark/summarize.py /tmp/minikv-reliable-experiment --format csv > /tmp/minikv-summary.csv
```

将示例路径换成实际实验目录。可同时传入多个目录，但各实验始终分别汇总，不自动混合不同版本、配置或负载的结果。重定向会覆盖同名输出文件，保留不同实验时应使用不同文件名。

工具先核对计划是否包含参数指定的完整矩阵，再逐轮核对命令、报告、状态和退出记录，按 WAL 模式与快照间隔分组。输出包含每轮值，以及成功 QPS、每轮 P99、进程 RSS/HWM 的有效轮次数、最小值、中位数和最大值。**多轮 P99 的中位数不是合并全部请求后的 P99。** 失败、中断、缺失和未完成的轮次仍列出；缺少资源样本使用 `null`，不会按 0 计算。

资源和快照证据从原始 JSONL 重新计算，不直接采用 `index.json` 或 `result.json` 中缓存的指标。开启快照但缺少测量期活动证据的轮次会明确标记，不能据此推断快照代价。

目录可以整体移动。原始绝对命令路径作为来源记录，不会被跟随读取；保留的执行文件副本存在时会核对 SHA-256。便携归档可以省略全部执行文件副本，此时显示 `recorded_hashes_only`，说明只有记录中的哈希，无法重新核验文件内容；存在但不匹配的副本会使相关结果无效。

JSON 和 CSV 汇总结构版本为 1；QPS 单位为请求/秒，P99 为毫秒，内存为字节。所有计划轮次有效时退出 0；存在失败、缺失、中断、未完成或无效轮次时仍输出汇总并退出 1；参数或根 manifest 不合法时退出 2。可选采样缺失本身不等于请求失败，仍应查看警告和有效样本数。

## 采样与快照观测

`--sample-ms` 控制周期资源采样，`--stats-ms` 控制周期 `/stats` 查询。二者都可设为 0：

```sh
python3 benmark/experiment.py --sample-ms 0 --stats-ms 0
```

此时仍保留正常完成所需的 before、after、settled 状态文件；资源 JSONL 可以为空。采样间隔是目标间隔，文件读取和状态请求耗时会使实际时间发生偏移，不能据此假设严格等距采样。

程序使用压测报告中的 `measurement_started_at` 和 `elapsed_ns`，将样本按墙钟时间近似对齐到测量区间；摘要标记为 `alignment: wall_clock_approximate`，不声称跨进程共享同一个单调时钟基准。

开启快照的实验中，`snapshot_activity_observed` 只在以下任一证据存在时为真：

- 至少两个成功状态查询的起止时间都落在测量区间内，且这两个样本之间的快照完成次数增加。
- 完全落在测量区间内的成功样本直接观察到 `snapshot_in_progress: true`。

样本不足时，快照完成次数增量为 `null`；只在预置阶段或测量之后发生的快照，不会被算成测量期的证据。没有观察到活动会打印提示，但不会自动判定实验失败，也不能据此断言快照没有发生。关闭快照组的 `snapshot_activity_observed` 为 `null`。

周期状态请求的失败会留在原始样本和错误次数中；元数据或某些资源字段不可用也会明确记录。它们不必然导致整次实验失败，因此 `result.status: ok` 不能替代对观测覆盖度的检查。开始、结束和 WAL 排空阶段的必要状态检查失败，则会使该轮失败；检查要求引擎未失效、未关闭且 `snapshot_failures_total` 为 0。

## 环境与执行文件记录

元数据中的 `collected_at_utc` 是采集时的 UTC 时间；`platform` 记录系统、内核版本、架构及 Python 版本。`cpu` 包括 `/proc/cpuinfo` 中的型号、逻辑 CPU 数、采集进程的 CPU affinity 和 `clock_ticks_per_second`。逻辑 CPU 总数与当前进程允许使用的 CPU 集合可能不同。

`memory.total_bytes` 与 `memory.available_bytes` 来自当前可见的 `/proc/meminfo`，单位为字节。它们提供机器内存背景，不代表进程 RSS，也不代表容器的 cgroup 内存额度。

`git` 记录当前 HEAD、相对 HEAD 的受跟踪改动摘要及工作区状态：

- `tracked_diff_sha256` 是二进制格式 Git diff 的 SHA-256，不记录 diff 内容；该 diff 比较 HEAD 与当前工作树。
- `tracked_dirty` 与 `dirty` 来自 Git 工作区状态，包含暂存区变化；即使工作树内容与 HEAD 相同，暂存区仍可能使它们为真。
- `untracked_files` 只记录 Git 未忽略的未跟踪文件名，不读取其内容。文件名中的非 UTF-8 字节用转义文本表示。

每个 `binaries` 条目记录所选文件路径、字节数与 SHA-256。哈希期间检测到文件被替换或改变时，该条目不可用。`cmake_caches` 记录仓库默认构建目录，以及所选执行文件旁实际存在的 CMake 缓存中的指定编译字段，包括构建类型、编译器、编译和链接参数、sanitizer 开关等。

这里的 `binaries` 位于 `manifest.metadata`，描述原始所选文件。程序在矩阵开始前将它们复制到输出目录的 `binaries/`，校验副本 SHA-256 与初始记录一致，并设置为 0555 后从副本运行。复制期间检测到内容变化会终止实验。`manifest.executables` 记录这些运行副本的路径和哈希；原始构建路径随后被 `make` 替换，不会换掉本次矩阵正在使用的文件。

**源码状态、执行文件哈希与 CMake 缓存是独立证据。** 缓存可能来自较早的构建，这些记录不能单独证明执行文件由当前源码和参数生成。实验前应完成对应构建，并保留执行副本与源码记录供后续复查。元数据不转储环境变量、不读取未跟踪文件内容，也不访问外部网络；Git 元数据查询会排除能够重定向仓库或配置的 `GIT_*` 环境变量。

## 进程资源采样口径

每个进程样本包括 PID、UTC 时间和 `monotonic_ns`。进程由 `pid` 与 `starttime_ticks` 共同识别；采样前后读取 `/proc/<pid>/stat`，发现进程退出或 PID 对应的启动时间变化时，会丢弃该次可能混合了不同进程的数据。

| 字段 | 来源与含义 |
| --- | --- |
| `rss_bytes` | `/proc/<pid>/status` 的 VmRSS：读取时的驻留内存 |
| `hwm_bytes` | 同一文件的 VmHWM：内核记录的该进程生命周期 RSS 峰值 |
| `cpu_user_ticks` / `cpu_system_ticks` | `/proc/<pid>/stat` 的用户态 / 内核态累计 CPU ticks，包含该进程线程，不包含已等待子进程的 CPU 时间 |
| `starttime_ticks` | 同一 stat 文件中的启动时间，以开机后的 ticks 表示；用于区分 PID 复用 |
| `read_bytes` / `write_bytes` | `/proc/<pid>/io` 的存储 I/O 字节记账，不是 HTTP payload 或 read/write 系统调用的总传输量 |
| `cancelled_write_bytes` | `/proc/<pid>/io` 中取消写入的字节记账，与 `write_bytes` 分别保留 |

CPU 时间需要除以元数据中的 `cpu.clock_ticks_per_second`；采样间隔使用 `monotonic_ns` 的差值，避免把墙钟调整当作经过时间。只有同一进程身份的两个有效样本才能计算增量。

`max(rss_bytes)` 只是采样时刻观察到的最大 RSS，可能漏掉间隔内峰值。VmHWM 能保留内核记录的生命周期峰值，但它包含启动、数据预置等阶段，不能自动当作压测测量阶段独有的峰值。I/O 记账也不等于已经同步持久化的字节数；写入确认进度应结合 `/stats` 的持久化序列判断。

结果摘要中的 `measurement_sampled_rss_max_bytes` 只使用近似测量区间内的 RSS 样本；`observed_lifetime_hwm_bytes` 是所有已采样 HWM 的最大值。后者仍然可能漏掉最后一次样本之后发生的峰值。相应样本为空时使用 `null`。

各组的 `available` 仅在该组所有要求字段都成功采集时为真。权限不足或缺失字段会保留其他可用实测值，并将缺失值记为 `null`、在 `errors` 中记录原因。进程身份无法确认时，该次进程数值全部为 `null`。**不可用值不应按 0 纳入统计。** `/proc` 各文件分别读取，资源样本不是原子快照。

## 可重复的范围

当前压测采用固定并发的闭环负载：服务变慢时，请求发送速度也会下降。固定 seed 与负载生成器版本可以固定每个请求序号的操作、key 和 value，不能固定网络到达顺序、读命中结果或并发写入的最终状态。

对照实验应重复运行，联合查看成功吞吐量、错误率、延迟分布、持久化进度和资源开销。额外的采样本身会带来开销；比较时保持相同频率，并通过关闭采样的对照评估其影响。当前资源记录不等价于固定外部到达速率实验、真实断电验证或长期稳定性测试。

## 失败、中断与现场保留

每个子进程使用单独的会话。程序通过自己创建的进程句柄，按 **压测 → 网关 → 引擎** 的顺序关闭；超过配置的关闭等待时间才强制结束，并把强制关闭或非零退出写入该轮错误。它不会按执行文件名称查找或终止其他进程。

某一轮启动失败、运行超时、压测请求失败、报告不合法或正常关闭失败时，该轮记为 `failed`，保留现场后继续后续矩阵。收到 SIGINT/SIGTERM 时，当前轮记为 `interrupted`，完成清理并停止后续矩阵。全部成功时程序退出码为 0，存在失败或中断时为 1；参数错误、执行文件不可用或输出目录已存在时，参数解析以 2 退出。

输出目录不会被自动清空或覆盖，每轮的 `data/` 在停机后仍保留。重新运行使用新的输出目录；程序不提供从已有产物目录续跑的功能。若进程被无法处理的信号直接结束，状态可能仍停留在 `running`，应结合日志和实际进程状态检查现场，不能当成完整结果。
