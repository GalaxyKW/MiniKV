# 运行状态与指标口径

[返回 README](../README.md) · [配置与操作](usage.md) · [存储设计](design.md) · [性能实验](testing.md)

`GET /stats` 返回引擎、请求调度和网关 RPC 池的 JSON 状态。启动服务后，在仓库根目录执行：

```sh
curl -sS http://127.0.0.1:8080/stats | python3 -m json.tool
```

正常响应包含 `schema_version: 1`、`engine`、`server` 和 `gateway` 四个字段。接口只读取聚合状态，不读取 key/value，不触发 WAL 同步或快照，不推进日志序列。返回内容不包含用户数据、数据目录或原始 I/O 错误文本。

## 如何读取一个样本

- `engine.applied_sequence - engine.durable_sequence` 表示内存已应用但尚未确认同步的日志记录数量；包含删除未命中产生的日志。
- `engine.data_bytes` 接近正的 `data_capacity_bytes` 且 `data_rejections_total` 增加，表示 PUT 达到了数据量上限；即使请求队列空闲，也可能返回 HTTP 503。覆盖较小值或删除数据可释放这项额度。
- `engine.wal_pending_bytes` 是全部未同步 WAL 字节，**包含正在写盘的批次**。`wal_inflight_bytes` 只表示当前实际提交中的批次，`wal_queued_records` 只计算尚未被取走的记录。
- `server.request_queue_depth` 表示尚未执行的任务，`requests_inflight` 还包含正在执行、等待可靠确认和等待 reactor 消费结果的请求。任一容量耗尽都可能使 `requests_rejected_total` 增长。
- `engine.wal_capacity_waiters` 表示因 WAL 字节额度不足而阻塞的写请求，仍会占用数据线程；`wal_durable_waiters` 表示等待可靠确认的请求，通过异步完成路径释放数据线程。因此有持久化等待时，`workers_active` 也可能接近 0。
- `gateway.rpc.pool_in_use` 包含拨号和执行中的数据 RPC；池满时其他请求等待。`pool_wait_duration_ns_total` 的增量可以帮助识别网关等待。
- `gateway.http.data.inflight` 覆盖上传、RPC 等待及 handler 响应写入；`rejected_total` 增长表示网关在解析前拒绝了新请求。数据 RPC 池仍空闲时，慢上传也可能占满 HTTP 名额。
- `engine.snapshot_in_progress` 与快照各阶段的耗时增量可以帮助判断尾延迟是否伴随快照发生。它们不是请求延迟直方图。

引擎字段在同一次状态锁持有期间取样；线程池、连接计数和网关分别取样。**整份响应不是跨线程、跨进程的原子快照**，也不提供逐 key 事务视图。计数器可能在连续读取字段期间继续增长。

计数和累计耗时从对应进程启动时重新计数；重启会保留从文件恢复的序列与数据量。新建数据目录会制作初始空快照，因此初次启动的 `snapshot_successes_total` 通常从 1 开始。普通 v2 恢复不计作本次进程的 WAL 提交或快照；旧 WAL 升级实际制作的新快照会计入快照次数、耗时和文件字节。

## 引擎字段

`engine` 中的大小单位为字节，`*_ns*` 为单调时钟测得的纳秒，序列和计数为无符号 64 位整数。

| 字段 | 含义 |
| --- | --- |
| `wal_mode` | 实际持久化模式：`throughput` / `reliable` |
| `keys` | 当前内存数据集中的 key 数量 |
| `data_bytes` / `data_capacity_bytes` | 当前所有 key/value 的总字节数 / 配置上限，容量 0 表示不限额；不等于进程 RSS |
| `data_rejections_total` | 本进程因数据字节上限拒绝的 PUT 数量；拒绝不写 WAL、不推进序列 |
| `applied_sequence` / `durable_sequence` | 已应用到内存 / 已确认持久化的日志序列 |
| `wal_pending_bytes` | 全部未同步记录的编码字节数，包含队列和写盘中的批次 |
| `wal_inflight_bytes` | 当前 WAL 提交批次的字节数；该次提交成功或失败后归零 |
| `wal_queued_records` | 仍在待写队列中的记录数量，不包含已取走的批次 |
| `wal_queue_capacity_bytes` | 配置的未同步 WAL 字节上限 |
| `wal_capacity_waiters` | 当前因未同步 WAL 容量不足而等待的写请求数 |
| `wal_capacity_waits_total` / `wal_capacity_wait_duration_ns_total` | 已结束的 WAL 容量等待次数 / 累计耗时，包含失败与关闭唤醒 |
| `wal_durable_waiters` | 当前等待可靠确认的 GET / PUT / DELETE 请求数 |
| `wal_durable_waits_total` / `wal_durable_wait_duration_ns_total` | 已结束的可靠确认等待次数 / 累计耗时，包含失败与关闭唤醒 |
| `async_requests_inflight` / `async_requests_capacity` | 引擎异步名额的占用数 / 上限，包含提交预留、可靠确认等待，以及尚未返回或释放捕获资源的回调 |
| `async_callback_failures_total` | 完成回调抛出异常的次数；异常被捕获，不会使其他回调停止或将存储标记为失效 |
| `wal_commits_total` / `wal_commit_failures_total` | 本进程成功 / 失败的非空 WAL 批次提交次数 |
| `wal_commit_duration_ns_total` | 所有已结束的 WAL 批次提交耗时之和，包含失败 |
| `wal_commit_last_duration_ns` | 最近一次已结束的 WAL 提交耗时，可能属于失败尝试 |
| `snapshot_successes_total` / `snapshot_failures_total` | 完整完成 / 执行失败的快照次数，包含手动与自动快照 |
| `snapshot_in_progress` | 快照已开始执行且尚未结束；不包含排队等待前一个快照的调用 |
| `snapshot_sequence` | 最近完整完成 checkpoint 的序列；启动时从已恢复的快照头读取 |
| `snapshot_capture_duration_ns_total` | 已结束的捕获阶段耗时：等待 I/O 锁、复制状态、同步捕获批次、确定 WAL 边界 |
| `snapshot_capture_state_lock_acquisitions_total` | 已结束的快照捕获临界区次数，包含其中抛出异常的尝试 |
| `snapshot_capture_state_lock_duration_ns_total` / `snapshot_capture_state_lock_duration_ns_max` | 上述临界区持有状态锁的累计耗时 / 本进程已完成临界区的最大耗时 |
| `snapshot_write_duration_ns_total` | 已结束的快照文件编码、写入、同步、rename 和目录同步耗时 |
| `snapshot_file_write_calls_total` | 快照临时文件的实际 `write` 调用次数，包含短写、EINTR、返回 0 和失败调用 |
| `snapshot_file_written_bytes_total` | 上述调用返回的正字节数之和，包含失败尝试中已经写入的前缀 |
| `snapshot_file_installed_bytes_total` | 完成文件同步、rename 与目录同步的快照文件字节数之和；随后 checkpoint 失败也保留 |
| `snapshot_compact_duration_ns_total` | 已结束的回收阶段耗时：等待 I/O 锁、复制 WAL 后缀并持久化安装 |
| `snapshot_compact_written_bytes_total` | WAL 后缀临时文件的 `write` 正返回字节数之和，包含失败尝试中已写入的前缀 |
| `io_failed` | 是否进入拒绝数据请求的存储失效状态 |
| `stopping` | 是否已进入存储关闭过程 |

WAL 提交计时从实际写出批次开始，到同步结束或发生错误为止，不包含等待 I/O 锁的时间。快照的阶段计时包含各阶段内的失败，直到阶段结束才累加；阶段正在进行时，不会持续增加该累计值。等待前一个快照和最终销毁内存副本的时间不计入三个阶段。

快照捕获阶段可能包含一次 WAL 提交，因此它与 WAL 提交耗时存在重叠，不能把两类累计值直接相加当成总运行时间。对两个没有跨进程重启的样本，平均 WAL 尝试耗时可按下式计算：

```text
Δ wal_commit_duration_ns_total
─────────────────────────────────────────────────────────────
Δ wal_commits_total + Δ wal_commit_failures_total
```

分母为零时没有新的已完成尝试，不能据此推断磁盘当前没有阻塞。累计平均值也不能代替 P99 等分位数。

发生 WAL 提交失败时，`wal_pending_bytes` 保留未获确认的数据量，而 `wal_inflight_bytes` 归零；失败批次不再处于正在提交的状态。这时不应把 `pending - inflight` 当成仍可正常提交的队列大小，需结合 `io_failed` 处理存储故障。

### 快照捕获临界区

`snapshot_capture_state_lock_*` 只计复制内存状态、读取序列和分离待提交队列的那一次临界区：成功取得状态锁后开始，在释放前结算，包含异常展开。它不包含等待状态锁、等待 I/O 锁、WAL 同步，也不包含快照前后更新状态的其他短临界区；最后的计账和解锁尾部不计入耗时。计量不新增锁或扫描，每次临界区增加两次时钟读取及常数次计数更新。

在未跨重启的窗口内，用持锁累计耗时增量除以临界区次数增量，可得到期间结束的捕获临界区平均耗时；分母为 0 时不可用。`*_max` 是进程生命周期内已完成临界区的最大值，不能通过相邻最大值相减得到窗口最大值。这些值不是逐请求暂停或延迟分位数，也不能把 `capture` 阶段耗时减去持锁耗时就称为纯 I/O 时间。

### 快照写入量

文件写调用、写入字节和安装字节在写出阶段结束时一起发布；后缀写入量在回收阶段结束时发布，失败也会结算。实际写入只更新阶段局部整数，沿用阶段结束时已有的状态锁合并计数，不在每条记录或每次系统调用时获取状态锁。阶段仍在进行时，已发生的 I/O 可能尚未出现在累计值中；进程硬退出前未结算的值不会恢复。

三个文件口径需要分别看：调用次数衡量实际进入 `write` 的次数；`written` 是内核通过正返回值确认接受的字节，不承诺已同步；`installed` 是已完成文件与目录同步的完整快照字节。安装成功后，测试钩子或 WAL 回收仍可能失败，因此 `installed` 可以增加而 `snapshot_successes_total` 不增加。它也不是当前快照文件大小，重试安装会继续累计。

全新空目录的初始快照包含 28 字节头部，会贡献写入与安装字节；普通加载不算本进程新写入，但旧 WAL 升级时新生成的快照会计入。`snapshot_compact_written_bytes_total` 记录替换 WAL 的 24 字节 v2 文件头与后缀重写，不包含普通 WAL 追加；因此无后缀记录也可能增加 24 字节。上述计数包含文件头与 CRC，既不是业务 value 总量，也不包含文件系统日志、目录元数据或设备内部写入，不能直接命名为设备写放大或要求与 `/proc/<pid>/io` 相等。

跨窗口的阶段在结束时贡献完整计数，可能包含窗口开始前的工作；直接差分除以窗口时长不一定等于该窗口内的实际写入带宽。比较每次快照的工作量时，应使用共同的空闲边界与完成计数，并保留失败和不同快照次数。持锁与文件指标用于缩小排查范围，仍需结合客户端尾延迟做端到端验收。

## 引擎请求调度

| `server` 字段 | 含义 |
| --- | --- |
| `connections` / `connection_capacity` | 当前引擎 TCP 连接数 / 配置上限，包含状态查询连接 |
| `connections_rejected_total` | 因达到连接数上限而关闭的新连接数量 |
| `request_queue_depth` / `request_queue_capacity` | 等待执行的数据请求数 / 队列上限 |
| `workers_active` / `workers_capacity` | 正在执行数据请求的线程数 / 数据工作线程总数 |
| `requests_inflight` / `requests_capacity` | 数据请求名额当前占用数 / 上限，容量为数据线程数加任务队列容量；结果已消费但提交或回调仍持有名额时，也继续计入 |
| `requests_started_total` | 已从数据任务队列取出并交给 worker 的请求数 |
| `request_queue_wait_duration_ns_total` | 上述请求从成功入队到出队的累计耗时 |
| `requests_rejected_total` | 因整请求名额耗尽或数据任务入队失败而返回 BUSY 的请求数量 |

数据任务统计不包含状态查询。引擎为 Stats 提供固定的 **1 个工作线程、1 个待执行位置和 2 个整请求名额**，使数据任务达到上限时仍可以读取状态。Stats 达到自身限制时也返回 BUSY，但不增加数据任务的拒绝计数。

`workers_active` 覆盖取出任务后的执行与异步提交，包含 WAL 容量等待，不包含交给完成线程的可靠确认等待。断连和连接超时不会撤销操作，也不会立即释放 `requests_inflight`。名额最早在 reactor 消费或丢弃结果后归还；若提交任务或回调仍持有它，还需等这些引用释放。归还不以网络发送完成为条件，发送缓冲另受连接数和帧大小约束。

引擎的异步名额在回调及其捕获资源释放后归还，服务器名额还覆盖完成结果被消费的过程。两者属于不同生命周期，不能相加计算请求总量；也不能要求每次并发取样时两者严格相等。直接调用同步 `Engine::execute` 的请求不占异步名额。

## 区分三种等待

请求先等待数据 worker，再执行内存与 WAL 操作；写请求可能先等待 WAL 容量，可靠模式的读写还可能等待持久化进度。三类计时边界不同：

| 等待 | 起止位置 | 不包含什么 |
| --- | --- | --- |
| 请求排队 | 持队列锁成功入队，到 worker 取出该任务 | 收包、解码、入队前锁竞争、执行和发送响应 |
| WAL 容量 | 首次发现未同步字节额度不足，到等待条件满足并重新取得状态锁 | 编码记录、内存修改和随后可靠确认 |
| 可靠确认 | 首次发现目标序列尚未持久化，到完成线程持状态锁选出结果；同步 API 到等待结束并重新取得状态锁 | 之前的任务排队、WAL 容量等待、完成回调执行和响应发送 |

排队中的请求尚未计入 `requests_started_total` 或排队累计耗时。每个已接纳的数据请求出队时都会计数，半包、非法帧与入队拒绝不计入。断连不撤销已接纳的操作，正常停机也会排空它们，因此这些任务仍会计入。

Engine 的两类等待只在初始条件不满足时计数，已经持久化的 GET 不会增加可靠等待次数。等待尚未结束时只增加 `*_waiters`，次数与耗时在结束时一起累加；失败或关闭唤醒也属于已结束等待。实际计时包含线程调度与重新获取锁的时间，不能当作单独的磁盘耗时。

两个未跨重启的样本之间，可用 `Δ wal_durable_wait_duration_ns_total / Δ wal_durable_waits_total` 计算期间结束的可靠等待的平均耗时；容量等待使用同样口径，平均排队时间的分母为 `Δ requests_started_total`。分母为 0 时没有新的已结束观测，应结合当前 waiter 与队列深度判断。窗口开始前进入、窗口内结束的等待会贡献完整耗时。

不同请求的等待会重叠，也可能与 WAL 提交、快照计时重叠；不要把这些累计值相加当成总运行时间，也不能从平均值推导 P99。它们用于定位下一步实验，逐请求尾延迟仍由客户端报告等观测提供。

数据容量、等待、异步容量和新增快照持锁及写入量字段沿用 `schema_version: 1`。新版网关连接不提供这些字段的旧版引擎时，会省略相应字段；缺失表示不可用，不能当作实测 0，实际的 0 会保留。数据量和正容量均提供时，网关还会拒绝数据量大于容量的异常状态。原始 schema 的基础字段必须完整且非 null；缺失或 null 会返回 `502 invalid_backend_stats`，保留本机网关统计，不会把缺失的计数或健康标志补成 0 或 false。计时沿用现有锁，读取时钟与累计计数仍有开销。早期同步服务版本的可靠等待会占据数据线程，比较工作线程利用率时必须同时记录代码版本与执行方式。

## 网关 RPC

`gateway.uptime_seconds` 是 HTTP 网关开始服务以来的单调时钟秒数。`gateway.rpc` 只描述 `/kv` 使用的数据 RPC 池；状态查询另用容量为 1 的池，因此不会污染数据请求计数或占据数据池名额。

新版网关还返回 `gateway.http.data` 和 `gateway.http.stats`，分别描述 `/kv` 与 `/stats` 的 HTTP 接纳名额：

| 字段 | 含义 |
| --- | --- |
| `capacity` | HTTP handler 同时接纳数上限；数据默认等于 RPC 池大小，状态固定为 1 |
| `inflight` | 已接纳且 handler 尚未返回的请求数，包含正在上传、RPC 排队、执行与 handler 内响应写入的请求 |
| `rejected_total` | 名额满时在参数解析前拒绝的累计请求数，不包含引擎 BUSY 或其他错误 |

这些字段来自本机网关，不接受后端提供的值；计数重启归零。状态查询会占用自己的 HTTP 名额，因此正常 `/stats` 响应中的 `http.stats.inflight` 包含本次查询。名额计数与拒绝计数分别取样，与 RPC 统计也不构成原子快照。旧网关没有 `http` 字段时表示不可用，不能补成实测 0。

HTTP 名额不覆盖连接读头、拒绝路径及 net/http 在 handler 返回后的缓冲和正文收尾，不能用它推算全部 HTTP 连接、goroutine 或进程内存上限。HTTP 名额、数据 RPC 名额和引擎请求名额分别在各层资源释放后归还，取消 HTTP 等待不会撤销引擎操作。

| `gateway.rpc` 字段 | 含义 |
| --- | --- |
| `pool_capacity` / `pool_in_use` | 数据 RPC 名额上限 / 正在拨号或执行 RPC 的名额数 |
| `connections` / `idle_connections` | 数据池已建立连接数 / 可复用的空闲连接数 |
| `closed` | 数据 RPC 客户端是否已关闭 |
| `calls_total` / `errors_total` | 逻辑 RPC 调用次数 / 最终返回 RPC 错误的次数 |
| `retries_total` | 发生的额外只读尝试次数 |
| `pool_acquires_total` | 请求 RPC 名额的尝试次数，包含立即获得、超时和取消 |
| `pool_wait_duration_ns_total` | 已结束的名额申请累计等待时间，不包含随后拨号或仍在等待的申请 |
| `exchanges_total` / `exchange_errors_total` | 获得连接后开始收发的尝试次数 / 已结束且返回错误的尝试次数 |
| `exchange_duration_ns_total` | 已结束的收发尝试耗时，包含取消处理和归还连接，不包含池等待与拨号 |

`calls_total` 在调用开始时增加，错误与耗时在对应尝试结束时更新，不能在并发采样时把它们强行视为同一个完成时刻。GET 内部重试会增加名额申请次数；只有取得连接后才增加收发尝试次数，逻辑调用仍只计一次。HTTP 接纳拒绝或参数校验失败不会调用 RPC；后端返回 NOT_FOUND、BUSY 或 I/O 状态属于成功收到协议响应，不增加 RPC 传输错误计数。

## 访问与故障边界

成功读取状态返回 HTTP 200；`io_failed: true` 仍然可以出现在 200 响应中。**状态可读取不代表存储可以继续处理数据请求**，检查健康状况时还需读取这个字段。

| 情况 | HTTP 状态 | JSON `error` |
| --- | ---: | --- |
| 网关状态 HTTP 名额已满 | 503 | `gateway_overloaded` |
| 后端连接失败、RPC 客户端关闭或 Stats 队列满 | 503 | `backend_unavailable` |
| 状态 RPC 超时 | 504 | `backend_timeout` |
| 旧引擎不识别 Stats 操作 | 502 | `stats_unsupported` |
| 后端返回不合法的状态帧或 JSON | 502 或 503 | `invalid_backend_stats` 或 `backend_unavailable` |

这些失败响应仍包含 `schema_version` 和本机 `gateway` 数据，省略不可得的 `engine` 与 `server`，不返回陈旧缓存。取得 HTTP 名额后的非 GET 方法返回 405，并设置 `Allow: GET`；名额已满时优先返回 503。所有响应禁止缓存。

状态通道仍共享引擎连接总上限与状态锁：连接数耗尽时新状态连接可能被拒绝，大数据集快照复制也可能让查询等待状态锁并超时。它没有独立的磁盘健康探针，也不承诺在任意过载下总能访问。单次查询沿用 `MINIKV_RPC_TIMEOUT_MS` 的等待、拨号和收发预算；一个网关最多比数据池上限多持有 1 条状态连接。

`/stats` 使用现有 HTTP 监听地址，没有额外鉴权。其对应的二进制协议扩展见[网络协议](design.md#网络协议-v1)。
