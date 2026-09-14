# 压测配置与 JSON 报告

[返回 README](../README.md) · [性能实验](testing.md) · [运行状态](observability.md)

`minikv-bench` 默认输出可读的文本报告，`-format json` 输出一个带版本号的 JSON 对象，便于保存原始结果和做后续比较。所有命令在仓库根目录执行；使用独立的实验数据目录启动引擎与网关后运行：

```sh
./bin/minikv-bench \
  -url http://127.0.0.1:8080/kv \
  -workers 20 -requests 20000 -op mixed \
  -keyspace 1000 -preload-count 1000 \
  -write-ratio 20 -delete-ratio 5 \
  -value-size 128 -seed 1 -timeout 2s \
  -format json > /tmp/minikv-bench-result.json

python3 -m json.tool /tmp/minikv-bench-result.json
```

JSON 模式下，数据预置进度和诊断写入 stderr，stdout 只包含报告。每轮应使用不同的输出文件名；终端重定向会覆盖同名文件。报告记录的是**压测客户端**的配置和构建信息。[自动化实验脚本](benchmark-experiments.md)可同时保留服务器配置、二进制摘要、当前源码状态、可见主机信息和资源采样；文件系统、设备与其他主机负载仍需补充说明。

## 参数与失败退出

| 参数 | 默认值 | 接受范围与含义 |
| --- | --- | --- |
| `-url` | `http://127.0.0.1:8080/kv` | 带主机的 HTTP/HTTPS 地址；不带用户信息、query 或 fragment |
| `-workers` | 50 | 正整数；并发 worker 数 |
| `-requests` | 200000 | 正整数；测量请求数，不含数据预置 |
| `-op` | `mixed` | `put` / `get` / `delete` / `mixed`，不区分大小写 |
| `-keyspace` | 20000 | 正整数；生成 `k0` 至 `k(keyspace-1)` |
| `-write-ratio` / `-delete-ratio` | 20 / 5 | 各为 0–100，之和不超过 100；mixed 剩余比例为 GET |
| `-value-size` | 128 | 0–1048576 字节；用于 PUT 和数据预置 |
| `-preload` | true | 仅在 GET/mixed 模式下预置数据；关闭写作 `-preload=false` |
| `-preload-count` | 20000 | 非负整数；0 表示整个 key 空间，执行时最多预置 keyspace 个 key |
| `-seed` | 1 | 有符号 64 位整数；与请求编号共同确定请求内容 |
| `-timeout` | 2s | 正的 Go duration，限制一次 HTTP 请求及响应体读取 |
| `-format` | `text` | `text` / `json` |

非法参数和多余位置参数在发送请求前报错，不再静默把负数、零或越界比例改成其他实验配置。整数溢出的容量也会被拒绝；合法的大请求数仍需按精确延迟切片的内存成本预留客户端资源。

| 退出码 | 含义 |
| --- | --- |
| 0 | 请求全部完成，只有成功和正常未命中；`-help` 也退出 0，但不执行实验或输出报告 |
| 1 | 数据预置失败、测量存在系统失败，或报告写出失败 |
| 2 | 参数解析或配置校验失败，不输出实验报告 |

数据预置失败时仍输出 JSON 报告：`complete=false`、`error="preload_failed"`，测量请求数与延迟样本数为 0；`preload.completed_keys` 保留已经成功写入的 key 数。测量中有失败请求时，`complete=true` 仅表示所有测量请求均已结束，是否成功需读取 `outcomes.failures` 或退出码。

## 稳定的请求集合

报告的 `workload_generator` 标识请求生成算法，当前为 `indexed-pcg-v1`。相同算法版本、seed、请求编号和负载参数，生成相同的操作、key 和 PUT value，不依赖 worker 编号、执行速度或调度顺序。

每个请求使用局部 PCG 状态，先抽取 key，再决定 mixed 操作，因此只改变操作比例或 value 大小不会改变 key 访问轨迹。value 使用由 seed 和请求编号组成的前缀，再截断或填充到指定大小；短 value 不保证请求间唯一。

固定负载集合不等于固定并发执行结果：请求到达顺序、GET/DELETE 命中率与最终状态仍可能不同。算法版本变化时，同一个 seed 不代表与旧版本相同的负载。客户端版本和 `workload_generator` 都应保留在比较记录中。

## JSON schema 1

报告中的时间戳为 UTC RFC 3339，`*_ns` 与 `latency_ns` 内的时间单位为纳秒。请求计数和配置字段使用整数。

| 顶层字段 | 内容 |
| --- | --- |
| `schema_version` | 报告结构版本，目前为 1 |
| `started_at` | 本次实验开始时间，位于数据预置之前 |
| `measurement_started_at` | 测量阶段开始时间；数据预置失败时省略 |
| `complete` / `error` | 测量是否完成，以及启动测量前的失败分类 |
| `load_model` | 当前为 `closed_loop`，每个 worker 等上一个请求结束后再发下一个 |
| `workload_generator` | 按请求编号生成负载的算法版本 |
| `config` | 完整客户端配置：URL、worker/request 数、操作、key 空间、超时、比例、预置、seed 和 value 大小 |
| `client_build` | 客户端 Go 版本、操作系统、架构；构建信息可得时附带 VCS 提交、时间和工作区修改标志 |
| `preload` | `target_keys`、`completed_keys`、`elapsed_ns`，与测量结果分开记录 |
| `elapsed_ns` | 测量请求分发到全部完成的耗时，不含数据预置和报告排序 |
| `outcomes` | 请求结果及互斥失败分类，见下表 |
| `operations` | 实际生成并尝试的 `put`、`get`、`delete` 数量，包含失败请求 |
| `http_statuses` | 已收到的 HTTP 状态分布；JSON 对象的状态码键为字符串 |
| `latency_ns` | `samples`、`mean`、`min`、`p50`、`p95`、`p99`、`p99_9`、`max` |
| `qps_total` / `qps_successful` | 总测量请求数 / 成功加正常未命中数除以测量秒数 |
| `system_success_rate_pct` | 成功加正常未命中占全部测量请求的百分比 |

`config.preload_count` 保留原始配置，`preload.target_keys` 给出根据模式、开关和 key 空间计算的实际目标数。例如配置 20000、keyspace 1000 时，两者分别为 20000 和 1000。`config.timeout_ns` 保存解析后的完整超时值；输出格式本身不改变负载。

延迟使用单调时钟测量 HTTP 请求构造、编码与收发过程，包含失败请求，不包含 key/value 的负载生成。百分位数使用排序后的 nearest-rank 样本；平均值向下取整到纳秒，累计计算不会因所有请求耗时之和超过 int64 而溢出。

## 失败分类与状态码

| `outcomes` 字段 | 口径 |
| --- | --- |
| `requests` | 实际完成的测量请求数 |
| `successes` | HTTP 状态和正文都符合对应操作的成功契约 |
| `logical_misses` | GET/DELETE 收到 404 且正文为 `NOT_FOUND\n` |
| `failures` | 其余请求；等于 `network_errors + http_failures + protocol_failures` |
| `network_errors` | 客户端传输或读取响应体失败；等于 `timeouts + transport_errors` |
| `timeouts` | 客户端返回超时错误；完整读取且未超限的 HTTP 504 响应属于 HTTP 状态失败 |
| `transport_errors` | 非超时的网络/响应体读取错误，例如截断响应 |
| `http_failures` | 正文完整读取且未超限，但 HTTP 状态不被该操作接受，包括服务端 503/504 和重定向 |
| `protocol_failures` | 状态可接受但正文不符，或响应超过最大 KV 响应大小 |

客户端不跟随 HTTP 重定向，避免一次基准请求悄悄访问另一端点或改变方法。响应体读取限制为 1 MiB value 加 `VALUE ` 前缀与换行，超过时计为协议失败。

失败分类先检查响应体是否读取失败或超限，只有完整读取且未超限时才按状态与正文契约分类。例如 HTTP 504 后正文截断仍是传输错误。已收到响应头后，即使正文截断或读取超时，HTTP 状态也保留在 `http_statuses`；在收到响应头之前失败的请求没有状态码。因此 HTTP 200 的数量不等于成功请求数，也可能与网络错误同时出现。HTTP 状态分布和失败分类分别描述响应的两个方面，不能相加当作总请求数。

负载仍为固定并发的闭环模型；要测固定到达速率下的过载行为，需要另行设计实验，不能仅凭闭环 P99 宣称系统容量。完整实验要求见[性能实验](testing.md#运行一次可复现的压测)。
