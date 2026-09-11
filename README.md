# MiniKV

MiniKV 是一个使用 C++17 存储引擎和 Go HTTP 网关的单机键值存储项目。数据驻留内存，WAL 与快照负责持久化和恢复。

目前支持 PUT / GET / DELETE、带校验和的 WAL 与快照、批量同步提交、非阻塞 TCP 连接管理，以及故障恢复测试。

## 构建

需要 Linux、支持 C++17 的编译器、CMake 3.16+、Go 1.22+ 和 Python 3。引擎使用 Linux 的 epoll、eventfd 和 flock；Go 程序没有第三方模块依赖。

```sh
git clone https://github.com/GalaxyKW/MiniKV.git
cd MiniKV
make
```

生成文件：

- `build/engine`：C++ 引擎。
- `bin/minikv-go`：HTTP 网关。
- `bin/minikv-bench`：压测工具。

构建产物不提交到源码仓库。可以用 `make JOBS=4` 调整编译并行度。

## 启动

在项目根目录启动引擎：

```sh
MINIKV_DATA_DIR=./data \
MINIKV_WAL_MODE=reliable \
MINIKV_WAL_FLUSH_MS=2 \
./build/engine
```

在另一个终端的项目根目录启动网关：

```sh
./bin/minikv-go
```

引擎默认监听 `127.0.0.1:9090`，网关默认监听 `:8080`。SIGINT/SIGTERM 会停止接入，给已接纳请求最多 5 秒发送响应，然后等待存储任务完成并同步待提交 WAL；磁盘阻塞可能延长退出时间。停止服务时先停网关，再停引擎。

已有旧版 `data.db` / `wal.log` 时，需要先停止旧进程、备份目录，再运行 `MINIKV_DATA_DIR=./data ./build/engine --import-legacy`。原文件保留，新文件使用 `snapshot.v1` / `wal.v1`。引擎与网关需要一起更新；迁移边界见 [设计与迁移说明](docs/design.md#旧版数据迁移)。

## HTTP API

```sh
curl -X POST http://127.0.0.1:8080/kv \
  -H 'Content-Type: application/json' \
  -d '{"key":"tenant:1","value":"hello\nworld  "}'

curl 'http://127.0.0.1:8080/kv?key=tenant%3A1'

curl -X DELETE 'http://127.0.0.1:8080/kv?key=tenant%3A1'
```

key 为 1–4096 字节，value 为 0–1 MiB。JSON 字符串中的换行、冒号、NUL 和尾部空白会被保留。GET/DELETE 的 key 使用 URL 编码。

| 结果 | HTTP 状态 | 文本响应 |
| --- | ---: | --- |
| PUT/DELETE 成功 | 200 | `OK\n` |
| GET 命中 | 200 | `VALUE ` + 原值 + 一个换行 |
| GET/DELETE 未命中 | 404 | `NOT_FOUND\n` |
| 参数或 JSON 无效 | 400 | 错误说明 |
| value 或请求体过大 | 413 | 错误说明 |
| 存储 I/O 失败或请求队列已满 | 503 | 错误说明 |
| 后端通信失败 / 超时 | 502 / 504 | 错误说明 |

读取值时只移除固定的 `VALUE ` 前缀和最后一个换行，不要对整个响应做 trim。网络失败时写入可能已执行，因此网关不自动重试写请求。

## 持久化保证

- `throughput`：内存更新、WAL 入队后返回，尚未同步的写入可能在故障中丢失。这是默认模式。
- `reliable`：WAL 完成 `fdatasync` 后才确认写入；多个请求可共享一次同步。GET 也会等待其观察到的状态持久化。
- 快照按“写临时文件 → 同步文件 → rename → 同步目录 → 回收旧 WAL”安装。
- WAL 尾部不完整记录可截断修复；校验和错误、日志序列缺失和快照损坏会导致启动失败。
- 存储 I/O 失败会使引擎拒绝后续请求并输出错误，不会把失败的记录标记为已同步。

详细的提交顺序、文件格式、故障模型与实现限制见 [设计说明](docs/design.md)。

## 测试

```sh
make test
make sanitize-test
```

测试包含：

- C++ 存储恢复、并发写入与快照、group commit、并发关闭、日志截断和校验和检查。
- 缺失快照时拒绝误建空库、恢复已被快照覆盖的 WAL 后继续写入并再次重启。
- WAL/快照 I/O 故障注入、真实短写/EFBIG、多个快照边界的进程退出。
- Go 网关参数校验、连接池总量上限、请求取消、超时分类和写请求不重试；启用 race 检查。
- C++ 与 Go 的端到端测试：40 条空闲/不完整连接、TCP 分片与流水线、1 MiB value、客户端 RST、过载反馈、停机响应、SIGKILL 后恢复。
- AddressSanitizer 和 UndefinedBehaviorSanitizer 检查。

测试使用独立临时目录和本机临时端口。进程退出测试不等同于真实断电测试；硬件持久性依赖文件系统和设备正确执行同步操作。CI 会执行上述两组命令。

## 压测

启动服务后，在项目根目录运行：

```sh
./bin/minikv-bench \
  -url http://127.0.0.1:8080/kv \
  -workers 40 -requests 400000 -op mixed \
  -keyspace 20000 -preload-count 20000 \
  -write-ratio 20 -delete-ratio 5 \
  -value-size 128 -seed 1
```

报告包含总 QPS、成功吞吐量（含正常未命中）、平均延迟、P50/P95/P99/P99.9、错误数与 HTTP 状态分布。预热失败或存在系统失败时，工具以非零状态退出。`-preload=false` 会关闭预热。

对比实验应固定硬件、构建类型、持久化模式、批量参数、数据量、value 大小、随机种子和命中率，并保存服务启动配置。压测默认采用固定并发的闭环负载；应另做固定到达速率的过载实验，不能仅凭闭环 P99 判断容量。

测试快照影响时，可用 `MINIKV_SNAPSHOT_INTERVAL_MS=1000` 缩短周期，并运行覆盖多个周期的负载。当前刷盘和快照仍持有存储锁，后台 I/O 对尾延迟的影响需要单独测量。

`benmark/out/` 中的现有数据与图表属于修复前版本，不代表新版性能。绘图依赖 Matplotlib：

```sh
python3 benmark/paint.py
# 服务运行时重新采样：
python3 benmark/paint.py --rerun
```

## 配置

| 环境变量 | 默认值 | 用途 |
| --- | --- | --- |
| `MINIKV_DATA_DIR` | `./data` | 相对当前工作目录或绝对数据目录 |
| `MINIKV_WAL_MODE` | `throughput` | `throughput` / `reliable` |
| `MINIKV_WAL_BATCH_SIZE` | 512 | 批量同步触发条数 |
| `MINIKV_WAL_FLUSH_MS` | 100 | 同步触发间隔，毫秒 |
| `MINIKV_WAL_QUEUE_BYTES` | 16777216 | 待提交 WAL 字节上限 |
| `MINIKV_SNAPSHOT_INTERVAL_MS` | 1200000 | 快照周期，0 禁用自动快照 |
| `MINIKV_ENGINE_HOST` / `MINIKV_ENGINE_PORT` | `127.0.0.1` / 9090 | 引擎监听地址 |
| `MINIKV_WORKERS` | 20 | 完整请求执行线程数 |
| `MINIKV_REQUEST_QUEUE_SIZE` | 128 | 待执行请求数量上限 |
| `MINIKV_MAX_CONNECTIONS` | 256 | 引擎连接数量上限 |
| `MINIKV_CLIENT_IDLE_MS` | 30000 | 连接无进展超时，毫秒 |
| `MINIKV_ENGINE_ADDR` | `127.0.0.1:9090` | 网关连接的引擎地址 |
| `MINIKV_HTTP_ADDR` | `:8080` | 网关监听地址 |
| `MINIKV_RPC_POOL_SIZE` | 64 | 网关到引擎的总连接上限 |
| `MINIKV_RPC_TIMEOUT_MS` | 2000 | 连接池等待与 RPC 的总超时 |

## 目录

- `cpp_engine/engine.*`：存储状态、WAL、快照与恢复。
- `cpp_engine/codec.h`：协议及持久化记录编码。
- `cpp_engine/server.cpp`：epoll 连接管理与请求调度。
- `go_server/`：HTTP API 与有界 RPC 连接池。
- `benmark/`：压测、绘图与历史结果。
- `tests/`：存储与端到端回归测试。
- `docs/`：设计取舍、格式与迁移说明。
