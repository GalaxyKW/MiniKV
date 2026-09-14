# 测试与性能实验

所有命令均在仓库根目录执行。构建和启动步骤见 [README](../README.md)，持久化语义与故障模型见 [设计说明](design.md)。

## 运行回归检查

```sh
make test
make sanitize-test
```

| 命令 | 实际执行内容 |
| --- | --- |
| `make test` | 构建 C++ 引擎、Go 网关与压测工具；运行三个 C++ 测试程序、`go test -race -timeout 60s ./...` 和 Python 端到端测试 |
| `make sanitize-test` | 使用 AddressSanitizer 与 UndefinedBehaviorSanitizer 构建 C++ 引擎及三个测试程序；运行 C++ 测试，以及连接该引擎的端到端测试 |
| `make unit-test` | 构建后运行 C++ 测试与 Go race 检查 |
| `make integration-test` | 构建后运行 Python 端到端测试 |

[CI 工作流](../.github/workflows/ci.yml) 在 push 和 pull request 时执行 `make test` 与 `make sanitize-test`。ThreadSanitizer 检查需要按下文手动运行。

测试使用独立临时数据目录；网络测试在本机临时端口启动服务，因此执行环境必须允许监听本机端口。无需提前启动常驻的引擎或网关。

## 每层测试验证什么

| 范围 | 验证内容 | 测试入口 |
| --- | --- | --- |
| 存储与恢复 | 二进制值、并发写入、group commit、目录独占、旧数据导入、并发关闭、WAL 尾部修复、校验和错误与文件缺失 | [engine_test.cpp](../tests/engine_test.cpp) |
| WAL 并发 | 暂停 WAL 同步时吞吐模式请求仍可执行；未同步批次仍占用队列额度；可靠模式按批次确认；关闭时排空正在同步和待写的批次 | [engine_test.cpp](../tests/engine_test.cpp) |
| 快照并发 | 快照文件写盘期间可靠读写继续完成；快照捕获固定版本；自动快照、多个快照串行执行、关闭等待快照、WAL 替换保留后续写入 | [snapshot_test.cpp](../tests/snapshot_test.cpp) |
| 故障边界 | WAL/快照 I/O 故障注入、文件大小限制触发真实短写/EFBIG、快照安装与 WAL 后缀替换边界的子进程退出、恢复后再次写入和重启 | [engine_test.cpp](../tests/engine_test.cpp)、[snapshot_test.cpp](../tests/snapshot_test.cpp) |
| HTTP 与 RPC | 参数与状态码映射、值字节保留、连接复用与总连接上限、取消与超时、异常响应处理、写请求不重试 | [main_test.go](../go_server/main_test.go)、[client_test.go](../go_server/client_test.go) |
| 运行状态 | WAL 队列与写盘批次区分、提交和快照失败计数、重启归零、RPC 等待与重试统计；状态查询不泄漏用户数据 | [stats_test.cpp](../tests/stats_test.cpp)、[stats_test.go](../go_server/stats_test.go) |
| 压测结果分类 | 同时检查 HTTP 状态和响应格式、正常未命中分类、关闭预热、预热失败处理；并发聚合中成功、失败、未命中与网络错误的计数守恒 | [main_test.go](../benmark/main_test.go) |
| 跨进程行为 | 空闲/不完整连接、TCP 分片与流水线、1 MiB value、客户端 RST、非法帧、队列过载、停机响应、SIGKILL 后恢复与混合负载 | [integration_test.py](../tests/integration_test.py) |

端到端测试还验证数据 worker、数据队列和网关 RPC 名额被占用时，`/stats` 仍可返回；引擎退出后可继续获取网关统计，重启后可读取恢复序列。

这些检查分别验证具体并发交错、错误处理和恢复边界。进程退出测试不等同于真实断电测试；文件同步的持久性仍依赖操作系统、文件系统和设备履行 `fdatasync/fsync` 约定。已有回归检查也不代表已经测得性能提升。

定位问题时可以单独运行测试。C++ 测试程序接受一个完整用例名称；可用名称见各文件末尾的测试列表。例如：

```sh
./build/engine_test 'in-flight WAL acknowledgements'
./build/snapshot_test 'snapshot I/O progress and captured version'
go test -race -count=1 ./go_server
python3 tests/integration_test.py -v MiniKVIntegration.test_acknowledged_writes_survive_kill
```

上面的命令假定已经执行过 `make`。完整回归仍应使用 `make test`，确保其他受影响路径一并检查。

## 手动检查 C++ 数据竞争

ThreadSanitizer 使用独立构建目录，与 ASan/UBSan 检查分开运行：

```sh
cmake -S cpp_engine -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON \
  '-DCMAKE_CXX_FLAGS=-fsanitize=thread -fno-omit-frame-pointer' \
  -DCMAKE_EXE_LINKER_FLAGS=-fsanitize=thread
cmake --build build-tsan --target engine_test snapshot_test stats_test -j2
TSAN_OPTIONS=halt_on_error=1 ctest --test-dir build-tsan --output-on-failure
```

该命令覆盖存储、快照和运行状态三个测试程序，不运行端到端网络测试。Go 数据竞争检查已由 `make test` 中的 `go test -race` 执行。

如果 TSan 在测试启动前报告 `unexpected memory mapping`，在支持 `setarch` 的 x86_64 Linux 环境中，可以仅对本次测试进程及其子进程关闭地址随机化后重试：

```sh
setarch x86_64 -R env TSAN_OPTIONS=halt_on_error=1 \
  ctest --test-dir build-tsan --output-on-failure
```

这不会修改系统级 ASLR 配置。如果环境禁止 `setarch` 修改进程属性，需要在允许该操作的环境中运行；此类启动失败无法得出测试是否通过的结论。

## 运行一次可复现的压测

按 [README](../README.md) 启动引擎和网关后，运行：

```sh
./bin/minikv-bench \
  -url http://127.0.0.1:8080/kv \
  -workers 40 -requests 400000 -op mixed \
  -keyspace 20000 -preload-count 20000 \
  -write-ratio 20 -delete-ratio 5 \
  -value-size 128 -seed 1 -timeout 2s
```

这会先预置整个 key 空间，再运行 PUT/GET/DELETE 混合负载。工具称为“预热”的这一步用于写入数据，不保证所有并发连接已建立或待提交 WAL 已同步；短实验可能包含这些启动成本。预置耗时不计入压测总耗时。`-preload` 默认开启，只对 `get` 和 `mixed` 生效；混合负载中的 DELETE 会改变后续命中率，预置所有 key 并不代表全程命中。

如果需要测量当前数据集而不预热，显式使用 Go 布尔参数写法 `-preload=false`：

```sh
./bin/minikv-bench \
  -url http://127.0.0.1:8080/kv \
  -op get -workers 40 -requests 400000 \
  -keyspace 20000 -preload=false -value-size 128 -seed 1
```

上面的 mixed 压测会写入或删除 `k0` 至 `k19999` 范围内的 key，应使用专门的实验数据目录。比较不同版本或配置时，保持初始数据状态一致，记录以下信息：

- 代码提交、编译器和 Go 版本、构建类型；默认 C++ 构建为 `RelWithDebInfo`。
- CPU、内存、文件系统、存储设备，以及客户端是否与服务端共用主机。
- 引擎和网关的完整启动配置，尤其是 WAL 模式、批量阈值、刷新间隔、队列容量、快照周期与连接池大小。
- 请求数、并发度、操作比例、key 空间、value 大小、随机种子、预热方式与观测到的未命中率。
- 每轮原始输出、重复轮次与波动；不要只保存最好的一轮。

固定随机种子便于控制负载生成参数，但并发调度和混合读写仍会改变实际交错与命中率。`throughput` 与 `reliable` 的成功确认语义不同，应分别报告结果。

## 正确解读报告

| 指标 | 口径 |
| --- | --- |
| QPS | 所有已尝试请求数 / 测量耗时，包含失败请求 |
| 成功吞吐量 | 成功请求与正常未命中之和 / 测量耗时 |
| 系统成功率 | 成功请求与正常未命中之和 / 总请求数 |
| 逻辑未命中 | GET/DELETE 返回 HTTP 404 且正文恰为 `NOT_FOUND\n` |
| 失败请求 | 网络错误，或 HTTP 状态/响应格式不符合操作契约 |
| 网络错误 | 客户端请求或读取响应失败，是失败请求的一部分 |
| 平均与 P50/P95/P99/P99.9 延迟 | 对全部测量请求统计，包含失败请求；不包含预热 |

报告还列出 HTTP 状态码分布。工具会同时检查状态码和响应正文；预热失败或测量中存在系统失败时以非零状态退出，正常未命中本身不会导致失败退出。完整实现见 [压测工具](../benmark/main.go)。

延迟保存在一个按请求编号索引的精确切片中，报告前排序计算分位数；该切片在 64 位环境中占约 `8 × requests` 字节，另有每个 worker 的局部计数。请求数很大时仍需为客户端准备足够内存。GET/DELETE 不生成写入 value，`value-size` 影响 PUT 和数据预置。

当前工具采用固定并发的闭环负载：每个 worker 等当前请求结束后才发送下一个请求。服务变慢时，客户端的发送速率也会下降；因此闭环 QPS 与 P99 不能单独证明系统在固定外部到达速率下的容量。评估过载行为还需要额外的固定到达速率实验，并联合观察排队、超时、拒绝和资源占用。当前工具未提供固定到达速率模式。

## 评估快照的代价

并发快照让快照文件写盘与普通请求、WAL 提交并行执行，但复制内存数据仍持有状态锁并额外占用一份数据集的内存。最终重写 WAL 后缀时还会串行化其他 WAL I/O，可靠模式请求可能等待该阶段完成。锁顺序与容量约束见 [设计说明](design.md#wal-io-与状态锁)。

先按[停机顺序](usage.md#停机备份与恢复)停止之前的网关和引擎，释放默认端口，再使用专门的实验目录启动一个快照周期较短的引擎：

```sh
MINIKV_DATA_DIR=./data-snapshot-bench \
MINIKV_WAL_MODE=reliable \
MINIKV_WAL_FLUSH_MS=2 \
MINIKV_SNAPSHOT_INTERVAL_MS=1000 \
./build/engine
```

另开终端启动网关并运行覆盖多个快照周期的负载。以相同初始数据和其他配置，分别比较自动快照关闭（`MINIKV_SNAPSHOT_INTERVAL_MS=0`）和开启的结果。至少记录数据集大小、快照耗时、进程内存峰值、成功吞吐量、错误率和尾延迟。快照次数与阶段累计耗时可以通过 [`/stats`](observability.md) 的前后样本计算增量；进程内存峰值仍需额外采集，当前压测报告没有这些指标。

一次实验仅改变一个主要变量，例如数据量、写入比例或快照周期，才能判断状态复制和 WAL 后缀重写的影响。并发设计本身不构成已有实测性能收益的证据。

## 历史数据与绘图

`benmark/out/` 中保留的现有数据与图表来自修复前版本，不代表当前版本的性能。展示新结果时，应同时提供提交、完整配置和原始报告。

[绘图脚本](../benmark/paint.py) 需要 Matplotlib。可以在单独的 Python 虚拟环境安装依赖，再从仓库根目录执行：

```sh
python3 -m venv /tmp/minikv-plot-venv
/tmp/minikv-plot-venv/bin/python -m pip install matplotlib

# 根据已有 CSV 绘图，不请求运行中的服务。
/tmp/minikv-plot-venv/bin/python benmark/paint.py

# 引擎和网关运行时，重新采样各操作比例并覆盖 CSV 与图表。
/tmp/minikv-plot-venv/bin/python benmark/paint.py --rerun
```

采样参数在脚本顶部定义；`--rerun` 会执行多轮完整压测，并预热各轮 key 空间。输出为 `benmark/out/mix_qps.csv` 和 `benmark/out/mix_qps_ternary.png`。图中采用报告的总 QPS，只展示操作比例与吞吐量的关系，不展示尾延迟、快照耗时或资源成本。
