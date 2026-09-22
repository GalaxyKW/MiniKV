# 数据容量控制的版本与配置对照

[返回 README](../README.md) · [容量设计](design.md#数据集容量) · [实验工具](benchmark-experiments.md#数据容量配置)

后续已完成[独立的 48 轮单阶段对照](performance-data-capacity-stagewise.md)。本页保留旧实验的中断结果与原始证据，旧轮次未并入新实验。

**本轮没有完成预声明矩阵，尚不能判断容量功能的性能开销。** 48 个计划位置中，33 轮通过完整核验，1 轮缺少采样与收尾证据，14 轮未执行。八组比较均缺少要求的四个有效配对，结论为 `inconclusive`。已启动轮次保留原结果，未完成位置保持缺失分类。

本轮同时补齐了实验工具的 `--max-data-bytes` 参数和实际生效核验，已通过完整回归：7 个 C++ 测试程序、Go race、23 项集成测试、130 项实验测试及 18 项文档测试。功能验证与性能判断分开；下文保留这次未完成实验的全部已有证据。

## 对照对象与边界

| 臂 | Engine | 逻辑容量 | 比较用途 |
| --- | --- | --- | --- |
| A | `9051e1b` | 无容量功能 | 修改前参考 |
| B | `479fe60` | 0，不限额 | B/A 观察加入数据量计数等改动的总差异 |
| C | 与 B 相同的执行文件 | 131,072 B | C/B 观察启用且未触顶的容量检查 |

Gateway 和 Benchmark 均固定为 `479fe60`，三臂使用相同文件字节；采集程序为 `385bb6c`。A/B 的 C++ 编译器与参数相同：GNU C++ 13.3、RelWithDebInfo、`-O2 -g -DNDEBUG -std=c++17 -Wall -Wextra -Wpedantic`。二进制哈希和构建记录均随归档保存，采集程序版本不代表被测引擎版本。

每轮 500,000 次 PUT，5,000 key、16 B value、seed 1，40 个客户端 worker、20 个引擎 worker、RPC 池 64、GOMAXPROCS 4；WAL batch 64、flush 2 ms。分别运行 throughput/reliable 和快照 OFF/ON，ON 间隔为 1,000 ms。PUT 从空库开始，随后主要覆盖已有 key；每轮使用新进程与新数据目录。最终 key/value 合计为 103,890 B，按最长 key 计算的上界为 105,000 B，均低于 C 的上限。

共四个 block，臂顺序为 `ABC / CBA / BCA / ACB`，每臂每 block 含四轮，合计 48 轮。奇数 block 先 throughput，偶数先 reliable；同一模式固定先 OFF 后 ON。同一对臂有两次正序、两次逆序，平均位置相同，但位置频次未完全平衡，也未随机化。每个 mode×snapshot 内计算同一 block 的 B/A 与 C/B；完整比较要求四个有效配对，不使用两组中位数相除替代配对。

测量前仅用 A 进行时长校准。300,000 次请求的最快轮为 10.743155048 s，按提前保存的公式 `ceil(300000 × max(1, 15 / fastest_seconds) / 100000) × 100000`，正式请求数取 500,000。另用 C 做过真实配置短验证；这两批各四轮均不纳入性能比较。正式计划在首轮前冻结，失败、偏慢或覆盖不足的轮次均保留，不补跑。

环境为共享 Xeon E3-1270 v3、8 个逻辑 CPU、Linux 6.8、NTFS3 数据盘；客户端、网关和引擎同机。测量期间未运行项目构建、测试或其他实验。每阶段保存 CPU、可用内存、源码及二进制元数据；没有隔离外部负载，也没有逐轮完整记录外部进程的 CPU/IO 竞争。共享环境是解释边界，不能用于删除异常值或证明异常来自外部。

## 预声明的覆盖与调查条件

正式轮次必须完成全部 PUT、无失败或拒绝，settled 时 applied = durable = 500,000、WAL pending 为 0，三个子进程正常退出。B/C 的容量统计须符合配置，最终数据量为 103,890 B、拒绝计数为 0。A 缺少的新增字段保持缺失，不补成零。

每轮测量至少 10 s。只取请求开始和结束均在客户端测量区间内的成功 stats 查询，以最早和最晚的 `snapshot_in_progress=false` 样本作为共同端点；至少两个端点，ON 期间完成快照至少五次，OFF 完成增量为零。资源每 100 ms、状态每 250 ms 采样。

每个比较须有四个均满足覆盖的配对，任一不足则标为 inconclusive。至少三对 QPS 比值低于 0.95，或至少三对 P99/P99.9 比值高于 1.05，分别触发对应调查。Engine RSS 若至少三对同时增加超过 5% 和 1 MiB，触发单独的内存调查。这些是调查阈值，不是显著性检验或自动撤销规则；未触发只表示本轮没有达到阈值，不能证明零开销或性能等价。

CPU 核数按每个角色自身采样窗口的 `(Δuser_ticks + Δsystem_ticks) / clock_ticks_per_second / Δmonotonic_seconds` 计算。该窗口与完整请求集合不完全重合，因此不计算 CPU/op。RSS 为测量内部样本的最大值，HWM 是已观察到的进程生命周期峰值，二者均不能代替真实瞬时内存上界。

## 结果与后续判断

| 臂 | 计划 | 有效 | 收尾证据不足 | 未执行 |
| --- | --- | --- | --- | --- |
| A | 16 | 9 | 1 | 6 |
| B | 16 | 12 | 0 | 4 |
| C | 16 | 12 | 0 | 4 |

33 个有效轮次共完成 1,650 万次成功 PUT，最终均为 5,000 key，applied = durable = 500,000、WAL pending = 0，三个进程正常退出。B/C 的最终数据量均为 103,890 B、容量拒绝为 0。有效轮次时长为 16.274–90.550 s；ON 的内部共同 idle 窗口完成 16–87 次快照，OFF 完成增量均为 0。

| 对照 | WAL 模式 | 快照 | 有效配对 / 要求 | 判断 |
| --- | --- | --- | --- | --- |
| B/A | throughput | off | 3 / 4 | inconclusive |
| B/A | throughput | on | 2 / 4 | inconclusive |
| B/A | reliable | off | 2 / 4 | inconclusive |
| B/A | reliable | on | 2 / 4 | inconclusive |
| C/B | throughput | off | 3 / 4 | inconclusive |
| C/B | throughput | on | 3 / 4 | inconclusive |
| C/B | reliable | off | 3 / 4 | inconclusive |
| C/B | reliable | on | 3 / 4 | inconclusive |

逐轮 QPS、P99、P99.9、最大延迟、CPU/RSS 和配对比值见 [analysis.json](../benmark/baselines/2026-09-22-data-capacity/analysis.json) 与 [CSV](../benmark/baselines/2026-09-22-data-capacity/analysis.csv)。这些前缀数据用于检查与后续调查，不替代完整矩阵的性能验收。

外层执行会话返回 143，检查时 driver 和 runner 已不存在，而第九阶段的 Engine/Gateway 仍存活。原始 `execution.json`、阶段 index 和当前 result 停留在 `running`，没有改写成正常完成；独立的 `interruption-observation.json` 记录了检查和清理。根因未确定，不能据此断言是引擎缺陷、超时或某一种基础设施故障。

第九阶段 throughput/OFF 正常完成。其 ON 轮的客户端报告已经写完，记载 500,000 次 PUT 全部成功，但状态与资源采样提前停止，缺少 `stats-after.json`、`stats-settled.json` 及进程退出记录，因此仍为 incomplete。清理前核对了两个遗留进程的可执行文件路径、PID 和 starttime，再发送 SIGTERM；随后进程消失，但没有将未知退出码补记为 0。其后的两轮 reliable 和第十至十二阶段共 14 轮均未执行。

下一步应在能够持续运行完整计划的执行环境中另建实验，事先冻结计划，再重做完整矩阵。源码中可调查的工作量包括新增的数据量查找和 reliable 路径的正容量预检；现有前缀不能证明这些工作导致某个慢轮，因此本次没有据此更改引擎实现。

本轮为小数据集、固定值大小、纯 PUT 的闭环实验。它不覆盖 GET/DELETE 混合流量、大 value、热点分布、到达率过载、容量拒绝路径或慢存储；容量上限也不是 RSS 上限。吞吐和尾延迟仅描述已发出的请求，不能外推为生产容量承诺。

## 原始证据与搬移复查

[原始归档](../benmark/baselines/2026-09-22-data-capacity/data-capacity.tar.gz) 含 514 个文件，原始大小 29,196,038 B、压缩大小 2,222,012 B；SHA-256 为 `e3e600aaf91b2280f9499a3b897c0e0d3cf8e5bb748d464be4ba6ec920fcc55c`。[MANIFEST.json](../benmark/baselines/2026-09-22-data-capacity/MANIFEST.json) 逐文件记录大小与哈希。归档保存冻结的 plan/driver、原始 execution、每个已有阶段的全部报告与样本、构建来源、校准、短验证、完整回归日志、中断及清理记录；没有删除未完成轮的完整客户端报告。

省略的是 `data/`、执行文件副本和源码构建目录。搬移后只能检查记录的二进制哈希，验证级别为 `recorded_hashes_only`，不能重新验证历史执行文件字节或恢复当时磁盘内容。结束后的 31 项哈希检查确认，四个冻结文件及九个已启动阶段的执行文件副本均未变化。

[审计脚本](../benmark/baselines/2026-09-22-data-capacity/audit_capacity.py)、analysis JSON/CSV、搬移检查是事后派生附件。审计将执行矩阵的完整性与已完成轮次的有效性分开，四配对门槛保持不变；组内比值只纳入有效配对，`recorded_ratios` 另存所有可计算的原始比值。完整客户端报告不能单独使未完成轮变成有效轮。

从仓库根目录复查：

```sh
(cd benmark/baselines/2026-09-22-data-capacity && sha256sum -c SHA256SUMS)
capacity_extract="$(mktemp -d)"
tar -xzf benmark/baselines/2026-09-22-data-capacity/data-capacity.tar.gz -C "$capacity_extract"
capacity_collector="$capacity_extract/collector"
mkdir "$capacity_collector"
git archive 385bb6c benmark/experiment.py benmark/experiment_support.py \
  benmark/summarize.py benmark/snapshot_report.py | tar -x -C "$capacity_collector"
python3 benmark/baselines/2026-09-22-data-capacity/audit_capacity.py \
  "$capacity_extract/data-capacity" --repository "$capacity_collector"
```

本归档的审计命令预期退出 **1**，输出 33 valid、1 incomplete、14 missing、8 inconclusive，并在解压目录生成 `analysis.json` 和 `analysis.csv`；不会启动服务。命令从 `385bb6c` 提取冻结的四个采集模块，避免后续代码变更影响历史复查；本地仓库须保有该提交。

[实际压缩包搬移检查](../benmark/baselines/2026-09-22-data-capacity/RELOCATION_CHECK.json) 验证了全部 514 个成员，并确认逐轮、配对、判断及数量与原目录一致；[独立隔离检查](../benmark/baselines/2026-09-22-data-capacity/ISOLATED_RELOCATION_CHECK.json) 还禁止打开原始实验目录，结果同样一致。重新构建两个引擎、共用 Go 程序并生成新路径计划的步骤见 [REPRODUCE.md](../benmark/baselines/2026-09-22-data-capacity/REPRODUCE.md)。新实验使用新目录与新哈希，不恢复或改写本次中断的计划。
