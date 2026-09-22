# 数据容量控制的独立矩阵对照

[返回 README](../README.md) · [容量设计](design.md#数据集容量) · [旧中断实验](performance-data-capacity.md) · [实验工具](benchmark-experiments.md#数据容量配置)

**本次 48 轮全部通过有效性与覆盖核验，八组比较均有四个有效配对，未触发预声明的性能或 Engine RSS 调查条件。** 这只描述本次小数据集、16 B value、纯 PUT 闭环负载；部分单轮尾延迟增加超过 5%，不能据此宣称零开销、性能等价或所有负载均已通过性能验收。

本次新建计划和执行目录，复用历史二进制与 A-only 时长校准。[旧中断实验](performance-data-capacity.md)的 33 valid、1 incomplete、14 missing 及八组 inconclusive 保持原记录，其轮次没有加入本次比较。

## 结果与需要保留的差异

12 个 stage 全部正常结束，A/B/C 各 16 轮，共完成 **2,400 万次成功 PUT**，无失败或容量拒绝。每轮 settled 时均为 5,000 key、applied = durable = 500,000、WAL pending = 0；B/C 数据量均为 103,890 B，容量分别为 0/131,072 B。A 没有的新增统计字段保持 null。官方汇总为 48 valid，覆盖核验为 48 eligible，其余分类均为零。

每轮测量为 **15.996–57.324 s**，满足至少 10 s 的要求。24 个快照 ON 轮次的内部共同 idle 窗口完成 **14–56 次**快照，24 个 OFF 轮次增量均为零；有效内部 idle 样本至少 54 个，没有失败的 stats 查询。

下表为同一 block 配对比值的中位相对变化：`100 × (median(分子/分母) − 1)`，不是两组汇总值相除。每行均有四对；括号是越过不利方向阈值的对数：QPS `<0.95`，P99/P99.9 `>1.05`。至少三对越界才触发对应调查。QPS 增加表示吞吐提高，延迟增加表示变慢。

| 对照 | WAL 模式 | 快照 | QPS 变化（越界对数） | P99 变化（越界对数） | P99.9 变化（越界对数） |
| --- | --- | --- | --- | --- | --- |
| B/A | throughput | OFF | −0.84%（0/4） | +1.48%（0/4） | +1.49%（1/4） |
| B/A | throughput | ON | +0.51%（0/4） | −1.09%（1/4） | −2.11%（1/4） |
| B/A | reliable | OFF | +0.16%（0/4） | −1.09%（0/4） | −3.42%（0/4） |
| B/A | reliable | ON | +0.01%（0/4） | −0.65%（0/4） | +0.01%（0/4） |
| C/B | throughput | OFF | +0.62%（0/4） | −1.38%（0/4） | −2.31%（0/4） |
| C/B | throughput | ON | −0.06%（0/4） | +0.12%（0/4） | +0.89%（1/4） |
| C/B | reliable | OFF | −0.05%（0/4） | −0.05%（0/4） | +0.23%（1/4） |
| C/B | reliable | ON | −0.04%（0/4） | +0.46%（0/4） | −1.17%（0/4） |

八行的预声明判断均为 `not_triggered`。中位数没有消除以下四个越界位置，它们完整保留在原始记录中：

- **B/A，block 1，throughput/ON：** P99 从 2.346493 增至 2.672029 ms（+13.87%），P99.9 从 3.100853 增至 3.947332 ms（+27.30%）。
- **B/A，block 4，throughput/OFF：** P99.9 从 3.204249 增至 3.379024 ms（+5.45%）。
- **C/B，block 2，throughput/ON：** P99.9 从 3.168151 增至 3.483977 ms（+9.97%）。
- **C/B，block 2，reliable/OFF：** P99.9 从 8.525762 增至 9.133704 ms（+7.13%）。

每个对应指标仅一对越界，未达到三对要求。后续若要解释这些差异，应另行设计调查，不能从四个 block 或共享环境直接归因于某段源码，也不能以未触发为理由删除这些轮次。完整逐轮 QPS、分位数、最大延迟和全部配对比值见 [analysis.json](../benmark/baselines/2026-09-22-data-capacity-stagewise/analysis.json) 与 [CSV](../benmark/baselines/2026-09-22-data-capacity-stagewise/analysis.csv)。

## CPU、内存与最大延迟

CPU 是每轮**测量内部采样区间的平均占用核数**，下表给出各模式 24 轮的范围，不计算 CPU/op，也不把不同角色的采样窗口当作完全重合。

| WAL 模式 | Engine 核数 | Gateway 核数 | Benchmark 核数 |
| --- | --- | --- | --- |
| throughput | 1.184–1.234 | 2.650–2.765 | 1.958–2.048 |
| reliable | 0.448–0.460 | 0.814–0.854 | 0.581–0.615 |

Engine 内部采样 RSS 最大值为 5.051–6.012 MiB，Gateway 为 13.953–14.586 MiB，Benchmark 为 18.055–19.820 MiB。Engine 的 B/A 配对差值为 −112 至 −48 KiB，C/B 为 −32 至 +28 KiB；八组均没有同时增加超过 5% 且超过 1 MiB 的配对，RSS 诊断为 `not_triggered`。这些是样本差值，不证明容量代码降低了内存，也不是瞬时峰值上界。

throughput 各轮最大延迟为 9.206–18.258 ms，reliable 为 14.281–29.918 ms。全实验最大观测值 **29.918306 ms** 出现在 block 2 的 A、reliable/OFF；最大延迟与分位数分别保留，不用 P99.9 代替最大值。

## 对照对象与冻结方法

| 臂 | Engine | 逻辑容量 | 比较用途 |
| --- | --- | --- | --- |
| A | `9051e1b` | 无容量功能 | 修改前参考 |
| B | `479fe60` | 0，不限额 | B/A 观察该版本加入计量等改动后的总差异 |
| C | 与 B 相同的执行文件 | 131,072 B | C/B 观察启用且未触顶的容量检查 |

Gateway 和 Benchmark 固定为 `479fe60`，所有臂使用相同的 Go 文件。四个执行文件复用旧实验的冻结字节，经复制后核对哈希；本次没有重新构建。历史构建为 GNU C++ 13.3、RelWithDebInfo、`-O2 -g -DNDEBUG -std=c++17 -Wall -Wextra -Wpedantic`，Go 1.22.0、CGO_ENABLED=1、GOAMD64=v1，作为 prior 来源保存。

环境为 Xeon E3-1270 v3、8 个逻辑 CPU、Linux 6.8、NTFS3 数据盘；客户端、网关与引擎共用主机。结果只适用于这里记录的机器、构建与负载条件。

collector 使用独立 clone，冻结在 **`bc7e031`**，包含 `experiment.py`、`experiment_support.py`、`summarize.py`、`snapshot_report.py`、`process_guard.py` 五个 `benmark/` 模块。采集程序版本不代表被测引擎版本。

每轮 500,000 次 PUT、5,000 key、16 B value、seed 1，40 个客户端 worker、20 个引擎 worker、RPC pool 64、GOMAXPROCS 4；WAL batch 64、flush 2 ms。每轮从空库开始，初始填充后主要覆盖已有 key，使用新进程与新数据目录。ON 快照周期为 1,000 ms。请求集合实际为 103,890 B，按最长 key 计算的上界 105,000 B 也低于 C 的上限。

四个 block 的臂序为 **ABC / CBA / BCA / ACB**；奇数 block 先 throughput，偶数先 reliable，每种模式固定先 OFF 后 ON。每臂每 block 四轮，共 48 轮。同一对臂有两次正序、两次逆序，平均位置相同，但位置频次不完全平衡，且未随机化。

正式请求数沿用**历史 A-only 校准**：300,000 请求的最快轮为 10.743155048 s，按 `ceil(300000 × max(1, 15 / fastest_seconds) / 100000) × 100000` 取 500,000。本次没有新 pilot；历史校准、短验证及旧实验结果均不加入正式配对。计划在首轮前冻结，失败、偏慢或覆盖不足的位置不得替换。

每次 controller 仅执行下一 stage，失败或未完成 claim 会停止计划，不重试或跳过；实际进程身份、父死保护、锁继承与正常退出证据分别记录。此私有 controller 要求 Python 3.9+/Linux 5.3+ 及 pidfd，实际为 Python 3.12/Linux 6.8；细节见 [CONTROL.md](../benmark/baselines/2026-09-22-data-capacity-stagewise/CONTROL.md) 和 [REPRODUCE.md](../benmark/baselines/2026-09-22-data-capacity-stagewise/REPRODUCE.md)。

11 个阶段间隙为 **21.899–169.361 s**，由上一 stage 的 finished_at 到下一 stage 的 started_at 计算，不并入客户端测量时长。无服务运行的阶段间曾核对已完成前缀的完整性与覆盖，未依据性能阈值改变计划。采样期间未运行项目构建、测试或其他实验；没有声明主机隔离、CPU 绑核或温度/频率控制，间隙与正反顺序也不能排除宿主负载、设备和缓存差异。

## 判定口径与解释边界

有效性检查要求计数、配置、容量、WAL 序列与正常进程退出一致；覆盖另要求至少 10 s 测量、至少两个内部 idle stats 端点、ON 至少五次完成快照、OFF 增量为零。stats 查询必须 HTTP 200，且开始和结束都位于客户端测量区间内。状态每 250 ms、资源每 100 ms 采样，不能把 preload、排空或测量外样本计入内部覆盖。

每组必须四对均 eligible，少一对即 inconclusive。QPS、P99、P99.9 至少三对越过前述 5% 不利方向条件才触发调查；RSS 还要求绝对增加超过 1 MiB。这是工程调查规则，不是统计显著性检验或自动回退规则。未触发不证明性能等价或零开销。

CPU 使用身份一致样本的 `(Δuser_ticks + Δsystem_ticks) / clock_ticks_per_second / Δmonotonic_seconds`。UTC 用于对齐测量边界，CPU 时长用进程样本的 monotonic 时间。RSS 是测量内采样最大值，HWM 是已观察到的生命周期峰值；都不能代替未采样时刻的真实瞬时上界。

范围仅为小数据集、固定短 value 的纯 PUT 闭环流量，不覆盖 GET/DELETE 混合、大 value、热点分布、固定到达率过载、容量拒绝或慢存储。容量上限不是 RSS 上限，吞吐与尾延迟也不能外推为生产容量承诺。

## 原始证据与搬移复查

本次 plan SHA256 为 `145fccce834f09445e18f7ef9381dbeb5366e89937b81c23865a80b9a0211458`，driver 为 `6f42e783ee13974c28fe4a7578993b5e73f26ca7d0c72689f4b465971e978960`，auditor 为 `2be891e89620388e727460c384be9272795acbdecb8c1d7b0573209138f01afc`。原目录最终审计退出 0，48 valid/eligible、八组 not_triggered；结束后的 60 项哈希核对均匹配。

本次[原始归档](../benmark/baselines/2026-09-22-data-capacity-stagewise/data-capacity-stagewise.tar.gz)包含 619 个文件，原始大小 35,865,235 B、压缩大小 2,732,956 B；SHA256 为 `e05918769798088f1eafc214b0c77b51cdd642314b3819cddc9f115dee80827b`。[MANIFEST.json](../benmark/baselines/2026-09-22-data-capacity-stagewise/MANIFEST.json)逐文件记录大小与哈希，[SHA256SUMS](../benmark/baselines/2026-09-22-data-capacity-stagewise/SHA256SUMS)用于核对发布附件。

解包根名为 `data-capacity-stagewise/`；[audit_capacity.py](../benmark/baselines/2026-09-22-data-capacity-stagewise/audit_capacity.py)是包旁附件。归档保留冻结计划、原始 execution、controller 验证、来源和构建记录、各 stage 的 manifest/index/helper、全部报告、状态/资源样本及日志。analysis 与搬移检查是派生附件，不改写原始结果。

[独立搬移复查](../benmark/baselines/2026-09-22-data-capacity-stagewise/ISOLATED_RELOCATION_CHECK.json)已通过：实际解包的 619 个成员逐项核对路径、类型、大小与哈希，审计后原始成员未变；48 轮、32 个有效配对和八组比较的全部测量与判定字段均与原分析一致。检查禁止打开原实验目录，实际尝试为零；[检查脚本](../benmark/baselines/2026-09-22-data-capacity-stagewise/ISOLATED_RELOCATION_CHECK.py)与实际命令一并保存。

搬移后 12 个 stage 的 binary 验证均为 `recorded_hashes_only`，guard 均为 `copy_verified`：归档省略了执行文件，保留了助手源码副本。分析中仅有这些明确的验证等级与缺副本提示变化，数值、配对和判断不变。

从仓库根目录只做离线复查，不启动服务：

```bash
(cd benmark/baselines/2026-09-22-data-capacity-stagewise && sha256sum -c SHA256SUMS) || exit 1
capacity_extract="$(mktemp -d)"
tar -xzf benmark/baselines/2026-09-22-data-capacity-stagewise/data-capacity-stagewise.tar.gz -C "$capacity_extract"
mkdir "$capacity_extract/collector"
git archive bc7e031 benmark/experiment.py benmark/experiment_support.py \
  benmark/summarize.py benmark/snapshot_report.py benmark/process_guard.py \
  | tar -x -C "$capacity_extract/collector"
python3 benmark/baselines/2026-09-22-data-capacity-stagewise/audit_capacity.py \
  "$capacity_extract/data-capacity-stagewise" --repository "$capacity_extract/collector"
```

预期退出 0，输出 48 valid/eligible、0 inconclusive；在解包目录生成新的 analysis JSON/CSV。五个模块从冻结提交提取，避免后续源码变化影响历史复查，本地仓库须保有该提交。

省略历史 data、执行文件副本或源码构建目录的精简归档，不能用于再次验证缺失的二进制字节或恢复当时磁盘内容。只有记录哈希时应报告 `recorded_hashes_only`；guard 副本验证与 binary verification 分开，存在但不匹配必须判无效。重新构建的新文件不能代替历史执行文件证明。独立复现步骤见 [REPRODUCE.md](../benmark/baselines/2026-09-22-data-capacity-stagewise/REPRODUCE.md)，新复现应记录新二进制哈希、新路径和新计划。
