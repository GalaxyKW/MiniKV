# 独立复现单阶段容量对照

本文件描述**本次正式采样全部结束后**的新一次独立复现。不要在采样期间执行下列构建、测试、分析或服务启动命令。新复现必须使用新目录、新计划和新的执行记录，不能重新启动本归档中已经登记的 stage。

本次实验原目录为 `/mnt/nvme/minikv-review/capacity-stagewise-iobxnqbx`，使用 `single-stage-v1` 控制器和独立的 `collector/` clone。collector 提交为 `bc7e031a962ff44367a160a5fc5adcda83ac1893`；A 引擎为 `9051e1bddd996c192515780bcd4e1dfde384fdaa`，B/C 引擎及共用 gateway、bench 为 `479fe60b5caecd9fe685dc10ad3b9aeb0f3e4c5d`。A/B 容量为 0（无限制），C 为 131072 字节。

本次 plan SHA256 为 `145fccce834f09445e18f7ef9381dbeb5366e89937b81c23865a80b9a0211458`；driver SHA256 为 `6f42e783ee13974c28fe4a7578993b5e73f26ca7d0c72689f4b465971e978960`，auditor SHA256 为 `2be891e89620388e727460c384be9272795acbdecb8c1d7b0573209138f01afc`。

旧中断实验 `/mnt/nvme/minikv-review/data-capacity-comparison-Q5TeKg` 的计划 SHA256 为 `42c9fb0a55e211be3aabb512821f23da86d52b3a73d93080da5a0049f0da7fd6`。它只提供历史 A-only 校准和构建来源；其采样结果不计入本次 48 轮，也不能用来补齐本次缺失位置。本次复用了经哈希核对的历史执行文件，未重新构建、未运行新 pilot。以下从源码构建的复现会产生新的二进制来源记录，不能宣称与历史文件逐字节相同。

## 1. 准备独立源码和执行文件

需要 Linux amd64、Bash、Git、CMake、C++17 编译器、Go **1.22.0**。本次私有 controller 还要求 **Python 3.9+、Linux 5.3+，以及可用的 `os.pidfd_open` / `signal.pidfd_send_signal`**。这不改变仓库通用 collector 的 Python 3.8+ 要求。本次实际 controller 环境是 Python 3.12/Linux 6.8。

历史 C++ 构建使用 `/usr/bin/c++`、`RelWithDebInfo`，参数为 `-O2 -g -DNDEBUG -std=c++17 -Wall -Wextra -Wpedantic`。先在 PATH 中安排 Go 1.22.0，不让 Go 自动下载其他工具链。主机、编译器和构建路径变化都可能改变二进制哈希，应保留本次真实构建信息。

下面命令按顺序用于一个**全新的**目录。`EVIDENCE` 指向仓库中本次实验的证据目录，`ARCHIVE` 指向原始包解出的 `data-capacity-stagewise/`，而非旧中断实验或只含摘要的目录。`audit_capacity.py` 是原始包旁的附件，位于 `EVIDENCE`，不在 tar 内。使用独立 clone，避免 worktree 的 `.git` 文件让 Go VCS 发现误指向外层仓库。

```bash
REPO=/absolute/path/to/MiniKV
EVIDENCE="$REPO/benmark/baselines/2026-09-22-data-capacity-stagewise"
ARCHIVE=/absolute/path/to/extracted/data-capacity-stagewise
RUN_ROOT=$(mktemp -d /tmp/minikv-capacity-stagewise-reproduction.XXXXXX)
export ARCHIVE RUN_ROOT
clean_env=(env -i PATH="$PATH" HOME="$HOME" LANG=C LC_ALL=C TZ=UTC GOTOOLCHAIN=local GOWORK=off GOENV=off)

for name in source-A source-B collector; do
  "${clean_env[@]}" git clone --quiet --no-hardlinks "$REPO" "$RUN_ROOT/$name" || exit 1
done
"${clean_env[@]}" git -C "$RUN_ROOT/source-A" checkout --quiet --detach 9051e1bddd996c192515780bcd4e1dfde384fdaa || exit 1
"${clean_env[@]}" git -C "$RUN_ROOT/source-B" checkout --quiet --detach 479fe60b5caecd9fe685dc10ad3b9aeb0f3e4c5d || exit 1
"${clean_env[@]}" git -C "$RUN_ROOT/collector" checkout --quiet --detach bc7e031a962ff44367a160a5fc5adcda83ac1893 || exit 1

for name in source-A source-B; do
  "${clean_env[@]}" cmake -S "$RUN_ROOT/$name/cpp_engine" -B "$RUN_ROOT/$name/build" \
    -DCMAKE_CXX_COMPILER=/usr/bin/c++ -DCMAKE_BUILD_TYPE=RelWithDebInfo -DBUILD_TESTING=OFF || exit 1
  "${clean_env[@]}" cmake --build "$RUN_ROOT/$name/build" --target engine -j2 || exit 1
done
mkdir -p "$RUN_ROOT/source-B/bin" "$RUN_ROOT/frozen"
(
  cd "$RUN_ROOT/source-B" || exit 1
  for item in 'go_server minikv-go' 'benmark minikv-bench'; do
    read -r package binary <<< "$item"
    "${clean_env[@]}" CGO_ENABLED=1 GOOS=linux GOARCH=amd64 GOAMD64=v1 \
      GOCACHE="$RUN_ROOT/go-cache" GOPATH="$RUN_ROOT/go-path" \
      go build -o "bin/$binary" "./$package" || exit 1
  done
) || exit 1
cp "$RUN_ROOT/source-A/build/engine" "$RUN_ROOT/frozen/engine-A"
cp "$RUN_ROOT/source-B/build/engine" "$RUN_ROOT/frozen/engine-B"
cp "$RUN_ROOT/source-B/bin/minikv-go" "$RUN_ROOT/frozen/gateway"
cp "$RUN_ROOT/source-B/bin/minikv-bench" "$RUN_ROOT/frozen/bench"
chmod 555 "$RUN_ROOT/frozen/"*
(
  cd "$RUN_ROOT/frozen" || exit 1
  sha256sum engine-A engine-B gateway bench
) > "$RUN_ROOT/SHA256SUMS"
{
  /usr/bin/c++ --version
  cmake --version
  "${clean_env[@]}" go version
  python3 --version
  uname -a
  "${clean_env[@]}" go version -m "$RUN_ROOT/frozen/gateway"
  "${clean_env[@]}" go version -m "$RUN_ROOT/frozen/bench"
} > "$RUN_ROOT/toolchain-and-go-build-info.txt"
```

继续前检查两个 Go 文件均记录 `go1.22.0`、`vcs.revision=479fe60b5caecd9fe685dc10ad3b9aeb0f3e4c5d`、`vcs.modified=false`、CGO_ENABLED=1 和 GOAMD64=v1。保留两个源码目录的 `build/CMakeCache.txt`、`build/CMakeFiles/{engine,minikv_storage}.dir/flags.make` 和链接参数。collector 自身的缓存不能证明 A/B 的实际构建配置。

## 2. 冻结属于新目录的计划

继承本次计划的工作负载、顺序和判断规则：500000 个纯 PUT、16 字节 value、5000 个 key、40 个客户端 worker、20 个引擎 worker、RPC pool 64、GOMAXPROCS=4、WAL batch 64/flush 2ms、ON 快照周期 1000ms。五个 collector 模块必须全部匹配：`experiment.py`、`experiment_support.py`、`summarize.py`、`snapshot_report.py`、`process_guard.py`，均位于 `benmark/`。

500000 请求数来自**历史 A-only 校准**。这里不新做 pilot；若新主机的测量不足 10 秒或 ON 内部窗口不足 5 次完成快照，应保留并标为覆盖不足，不替换短轮次。要重新校准，应在正式测量前另建并冻结计划，不能查看正式结果后调整请求数。

```bash
cp "$ARCHIVE/run_plan.py" "$RUN_ROOT/run_plan.py"
cp "$EVIDENCE/audit_capacity.py" "$RUN_ROOT/audit_capacity.py"
cp "$ARCHIVE/CONTROL.md" "$RUN_ROOT/CONTROL.md"
"${clean_env[@]}" ARCHIVE="$ARCHIVE" RUN_ROOT="$RUN_ROOT" python3 - <<'PY'
from datetime import datetime, timezone
import hashlib, json, os
from pathlib import Path
import subprocess, sys

archive = Path(os.environ['ARCHIVE']).resolve()
root = Path(os.environ['RUN_ROOT']).resolve()
original = (archive / 'plan.json').read_bytes()
plan = json.loads(original)
digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
expected_plan_sha256 = '145fccce834f09445e18f7ef9381dbeb5366e89937b81c23865a80b9a0211458'
assert hashlib.sha256(original).hexdigest() == expected_plan_sha256
assert plan['execution_protocol'] == 'single-stage-v1'
assert plan['policies']['stop_after_failed_stage'] is True
assert plan['collector_revision'] == 'bc7e031a962ff44367a160a5fc5adcda83ac1893'
modules = {'benmark/' + name for name in (
    'experiment.py', 'experiment_support.py', 'summarize.py',
    'snapshot_report.py', 'process_guard.py')}
assert set(plan['collector_files_sha256']) == modules
assert digest(root/'run_plan.py') == plan['driver_sha256']
assert digest(root/'audit_capacity.py') == plan['auditor_sha256']
assert all(digest(root/'collector'/name) == expected
           for name, expected in plan['collector_files_sha256'].items())
sources = {
    'engine-A': root / 'source-A/build/engine',
    'engine-B': root / 'source-B/build/engine',
    'gateway': root / 'source-B/bin/minikv-go',
    'bench': root / 'source-B/bin/minikv-bench',
}
plan['binaries'] = {
    name: {'source': str(source), 'path': str(root/'frozen'/name),
           'sha256': digest(root/'frozen'/name),
           'bytes': (root/'frozen'/name).stat().st_size}
    for name, source in sources.items()
}
for name, source in sources.items():
    assert digest(source) == plan['binaries'][name]['sha256']
for name in ('gateway', 'bench'):
    plan['binaries'][name]['go_build_info'] = subprocess.check_output(
        ['go', 'version', '-m', str(root/'frozen'/name)], text=True)
revisions = {}
for name, expected in [('source-A', plan['arms']['A']['revision']),
                       ('source-B', plan['arms']['B']['revision']),
                       ('collector', plan['collector_revision'])]:
    head = subprocess.check_output(
        ['git', '-C', str(root/name), 'rev-parse', 'HEAD'], text=True).strip()
    clean = not subprocess.check_output(
        ['git', '-C', str(root/name), 'status', '--porcelain', '--untracked-files=all'], text=True)
    assert head == expected and clean, (name, head, clean)
    revisions[name] = {'head': head, 'clean': clean}
plan['collector_files_sha256'] = {
    name: digest(root/'collector'/name) for name in sorted(modules)}
plan['driver_sha256'] = digest(root/'run_plan.py')
plan['auditor_sha256'] = digest(root/'audit_capacity.py')
plan['frozen_at'] = datetime.now(timezone.utc).isoformat()
plan['reproduction_of'] = {
    'plan_sha256': expected_plan_sha256,
    'calibration': 'Historical A-only calibration inherited; no new pilot; reuse 500000 requests',
    'historical_calibration_plan_sha256':
        '42c9fb0a55e211be3aabb512821f23da86d52b3a73d93080da5a0049f0da7fd6',
    'experiment': 'Independent source rebuild with new binary hashes and new execution evidence',
}
def write(name, body):
    with (root/name).open('x') as stream:
        stream.write(json.dumps(body, indent=2, allow_nan=False) + '\n')
write('binary-provenance.json', {'binaries': plan['binaries'], 'source_revisions': revisions})
sys.dont_write_bytecode = True
sys.path.insert(0, str(root/'collector/benmark'))
from experiment_support import collect_metadata
write('preflight-metadata.json', collect_metadata(
    root/'collector', {name: root/'frozen'/name for name in sources}))
write('plan.json', plan)
(root/'plan.json').chmod(0o444)
print('New plan SHA256:', digest(root/'plan.json'))
PY
```

生成后保留打印出的**新 plan SHA**。冻结后不要再改请求数、顺序、模块或二进制。不要从归档复制 `execution.json`、`execution.lock`、stage 目录或执行日志到新目录。归档中的旧绝对路径仅作为历史记录，不能用作新一次执行的输入路径。

## 3. 按冻结顺序逐阶段调用

采样开始前结束所有项目构建、测试和其他实验。每次调用只执行下表的一行，等待本次 controller 真实退出并核实成功后，再以**新的单阶段调用**推进下一行。不要把 12 阶段重新包进一个长寿命总控进程。

| 次序 | block | `--stage` | 模式顺序 |
| --- | --- | --- | --- |
| 1 | 1 | `stage-01-A` | throughput → reliable |
| 2 | 1 | `stage-02-B` | throughput → reliable |
| 3 | 1 | `stage-03-C` | throughput → reliable |
| 4 | 2 | `stage-04-C` | reliable → throughput |
| 5 | 2 | `stage-05-B` | reliable → throughput |
| 6 | 2 | `stage-06-A` | reliable → throughput |
| 7 | 3 | `stage-07-B` | throughput → reliable |
| 8 | 3 | `stage-08-C` | throughput → reliable |
| 9 | 3 | `stage-09-A` | throughput → reliable |
| 10 | 4 | `stage-10-A` | reliable → throughput |
| 11 | 4 | `stage-11-C` | reliable → throughput |
| 12 | 4 | `stage-12-B` | reliable → throughput |

每种模式内均为快照 OFF → ON，每 stage 四轮、全计划 48 轮。每轮使用新进程和新数据。以下是**单个 stage** 的调用示例；更换 shell 会话后重新设置 `RUN_ROOT` 和 `clean_env`，不要重复初始化目录或生成计划。

```bash
STAGE=stage-01-A  # 下一次独立调用时，按表改为尚未登记的下一 stage。
set -o noclobber
"${clean_env[@]}" python3 "$RUN_ROOT/run_plan.py" "$RUN_ROOT" \
  --repository "$RUN_ROOT/collector" --stage "$STAGE" \
  > "$RUN_ROOT/$STAGE-controller.log" 2>&1
controller_status=$?
echo "controller exit: $controller_status"
```

controller 每次重新验证 collector HEAD/clean、五个模块、driver 和四个执行文件的哈希。永久 inode 的 `execution.lock` 使用非阻塞排他锁，runner 继承锁描述符；controller 只关闭自己的副本。controller 在启动 runner 前先持久写入唯一 claim，再记录实际 PID、boot ID、starttime、exe/cmdline、guard 前缀和退出结果。`host-before` 保留每阶段开始时的环境记录。

推进必须同时具备：controller 对自己的 runner 调用 `wait()` 得到 0；index 是按序完成的四轮，每轮 `ok` 且三个角色均正常退出、未 forced；实际 `/proc` 检查确认没有遗留私有进程。阶段 1–11 成功后总体状态仍是 `running`，第 12 阶段成功后才是 `complete`；不要把中间的 `running` 自动解释为仍有进程存活。

**任何失败、interrupted 或未完成 claim 都停止该计划，不能重试或跳过。** 不得删除、改写 execution、lock、目录或日志以继续。工具等待超时只说明观察未结束，应继续等待原工具句柄并核对实际 `/proc/PID/{stat,exe,cmdline}` 身份，不能仅根据 JSON 判断进程已死或据此重启。

冻结的 `process_guard.py` 建立 controller → runner → 直接服务的父死保护。正常中断时 controller 先 TERM runner，给予有界清理时间，超时后 KILL 并等待；SIGKILL 父死保护不保证 WAL 排空，也不能充当正常退出证据。控制器停止后仍应保留不完整行，不能拿历史实验或新轮次替补。

## 4. 停止采样后做离线复查

以下完整结果汇总用于全部阶段结束或计划已停止，且确认没有存活的私有实验进程后。阶段间确认无运行服务时，也可仅用 auditor 核对已完成前缀的来源、完整性与覆盖；本次曾在首阶段和首 block 的停机间隙，分别确认 4 轮和 12 轮 eligible。这不允许依据性能阈值改变计划、顺序、请求数或替换轮次。失败或缺失不应被删除；`audit_capacity.py` 从计划列出全部 48 个位置，并区分有效的已完成前缀与整体配对不可分类。

```bash
python3 "$RUN_ROOT/collector/benmark/summarize.py" "$RUN_ROOT"/stage-??-*/ \
  --format json > "$RUN_ROOT/summary.json"
python3 "$RUN_ROOT/audit_capacity.py" "$RUN_ROOT" \
  --repository "$RUN_ROOT/collector"
```

汇总 glob 末尾 `/` 仅匹配目录，避免同名前缀日志和 host 文件混入。若一个 stage 都未启动，跳过上面的目录汇总，仍保留计划和失败证据。auditor 生成 `analysis.json` / `analysis.csv`；它校验单阶段顺序、来源、launcher/guard、测量边界、成功计数、容量、WAL 序列及采样覆盖。执行文件验证和 `process_guard_verification` 分开报告。

逐 block 在相同 WAL 模式、相同快照配置内比较 B/A 与 C/B。每个比较组必须四对均 eligible 才能判断预设规则：QPS 至少 3/4 比值 `<0.95`，或 P99/P99.9 至少 3/4 `>1.05`，触发调查；RSS 至少 3/4 同时增加超过 5% 和 1MiB，仅作诊断。覆盖不足时不可宣称通过；未触发也不代表性能等价或零开销。CPU 只报告内部采样区间的平均占用核数，不推断 CPU/op。

## 5. 保留证据与解释范围

保留 plan、controller、auditor、五模块哈希、来源与工具版本/构建参数、执行记录、控制器验证记录、各 stage 的 manifest/index、commands/result/report、helper 副本、全部 stats/resource JSONL、边界 stats 和日志。原始记录中的绝对路径保留历史含义；离线 auditor 按归档内相对位置寻找副本，不要求原机器路径存在。

精简归档可以省略数据目录、stage 执行文件副本、顶层 frozen 和源码/构建目录，但需明确：仅记录哈希时无法重新核对当时执行文件字节；重新构建的新文件不能补充为历史文件证明。guard 副本缺失可显示 `recorded_hashes_only`，存在但哈希不符会使证据无效。没有原 data 目录就不能做当时 WAL/快照的实际恢复；stats 是已记录运行证据。

主机没有声明隔离、随机化、CPU 绑核或温度/频率控制。固定 seed 只固定请求内容；阶段间等待时间应保留，不能并入测量时长。OFF 总在 ON 前，四 block 的正反配对不能消除所有顺序、宿主负载或缓存差异。原始观测可支持进一步调查，不能单凭阈值认定代码因果关系；本次和旧中断实验始终分别分析。
