# 重建数据容量对照

以下命令用于**当前正式实验结束后的独立复现**，按顺序在同一 Bash 会话中执行。不要在正式采样期间构建、测试或启动另一组服务。所有输出放进新目录；不要重新执行原目录的 `run_plan.py`，也不要修改原 `plan.json` 或复用其绝对路径。

本次对照固定如下：A 引擎 `9051e1bddd996c192515780bcd4e1dfde384fdaa`；B/C 引擎及共用 gateway、bench 为 `479fe60b5caecd9fe685dc10ad3b9aeb0f3e4c5d`；collector 为 `385bb6c27bc0c0e4020783b866939039512475ee`。A/B 不设容量，C 为 131072 字节。4 个 block 的臂序为 ABC / CBA / BCA / ACB，奇数 block 先 throughput，偶数先 reliable；每种模式先 OFF 再 ON，共 48 轮。

## 1. 在新目录构建并冻结执行文件

需要 Linux amd64、Bash、Git、CMake、C++17 编译器、Python 3.8+ 和 Go **1.22.0**。本次 C++ 使用 `/usr/bin/c++`、`RelWithDebInfo`，实际参数为 `-O2 -g -DNDEBUG -std=c++17 -Wall -Wextra -Wpedantic`。先安排好 PATH 中的 Go 1.22.0；不要通过自动下载新工具链改变版本。不同主机、工具版本或构建路径可能生成不同哈希，必须记录实际的新哈希。

```bash
# 改为可读取上述三个提交的本地仓库、原始完整归档目录。
REPO=/absolute/path/to/MiniKV
ARCHIVE=/absolute/path/to/extracted/data-capacity
RUN_ROOT=$(mktemp -d /tmp/minikv-data-capacity-reproduction.XXXXXX)
export ARCHIVE RUN_ROOT
clean_env=(env -i PATH="$PATH" HOME="$HOME" LANG=C LC_ALL=C TZ=UTC GOTOOLCHAIN=local GOWORK=off GOENV=off)

# local clone 有独立 .git。不要改用 git worktree：Go 的 VCS 发现可能
# 穿过 worktree 的 .git 文件，错误记录外层仓库的 revision/dirty 状态。
for name in source-A source-B collector; do
  "${clean_env[@]}" git clone --quiet --no-hardlinks "$REPO" "$RUN_ROOT/$name" || exit 1
done
"${clean_env[@]}" git -C "$RUN_ROOT/source-A" checkout --quiet --detach 9051e1bddd996c192515780bcd4e1dfde384fdaa || exit 1
"${clean_env[@]}" git -C "$RUN_ROOT/source-B" checkout --quiet --detach 479fe60b5caecd9fe685dc10ad3b9aeb0f3e4c5d || exit 1
"${clean_env[@]}" git -C "$RUN_ROOT/collector" checkout --quiet --detach 385bb6c27bc0c0e4020783b866939039512475ee || exit 1

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
  "${clean_env[@]}" go version -m "$RUN_ROOT/frozen/gateway"
  "${clean_env[@]}" go version -m "$RUN_ROOT/frozen/bench"
} > "$RUN_ROOT/toolchain-and-go-build-info.txt"
```

继续前检查 `toolchain-and-go-build-info.txt`：两个 Go 文件都应记录 `go1.22.0`、`vcs.revision=479fe60b5caecd9fe685dc10ad3b9aeb0f3e4c5d`、`vcs.modified=false`，CGO_ENABLED=1、GOAMD64=v1。保留两个源码目录的 `build/CMakeCache.txt`、`build/CMakeFiles/{engine,minikv_storage}.dir/flags.make` 和链接参数；不要把 collector 自身的 CMake 缓存误当作 A/B 的构建证据。

## 2. 生成属于新目录的计划及来源记录

保留原实验的工作负载、臂序、门槛和 500000 请求数，仅重建路径、二进制来源与本次冻结时间。原 A-only 校准属于原实验；这里不伪称重新做过校准。若新主机的轮次不足 10 秒或 ON 测量窗口内不足 5 次快照，应保留并标为覆盖不足。若要重新校准，必须另建一份计划，在正式运行前冻结，不能替换已完成的短轮次。

```bash
cp "$ARCHIVE/run_plan.py" "$RUN_ROOT/run_plan.py"
"${clean_env[@]}" ARCHIVE="$ARCHIVE" RUN_ROOT="$RUN_ROOT" python3 - <<'PY'
from datetime import datetime, timezone
import hashlib, json, os
from pathlib import Path
import subprocess, sys

archive, root = Path(os.environ['ARCHIVE']).resolve(), Path(os.environ['RUN_ROOT']).resolve()
original = (archive / 'plan.json').read_bytes()
plan = json.loads(original)
digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
assert hashlib.sha256(original).hexdigest() == '42c9fb0a55e211be3aabb512821f23da86d52b3a73d93080da5a0049f0da7fd6'
assert digest(root/'run_plan.py') == plan['driver_sha256']
assert all(digest(root/'collector'/name) == expected for name, expected in plan['collector_files_sha256'].items())
sources = {
    'engine-A': root / 'source-A/build/engine',
    'engine-B': root / 'source-B/build/engine',
    'gateway': root / 'source-B/bin/minikv-go',
    'bench': root / 'source-B/bin/minikv-bench',
}
plan['binaries'] = {
    name: {'source': str(source), 'path': str(root / 'frozen' / name),
           'sha256': digest(root / 'frozen' / name), 'bytes': (root / 'frozen' / name).stat().st_size}
    for name, source in sources.items()
}
for name, source in sources.items():
    assert digest(source) == plan['binaries'][name]['sha256']
revisions = {}
for name, expected in [('source-A', plan['arms']['A']['revision']),
                       ('source-B', plan['arms']['B']['revision']),
                       ('collector', plan['collector_revision'])]:
    head = subprocess.check_output(['git', '-C', str(root/name), 'rev-parse', 'HEAD'], text=True).strip()
    clean = not subprocess.check_output(['git', '-C', str(root/name), 'status', '--porcelain'], text=True)
    assert head == expected and clean, (name, head, clean)
    revisions[name] = {'head': head, 'clean': clean}
plan['collector_files_sha256'] = {name: digest(root/'collector'/name) for name in plan['collector_files_sha256']}
plan['driver_sha256'] = digest(root / 'run_plan.py')
plan['frozen_at'] = datetime.now(timezone.utc).isoformat()
plan['reproduction_of'] = {'plan_sha256': hashlib.sha256(original).hexdigest(),
                           'calibration': 'Original A-only calibration; no new pilot was run; reuse 500000 requests'}
def write(name, body):
    with (root/name).open('x') as stream:
        stream.write(json.dumps(body, indent=2, allow_nan=False) + '\n')
write('binary-provenance.json', {'binaries': plan['binaries'], 'source_revisions': revisions})
sys.path.insert(0, str(root/'collector/benmark'))
from experiment_support import collect_metadata
write('preflight-metadata.json', collect_metadata(root/'collector', {name: root/'frozen'/name for name in sources}))
write('plan.json', plan)
(root/'plan.json').chmod(0o444)
print('New plan SHA256:', digest(root/'plan.json'))
PY
```

此步骤不是逐位重放原实验；它生成新来源记录和新 plan SHA。原归档 plan 的 SHA 为 `42c9fb0a55e211be3aabb512821f23da86d52b3a73d93080da5a0049f0da7fd6`。归档移动后，其中的旧绝对路径仍只是历史记录，不能直接当作本次执行路径。

## 3. 独占采样时段执行一次并离线复查

确认所有构建、测试及其他项目实验已结束。driver 顺序执行 12 个 stage，每个使用新进程和新数据目录；普通失败仍保留并继续后续 stage，不重跑失败轮次。中断后不要删除 `execution.json` 来续跑或从中挑选替代轮次。

```bash
"${clean_env[@]}" python3 "$RUN_ROOT/run_plan.py" "$RUN_ROOT" \
  --repository "$RUN_ROOT/collector" > "$RUN_ROOT/driver.log" 2>&1
driver_status=$?
echo "driver exit: $driver_status"

# 在 driver 完全结束后运行；非零退出仍保留生成的逐轮结果。
python3 "$RUN_ROOT/collector/benmark/summarize.py" "$RUN_ROOT"/stage-??-*/ \
  --format json > "$RUN_ROOT/summary.json"
python3 "$REPO/benmark/baselines/2026-09-22-data-capacity/audit_capacity.py" "$RUN_ROOT" \
  --repository "$RUN_ROOT/collector"
```

汇总 glob 末尾的 `/` 限定目录，避免把同名前缀的日志和 host 文件误作输入。它只列出已有目录；先对照 plan/execution 确认 12 个 stage 是否齐全，缺失 stage 需明确列为 missing。分析时列出所有 48 轮，逐 block 比较相同 WAL 模式、相同快照配置的 B/A 与 C/B；不要把不同 arm 合并成同一基线。`index.status=complete` 只说明计划跑完；失败、覆盖不足和来源不符都不能作为有效配对。门槛与配对规则以新 `plan.json` 为准。

## 环境与归档验证范围

原实验共机运行，未声明主机隔离、随机化、CPU 绑核或温度/频率控制；固定 seed 只固定请求内容，不能固定调度、缓存、设备和宿主负载。OFF 始终在 ON 前，位置计数不完全平衡；四个 block 的正反配对降低顺序偏差，但不能消除它。未触发预设调查门槛不代表零开销或性能等价。CPU 是采样窗口内平均占用核数；RSS、进程生命周期 HWM 和逻辑数据字节是不同量，不能互相替代。

完整保留 plan、driver、来源记录、工具版本与构建参数的文本副本、执行记录、各 stage 的 manifest/index、commands/result/report、全部 stats/resource JSONL、边界 stats 和日志。精简归档可以排除每轮 `data/`、每 stage 的 `binaries/`、顶层 `frozen/` 和源码/构建目录：

- 仍可用对应 collector 对归档的配置、状态、计数、采样和结果做离线一致性验证。省略**全部** stage 执行文件副本时显示 `recorded_hashes_only`；有文件但哈希不符会无效。
- 只有记录的哈希，无法重新核验当时执行文件的字节；重新构建得到的新文件也不能替代缺失的历史执行文件证明。
- 没有 `data/`，无法对当时的 WAL/快照做实际恢复或验证磁盘内容。stats 中的序列与容量只是已记录运行证据，不是重新恢复验证。
- 离线复查不会启动服务，也不保证原结果能在新机器逐值复现。pilot、smoke、覆盖不足和失败轮次均保留其原分类，pilot/smoke 不进入正式对照。
