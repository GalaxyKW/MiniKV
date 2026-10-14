# 复核与独立复现

本目录记录 2026-10-14 的 16 轮实验，结论为未达到采用门槛。原始采样在 `snapshot-vector.tar.gz` 中，顶层为 `snapshot-vector/`；包旁 JSON 和分析脚本是派生产物。

## 只读复核归档

从仓库根目录运行，需要 Python 3.8+。输出使用新目录，避免覆盖其他结果：

```bash
EVIDENCE="$PWD/benmark/baselines/2026-10-14-snapshot-vector"
REVIEW_ROOT=$(mktemp -d /tmp/minikv-vector-review.XXXXXX)
(cd "$EVIDENCE" && sha256sum -c SHA256SUMS)
tar -xzf "$EVIDENCE/snapshot-vector.tar.gz" -C "$REVIEW_ROOT"
python3 "$EVIDENCE/analyze.py" "$REVIEW_ROOT/snapshot-vector" \
  --repo "$PWD" --storage-checks "$EVIDENCE/post-run-checks.json" \
  --output-dir "$REVIEW_ROOT/derived"
```

分析退出 0 表示原始记录、覆盖与所提供历史检查附件有效，**不表示采用门槛通过**。本批应得到 16 valid、8 项阈值越界、`adoption=not_adopted`。缺失或无效记录返回 1；参数错误返回 2。

归档不包含 `data/`、执行文件副本或源码构建目录。搬移后的 binary verification 为 `recorded_hashes_only`，不是重新验证历史执行文件；process guard 源码副本仍可核验。`post-run-checks.json` 记录原位置的 32 次完整检查，分析器不会再次调用引擎，也不会把缺失的数据目录当成已扫描。保留完整原始目录及二进制时，才可用 `post_run_checks.py --root <完整目录> --source-root <对应源码>` 重新检查。

## 新一次测量

需 Linux、C++17/CMake、Go 1.22.0、Python 3.8+。以下建立新源码、新执行文件、新计划，不恢复或补跑本批任何位置。先完成所有构建和验证，再开始采样；新主机可能不满足 20 秒/3 次快照的覆盖要求，应保留为覆盖不足，不能事后修改本计划或替换轮次。

```bash
set -euo pipefail
REPO="$PWD"
EVIDENCE="$REPO/benmark/baselines/2026-10-14-snapshot-vector"
RUN_ROOT=$(mktemp -d /tmp/minikv-vector-new.XXXXXX)
export REPO EVIDENCE RUN_ROOT
for variant in A B; do
  git clone --quiet --no-hardlinks "$REPO" "$RUN_ROOT/source-$variant"
  git -C "$RUN_ROOT/source-$variant" checkout --quiet --detach 05acff5cc33a67a9201e072c4b5de0996c6d2197
  cmake -S "$RUN_ROOT/source-$variant/cpp_engine" -B "$RUN_ROOT/source-$variant/build" \
    -DCMAKE_CXX_COMPILER=/usr/bin/c++ -DCMAKE_BUILD_TYPE=RelWithDebInfo -DBUILD_TESTING=ON
done
git -C "$RUN_ROOT/source-B" apply "$EVIDENCE/prototype.patch"
make -C "$RUN_ROOT/source-A" BUILD_TYPE=RelWithDebInfo BUILD_DIR=build all
make -C "$RUN_ROOT/source-B" BUILD_TYPE=RelWithDebInfo BUILD_DIR=build test
make -C "$RUN_ROOT/source-B" sanitize-test
mkdir "$RUN_ROOT/frozen" "$RUN_ROOT/prior"
cp "$RUN_ROOT/source-A/build/engine" "$RUN_ROOT/frozen/engine-A"
cp "$RUN_ROOT/source-B/build/engine" "$RUN_ROOT/frozen/engine-B"
cp "$RUN_ROOT/source-A/bin/minikv-go" "$RUN_ROOT/frozen/gateway"
cp "$RUN_ROOT/source-A/bin/minikv-bench" "$RUN_ROOT/frozen/bench"
chmod 555 "$RUN_ROOT/frozen/"*
tar -xzf "$EVIDENCE/snapshot-vector.tar.gz" -C "$RUN_ROOT/prior"
cp "$RUN_ROOT/prior/snapshot-vector/run_plan.py" "$RUN_ROOT/run_plan.py"
python3 - <<'PY'
from datetime import datetime, timezone
import hashlib, json, os
from pathlib import Path
root = Path(os.environ['RUN_ROOT'])
evidence = Path(os.environ['EVIDENCE'])
plan = json.loads((root / 'prior/snapshot-vector/plan.json').read_text())
digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
source = root / 'source-A'
plan['frozen_at'] = datetime.now(timezone.utc).isoformat()
plan['prototype_revision'] = 'uncommitted baseline + patch SHA256 ' + digest(evidence / 'prototype.patch')
plan['collector'] = str(source / 'benmark/experiment.py')
plan['engines'] = {v: str(root / ('frozen/engine-' + v)) for v in 'AB'}
plan['gateway'], plan['bench'] = (str(root / ('frozen/' + v)) for v in ('gateway', 'bench'))
paths = [root / ('frozen/' + v) for v in ('engine-A', 'engine-B', 'gateway', 'bench')]
paths += [source / ('benmark/' + v) for v in (
    'experiment.py', 'experiment_support.py', 'summarize.py', 'snapshot_report.py', 'process_guard.py')]
paths += [root / 'run_plan.py']
plan['frozen_files'] = [{'path': str(p), 'sha256': digest(p)} for p in paths]
(root / 'plan.json').write_text(json.dumps(plan, indent=2) + '\n')
(root / 'PLAN_SHA256').write_text(digest(root / 'plan.json') + '  plan.json\n')
PY
python3 "$RUN_ROOT/run_plan.py" > "$RUN_ROOT/controller.log" 2>&1
```

driver 已运行过的目录不可再次启动。保留全部计划位置和错误；失败时先核对仍存活的原进程及日志，不因观察超时启动替代轮次。每个 collector 限制单轮预算并回收服务进程，driver 在失败 stage 后停止。

采样全部结束后，用新目录的实际二进制、数据和当前 PID 状态执行后验检查，再分析：

```bash
python3 "$EVIDENCE/post_run_checks.py" --root "$RUN_ROOT" \
  --source-root "$RUN_ROOT/source-A" --output "$RUN_ROOT/post-run-checks.json"
python3 "$EVIDENCE/analyze.py" "$RUN_ROOT" --repo "$RUN_ROOT/source-A" \
  --storage-checks "$RUN_ROOT/post-run-checks.json" --output-dir "$RUN_ROOT/analysis"
```

新构建路径、编译器及主机状态可改变二进制哈希和性能，不应声称新测量逐字节复现历史执行文件。正式归档前还应保存新构建参数、工具链与验证日志；上述命令不承诺新实验会通过性能采用门槛。
