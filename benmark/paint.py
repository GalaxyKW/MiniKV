#!/usr/bin/env python3
import csv
import math
import re
import subprocess
import sys
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib import font_manager
import matplotlib.tri as mtri

ROOT = Path(__file__).resolve().parents[1]
BIN = ROOT / "bin" / "minikv-bench"
OUT_DIR = ROOT / "benmark" / "out"
OUT_DIR.mkdir(parents=True, exist_ok=True)

URL = "http://127.0.0.1:8080/kv"
WORKERS = 20
REQUESTS = 400000
KEYSPACE = 20000
STEP = 5  # 占比步长，越小采样越密
CONTOUR_LEVELS = 40  # 平滑色带层数

QPS_RE = re.compile(r"QPS\s*:\s*([0-9.]+)")


def setup_chinese_font() -> None:
    # Prefer loading concrete font files first to avoid font-name alias mismatch.
    file_candidates = [
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
        "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
        "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
    ]

    chosen_name = None
    for font_path in file_candidates:
        p = Path(font_path)
        if not p.exists():
            continue
        try:
            font_manager.fontManager.addfont(str(p))
            chosen_name = font_manager.FontProperties(fname=str(p)).get_name()
            break
        except Exception:
            continue

    if chosen_name is None:
        # Fallback by font family name if file-based loading is unavailable.
        name_candidates = [
            "Noto Sans CJK SC",
            "Noto Sans CJK JP",
            "Noto Sans CJK TC",
            "Source Han Sans SC",
            "WenQuanYi Micro Hei",
            "WenQuanYi Zen Hei",
            "SimHei",
            "Microsoft YaHei",
            "PingFang SC",
            "Arial Unicode MS",
        ]
        installed = {f.name for f in font_manager.fontManager.ttflist}
        for name in name_candidates:
            if name in installed:
                chosen_name = name
                break

    if chosen_name:
        plt.rcParams["font.family"] = [chosen_name]
        plt.rcParams["font.sans-serif"] = [chosen_name, "DejaVu Sans"]

    plt.rcParams["axes.unicode_minus"] = False

def run_once(put_ratio: int, del_ratio: int):
    cmd = [
        str(BIN),
        "-url", URL,
        "-workers", str(WORKERS),
        "-requests", str(REQUESTS),
        "-op", "mixed",
        "-keyspace", str(KEYSPACE),
        "-write-ratio", str(put_ratio),
        "-delete-ratio", str(del_ratio),
        "-preload=true",
        "-preload-count", str(KEYSPACE),
        "-seed=1",
        "-value-size=128",
    ]
    out = subprocess.check_output(cmd, text=True, cwd=str(ROOT))
    m = QPS_RE.search(out)
    if not m:
        raise RuntimeError(f"未解析到 QPS，输出如下:\n{out}")
    return float(m.group(1))


def load_rows_from_csv(csv_path: Path):
    rows = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(
                (
                    int(row["put_ratio"]),
                    int(row["get_ratio"]),
                    int(row["del_ratio"]),
                    float(row["qps"]),
                )
            )
    return rows

def bary_to_xy(put, get, dele):
    # 顶点定义：PUT 在上，GET 左下，DEL 右下
    # PUT=(0.5, sqrt(3)/2), GET=(0,0), DEL=(1,0)
    s = put + get + dele
    x = (put * 0.5 + get * 0.0 + dele * 1.0) / s
    y = (put * (math.sqrt(3) / 2.0)) / s
    return x, y

def main():
    setup_chinese_font()

    csv_path = OUT_DIR / "mix_qps.csv"
    rerun = "--rerun" in sys.argv

    if rerun:
        rows = []
        for put in range(0, 101, STEP):
            for dele in range(0, 101 - put, STEP):
                get = 100 - put - dele
                qps = run_once(put, dele)
                rows.append((put, get, dele, qps))
                print(f"PUT={put:3d}% GET={get:3d}% DEL={dele:3d}% -> QPS={qps:.2f}")

        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["put_ratio", "get_ratio", "del_ratio", "qps"])
            w.writerows(rows)
        print(f"已更新数据: {csv_path}")
    else:
        if not csv_path.exists():
            raise FileNotFoundError(
                f"未找到已有数据文件: {csv_path}。"
                "请先执行一次 `python3 benmark/paint.py --rerun` 生成数据。"
            )
        rows = load_rows_from_csv(csv_path)
        print(f"已从 CSV 读取数据: {csv_path}")

    xs, ys, cs = [], [], []
    for put, get, dele, qps in rows:
        x, y = bary_to_xy(put, get, dele)
        xs.append(x)
        ys.append(y)
        cs.append(qps)

    plt.figure(figsize=(8, 7))
    triang = mtri.Triangulation(xs, ys)
    contour = plt.tricontourf(triang, cs, levels=CONTOUR_LEVELS, cmap="viridis")
    plt.colorbar(contour, label="QPS")

    # 三角边界
    tri_x = [0, 1, 0.5, 0]
    tri_y = [0, 0, math.sqrt(3)/2, 0]
    plt.plot(tri_x, tri_y, "k-", linewidth=1.5)

    # 顶点文字
    plt.text(0.5, math.sqrt(3)/2 + 0.03, "PUT 100%", ha="center")
    plt.text(-0.03, -0.03, "GET 100%", ha="right")
    plt.text(1.03, -0.03, "DEL 100%", ha="left")

    plt.title(f"MiniKV 混合负载占比 vs QPS（三元图，步长={STEP}%）")
    plt.axis("off")

    png_path = OUT_DIR / "mix_qps_ternary.png"
    plt.savefig(png_path, dpi=180, bbox_inches="tight")
    print(f"已输出: {png_path}")

if __name__ == "__main__":
    main()
