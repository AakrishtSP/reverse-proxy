#!/usr/bin/env python3
"""Regenerate the benchmark charts embedded in README.md.

Reads bench/results.jsonl, writes PNGs to analysis/charts/.
Run:  .venv/bin/python analysis/make_charts.py
"""
import json
import os
import collections
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS = os.path.join(HERE, "..", "bench", "results.jsonl")
OUTDIR = os.path.join(HERE, "charts")
os.makedirs(OUTDIR, exist_ok=True)

rows = [json.loads(l) for l in open(RESULTS) if l.strip()]

# best req/s per (stage, mode, concurrency)
best = collections.defaultdict(float)
for r in rows:
    k = (r["stage"], r["mode"], r["concurrency"])
    best[k] = max(best[k], float(r["req_per_sec"]))

CONCS = [10, 50, 100, 200, 500, 1000]


def series(stage, mode):
    return [best.get((stage, mode, c), float("nan")) for c in CONCS]


plt.rcParams.update({"figure.figsize": (10, 6), "figure.dpi": 110})


def curve_chart(mode, stages, fname, title):
    plt.figure()
    for stage in stages:
        plt.plot(CONCS, series(stage, mode), marker="o", label=stage)
    plt.xscale("log")
    plt.yscale("log")
    plt.xticks(CONCS, [str(c) for c in CONCS])
    plt.xlabel("concurrency (wrk -c)")
    plt.ylabel("req/s (log scale)")
    plt.title(title)
    plt.grid(True, which="both", alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(OUTDIR, fname))
    plt.close()


HEADLINES = [
    "05-pool-16",
    "17-epoll-loops-8",
    "22-io-uring-loops-8",
]
curve_chart("open", HEADLINES, "throughput-open.png",
            "Keep-alive clients: req/s vs concurrency (fastest config per kind)")
curve_chart("close", HEADLINES, "throughput-close.png",
            "One-shot clients: req/s vs concurrency (fastest config per kind)")


def best_chart(fname):
    kinds = [
        ("threaded\n(05-pool-16)", "05-pool-16"),
        ("epoll\n(17-loops-8)", "17-epoll-loops-8"),
        ("io_uring\n(22-loops-8)", "22-io-uring-loops-8"),
    ]
    labels, opens, closes = [], [], []
    for label, stage in kinds:
        labels.append(label)
        opens.append(max(best.get((stage, "open", c), 0) for c in CONCS))
        closes.append(max(best.get((stage, "close", c), 0) for c in CONCS))
    x = range(len(labels))
    w = 0.35
    plt.figure()
    plt.bar([i - w / 2 for i in x], opens, w, label="open (keep-alive)")
    plt.bar([i + w / 2 for i in x], closes, w, label="close (one-shot)")
    plt.xticks(list(x), labels)
    plt.ylabel("best req/s (log scale)")
    plt.yscale("log")
    plt.title("Fastest config per server kind, either workload")
    plt.legend()
    for i, (o, c) in enumerate(zip(opens, closes)):
        plt.text(i - w / 2, o * 1.08, f"{o:,.0f}", ha="center", fontsize=9)
        plt.text(i + w / 2, c * 1.08, f"{c:,.0f}", ha="center", fontsize=9)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTDIR, fname))
    plt.close()


best_chart("best-by-kind.png")


def loops_chart(fname):
    cfgs = [("10-epoll-both", 1), ("16-epoll-loops-2", 2),
            ("14-epoll-loops-4", 4), ("17-epoll-loops-8", 8)]
    xs = [n for _, n in cfgs]
    plt.figure()
    for mode in ("open", "close"):
        ys = [best.get((s, mode, 100), 0) or 0 for s, _ in cfgs]
        plt.plot(xs, ys, marker="o", label=mode)
    plt.xlabel("epoll loops (SO_REUSEPORT)")
    plt.ylabel("req/s @ c100 (log scale)")
    plt.yscale("log")
    plt.xticks(xs, [str(n) for n in xs])
    plt.title("epoll scales with loops while cores last")
    plt.grid(True, which="both", alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(OUTDIR, fname))
    plt.close()


loops_chart("epoll-loops-scaling.png")
print("wrote:", sorted(os.listdir(OUTDIR)))
