#!/usr/bin/env python3
"""
Visualise the MapReduce results (objective 7).

Usage (from the project root):
    python3 src/visualize/plot_results.py [results.tsv] [run_log.txt] [output_dir]
Defaults: results/results.tsv  logs/run_log.txt  results/charts

results.tsv columns (tab separated), written by fraud_reducer.py:
    DIMENSION  BUCKET  total_txns  fraud_txns  fraud_rate_pct  avg_amount
run_log.txt is optional; it is only used for the shuffle-size chart.

Writes: fraud_by_hour.png, fraud_by_amount.png, combiner_shuffle.png
"""
import os
import re
import sys

import matplotlib
matplotlib.use("Agg")  # draw to files, no display needed
import matplotlib.pyplot as plt

BASE = "#4C78A8"       # normal bars
HIGHLIGHT = "#E45756"  # bars well above the overall fraud rate
AMOUNT_LABELS = ["< 1", "1-10", "10-50", "50-100", "100-500", "500-1000", "1000+"]

results_file = sys.argv[1] if len(sys.argv) > 1 else "results/results.tsv"
log_file = sys.argv[2] if len(sys.argv) > 2 else "logs/run_log.txt"
OUT_DIR = sys.argv[3] if len(sys.argv) > 3 else "results/charts"
os.makedirs(OUT_DIR, exist_ok=True)

hours, amounts = {}, {}
with open(results_file) as f:
    for line in f:
        p = line.rstrip("\n").split("\t")
        if len(p) != 6:
            continue
        dim, bucket = p[0], int(p[1])
        row = dict(total=int(p[2]), fraud=int(p[3]), rate=float(p[4]), avg=float(p[5]))
        (hours if dim == "HOUR" else amounts)[bucket] = row

all_rows = list(hours.values())
total_txns = sum(r["total"] for r in all_rows)
total_fraud = sum(r["fraud"] for r in all_rows)
overall = 100.0 * total_fraud / total_txns
print("Transactions: %d  Fraud: %d  Overall fraud rate: %.4f%%" % (total_txns, total_fraud, overall))


def style(ax):
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    ax.grid(axis="y", alpha=0.25)
    ax.set_axisbelow(True)


def bar_chart(data, labels, xlabel, title, filename):
    keys = sorted(data)
    rates = [data[k]["rate"] for k in keys]
    colors = [HIGHLIGHT if r > 3 * overall else BASE for r in rates]
    fig, ax = plt.subplots(figsize=(10, 4.8))
    bars = ax.bar([labels[k] for k in keys], rates, color=colors)
    ax.axhline(overall, color="#333333", linestyle="--", linewidth=1,
               label="overall fraud rate %.3f%%" % overall)
    ax.legend(frameon=False, loc="upper right", fontsize=9)
    for b, k in zip(bars, keys):  # fraud count above each bar
        ax.text(b.get_x() + b.get_width() / 2, b.get_height(), str(data[k]["fraud"]),
                ha="center", va="bottom", fontsize=7, color="#555555")
    ax.set_xlabel(xlabel)
    ax.set_ylabel("Fraud rate (% of transactions)")
    ax.set_title(title, loc="left", fontsize=12)
    ax.text(0.0, -0.22, "Number above each bar = fraud transactions. Red = more than 3x the overall rate.",
            transform=ax.transAxes, fontsize=8, color="#555555")
    style(ax)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, filename), dpi=150)
    plt.close(fig)
    print("wrote", filename)


hour_labels = {h: "%02d" % h for h in hours}
bar_chart(hours, hour_labels, "Hour of day since the first transaction (Time mod 24h; not clock time)",
          "Fraud rate by hour (2 days of European card transactions)", "fraud_by_hour.png")

amt_labels = {i: AMOUNT_LABELS[i] for i in amounts}
bar_chart(amounts, amt_labels, "Transaction amount band",
          "Fraud rate by transaction amount", "fraud_by_amount.png")

# Combiner effect, read from the Hadoop log: the 3 jobs print "Reduce shuffle bytes"
# in order clean, analysis WITH combiner, analysis WITHOUT combiner.
if log_file:
    try:
        text = open(log_file).read()
        vals = [int(v) for v in re.findall(r"Reduce shuffle bytes=(\d+)", text)]
        if len(vals) >= 3:
            with_c, without_c = vals[-2], vals[-1]
            fig, ax = plt.subplots(figsize=(6, 4.5))
            ax.bar(["Without Combiner", "With Combiner"], [without_c, with_c], color=[BASE, HIGHLIGHT])
            ax.set_yscale("log")
            for x, v in enumerate([without_c, with_c]):
                ax.text(x, v, "{:,} B".format(v), ha="center", va="bottom", fontsize=9)
            ax.set_ylabel("Reduce shuffle bytes (log scale)")
            ax.set_title("Data moved from map to reduce", loc="left", fontsize=12)
            style(ax)
            fig.tight_layout()
            fig.savefig(os.path.join(OUT_DIR, "combiner_shuffle.png"), dpi=150)
            plt.close(fig)
            print("wrote combiner_shuffle.png (%.0fx smaller with Combiner)" % (without_c / with_c))
        else:
            print("run_log.txt: expected 3 'Reduce shuffle bytes' lines, found", len(vals))
    except OSError as e:
        print("could not read", log_file, e)
