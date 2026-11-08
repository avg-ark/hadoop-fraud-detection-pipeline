#!/usr/bin/env python3
"""
Ranking and reliability step (statistical summary of the MapReduce output).

Usage (from the project root):
    python3 src/analysis/rank_results.py [results/results.tsv] [output.tsv]
Defaults: results/results.tsv  ->  results/ranking.tsv

For each dimension (HOUR, AMT) the buckets are ranked by fraud rate. Small buckets give noisy
rates, so every bucket also gets a 95% Wilson confidence interval for its fraud rate and a
verdict:
    FEW CASES  fewer than MIN_FRAUD fraud cases -> rate too uncertain to draw a conclusion
    HIGHER     enough cases and the interval lies entirely ABOVE the overall rate
               -> reliably riskier than average
    LOWER      enough cases and the interval lies entirely BELOW the overall rate
    AVERAGE    enough cases, but the interval contains the overall rate
"""
import math
import sys

MIN_FRAUD = 20      # minimum fraud cases before a bucket's rate is considered stable
Z = 1.96            # 95% confidence

AMOUNT_LABELS = ["< 1", "1-10", "10-50", "50-100", "100-500", "500-1000", "1000+"]


def wilson(fraud, total, z=Z):
    """95% Wilson score interval for a proportion, returned as percentages (low, high)."""
    if total == 0:
        return 0.0, 0.0
    p = fraud / total
    denom = 1 + z * z / total
    centre = (p + z * z / (2 * total)) / denom
    half = z * math.sqrt(p * (1 - p) / total + z * z / (4 * total * total)) / denom
    return 100 * (centre - half), 100 * (centre + half)


def main():
    src = sys.argv[1] if len(sys.argv) > 1 else "results/results.tsv"
    dst = sys.argv[2] if len(sys.argv) > 2 else "results/ranking.tsv"

    data = {"HOUR": [], "AMT": []}
    with open(src) as f:
        for line in f:
            p = line.rstrip("\n").split("\t")
            if len(p) == 6:
                data[p[0]].append((int(p[1]), int(p[2]), int(p[3])))   # bucket, total, fraud

    total = sum(t for _, t, _ in data["HOUR"])
    fraud = sum(fr for _, _, fr in data["HOUR"])
    overall = 100.0 * fraud / total
    print("Overall: %d transactions, %d fraud, rate %.4f%%" % (total, fraud, overall))

    rows = []
    for dim in ("HOUR", "AMT"):
        ranked = sorted(data[dim], key=lambda r: r[2] / r[1], reverse=True)
        print("\n%s ranked by fraud rate (95%% CI = Wilson interval)" % dim)
        print("%4s %-10s %8s %6s %8s %7s %16s  %s" %
              ("rank", "bucket", "txns", "fraud", "rate%", "lift", "95% CI %", "verdict"))
        for rank, (bucket, t, fr) in enumerate(ranked, 1):
            rate = 100.0 * fr / t
            lo, hi = wilson(fr, t)
            if fr < MIN_FRAUD:
                verdict = "FEW CASES"
            elif lo > overall:
                verdict = "HIGHER"
            elif hi < overall:
                verdict = "LOWER"
            else:
                verdict = "AVERAGE"
            label = ("%02d:00" % bucket) if dim == "HOUR" else AMOUNT_LABELS[bucket]
            print("%4d %-10s %8d %6d %8.4f %6.1fx %7.3f-%-8.3f  %s" %
                  (rank, label, t, fr, rate, rate / overall, lo, hi, verdict))
            rows.append((dim, rank, label, t, fr, rate, rate / overall, lo, hi, verdict))

    with open(dst, "w") as out:
        out.write("dimension\trank\tbucket\ttxns\tfraud\trate_pct\tlift\tci_low_pct\tci_high_pct\tverdict\n")
        for r in rows:
            out.write("%s\t%d\t%s\t%d\t%d\t%.4f\t%.2f\t%.4f\t%.4f\t%s\n" % r)
    print("\nwrote", dst)


if __name__ == "__main__":
    main()
