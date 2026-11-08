#!/usr/bin/env python3
"""
Fraud-analysis REDUCER (Hadoop Streaming).

Receives all values for each (DIMENSION, BUCKET) key, sorted by key.
Sums them and computes the final statistics.

Output (tab separated):
  DIMENSION  BUCKET  total_txns  fraud_txns  fraud_rate_pct  avg_amount
"""
import sys

current = None
cnt = frd = 0
amt = 0.0


def emit():
    dim, bucket = current.split("\t")
    rate = 100.0 * frd / cnt if cnt else 0.0
    avg = amt / cnt if cnt else 0.0
    print("%s\t%s\t%d\t%d\t%.4f\t%.2f" % (dim, bucket, cnt, frd, rate, avg))


for line in sys.stdin:
    dim, bucket, val = line.rstrip("\n").split("\t")
    key = dim + "\t" + bucket
    c, fr, a = val.split(",")
    if key != current and current is not None:
        emit()
        cnt = frd = 0
        amt = 0.0
    current = key
    cnt += int(c)
    frd += int(fr)
    amt += float(a)

if current is not None:
    emit()
