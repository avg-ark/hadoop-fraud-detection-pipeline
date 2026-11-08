#!/usr/bin/env python3
"""
Fraud-analysis MAPPER (Hadoop Streaming).

Input : one CLEAN row  Time,V1..V28,Amount,Class
Output: TWO key-value pairs per row

   key = <DIMENSION> \t <BUCKET>        value = 1,<is_fraud>,<amount>
         HOUR  \t 00..23   (hour of day, derived from Time seconds)
         AMT   \t 0..6     (amount band, index keeps natural sort order)

Value layout: count , fraud_count , amount_sum
Examples:
   HOUR	02    1,1,239.50
   AMT	4     1,0,120.00
"""
import sys

# upper bounds of each amount band (index = bucket id)
BOUNDS = [1, 10, 50, 100, 500, 1000]


def amount_bucket(amount):
    for i, upper in enumerate(BOUNDS):
        if amount < upper:
            return i
    return len(BOUNDS)


for line in sys.stdin:
    f = line.strip().split(",")
    if len(f) != 31:
        continue
    time_s, amount, cls = float(f[0]), float(f[29]), int(float(f[30]))
    hour = int(time_s // 3600) % 24
    value = "1,%d,%.2f" % (cls, amount)
    print("HOUR\t%02d\t%s" % (hour, value))
    print("AMT\t%d\t%s" % (amount_bucket(amount), value))
