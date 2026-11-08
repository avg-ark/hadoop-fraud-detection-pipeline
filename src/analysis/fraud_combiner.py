#!/usr/bin/env python3
"""
COMBINER (Hadoop Streaming) - a "mini reducer" that runs on each mapper's
local output BEFORE the shuffle, so far less data crosses the network.

It must output the SAME format it reads:  key \t count,fraud,amount_sum
That is why it only adds numbers up and does NOT compute rates.
Adding is associative, which is what makes it safe to use as a combiner.
"""
import sys

current = None
cnt = frd = 0
amt = 0.0

for line in sys.stdin:
    dim, bucket, val = line.rstrip("\n").split("\t")
    key = dim + "\t" + bucket
    c, fr, a = val.split(",")
    if key != current and current is not None:
        print("%s\t%d,%d,%.2f" % (current, cnt, frd, amt))
        cnt = frd = 0
        amt = 0.0
    current = key
    cnt += int(c)
    frd += int(fr)
    amt += float(a)

if current is not None:
    print("%s\t%d,%d,%.2f" % (current, cnt, frd, amt))
