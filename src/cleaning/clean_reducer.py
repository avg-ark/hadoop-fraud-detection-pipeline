#!/usr/bin/env python3
"""
Cleaning REDUCER (Hadoop Streaming).

Hadoop sorts mapper output by key before it reaches the reducer, so identical
rows arrive next to each other. We emit a row the first time we see it and
count the repeats as duplicates.
"""
import sys

previous = None
for line in sys.stdin:
    key = line.rstrip("\n").split("\t")[0]
    if key == previous:
        sys.stderr.write("reporter:counter:Cleaning,duplicate_rows,1\n")
        continue
    print(key)
    previous = key
