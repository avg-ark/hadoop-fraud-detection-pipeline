#!/usr/bin/env python3
"""
Cleaning MAPPER (Hadoop Streaming).

Input : one raw CSV line from creditcard.csv (31 columns:
        Time, V1..V28, Amount, Class)
Output: key = the cleaned, normalised row   value = 1
        (the reducer then keeps each distinct key once -> removes duplicates)

Invalid rows are DROPPED here and counted using Hadoop counters, which
Hadoop reads from stderr in the form  reporter:counter:<group>,<name>,<n>
"""
import sys

EXPECTED_COLS = 31


def counter(name):
    sys.stderr.write("reporter:counter:Cleaning,%s,1\n" % name)


for line in sys.stdin:
    line = line.strip()
    if not line:
        counter("blank_lines")
        continue

    # The Kaggle file quotes the header and the Class column, so strip quotes.
    fields = [f.strip().strip('"') for f in line.split(",")]

    if fields[0].lower() == "time":          # header row
        counter("header_rows")
        continue

    if len(fields) != EXPECTED_COLS:         # structurally broken record
        counter("bad_column_count")
        continue

    try:
        nums = [float(f) for f in fields]    # missing / non-numeric -> ValueError
    except ValueError:
        counter("non_numeric_or_missing")
        continue

    time_s, amount, cls = nums[0], nums[29], nums[30]
    if time_s < 0 or amount < 0 or cls not in (0.0, 1.0):
        counter("out_of_range")
        continue

    fields[30] = str(int(cls))               # normalise "0" / 0.0 -> 0
    print("%s\t1" % ",".join(fields))
    counter("valid_rows")
