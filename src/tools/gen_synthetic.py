#!/usr/bin/env python3
"""
SYNTHETIC test data in the same schema as Kaggle creditcard.csv.
*** SYNTHETIC - for testing the pipeline only, NOT real transactions. ***

Includes a few deliberately bad rows (duplicate, missing value, wrong column
count, negative amount) so the cleaning job has something to catch.
Usage: python gen_synthetic.py 20000 > synthetic.csv
"""
import random
import sys

n = int(sys.argv[1]) if len(sys.argv) > 1 else 20000
random.seed(42)

cols = ["Time"] + ["V%d" % i for i in range(1, 29)] + ["Amount", "Class"]
print(",".join('"%s"' % c for c in cols))

rows = []
for _ in range(n):
    fraud = random.random() < 0.02
    # fraud skews to night hours and small/very large amounts
    if fraud:
        hour = random.choice([1, 2, 3, 4, 22, 23])
        amount = random.choice([random.uniform(0, 5), random.uniform(500, 2000)])
    else:
        hour = random.choice(range(7, 23))
        amount = random.expovariate(1 / 80)
    t = random.randint(0, 1) * 86400 + hour * 3600 + random.randint(0, 3599)
    v = ["%.6f" % random.gauss(0, 1) for _ in range(28)]
    rows.append(",".join([str(t)] + v + ["%.2f" % amount, '"%d"' % fraud]))

for r in rows:
    print(r)

print(rows[0])                                   # exact duplicate
print(rows[1].replace(rows[1].split(",")[5], ""))  # missing value
print("1,2,3")                                   # wrong column count
print(",".join(rows[2].split(",")[:29] + ["-5.0", '"0"']))  # negative amount
