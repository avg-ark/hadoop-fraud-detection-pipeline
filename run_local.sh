#!/usr/bin/env bash
# Local simulation of both MapReduce jobs - no Hadoop needed.
# This is exactly what Hadoop does, on one machine:  cat | map | sort | reduce
# Usage:  bash run_local.sh [input.csv]      (default: data/creditcard.csv)
set -euo pipefail
cd "$(dirname "$0")"
IN=${1:-data/creditcard.csv}
OUT=results/local
mkdir -p "$OUT"

echo "== Cleaning =="
cat "$IN" | python3 src/cleaning/clean_mapper.py 2> "$OUT/clean_counters.txt" \
  | sort | python3 src/cleaning/clean_reducer.py 2>> "$OUT/clean_counters.txt" > "$OUT/clean.csv"
sort "$OUT/clean_counters.txt" | uniq -c
echo "clean rows: $(wc -l < "$OUT/clean.csv")"

echo "== Analysis =="
python3 src/analysis/fraud_mapper.py < "$OUT/clean.csv" | sort \
  | python3 src/analysis/fraud_combiner.py | sort \
  | python3 src/analysis/fraud_reducer.py > "$OUT/results_combiner.tsv"
python3 src/analysis/fraud_mapper.py < "$OUT/clean.csv" | sort \
  | python3 src/analysis/fraud_reducer.py > "$OUT/results_nocombiner.tsv"

if diff "$OUT/results_combiner.tsv" "$OUT/results_nocombiner.tsv" > /dev/null; then
  echo "LOCAL VALIDATION OK: with/without Combiner identical ($(wc -l < "$OUT/results_combiner.tsv") rows)"
else
  echo "LOCAL VALIDATION FAILED" >&2
  exit 1
fi
echo "Results: $OUT/results_combiner.tsv"
