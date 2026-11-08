#!/usr/bin/env bash
# Run the whole MapReduce pipeline on HDFS.
# Usage:  bash run_pipeline.sh        (from anywhere; HDFS and YARN must be running)
# Needs:  data/creditcard.csv  (see data/README.md)
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p logs results/validation
exec > >(tee logs/run_log.txt) 2>&1          # everything below is also saved to logs/run_log.txt

[ -f data/creditcard.csv ] || { echo "Missing data/creditcard.csv - see data/README.md" >&2; exit 1; }
STREAMING_JAR=$(ls "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar | head -1)

# 1. INGEST to HDFS
hdfs dfs -mkdir -p /fraud/raw
hdfs dfs -put -f data/creditcard.csv /fraud/raw/

# 2. CLEAN (mapper validates, reducer removes duplicates)
hdfs dfs -rm -r -f /fraud/clean
hadoop jar "$STREAMING_JAR" \
  -files src/cleaning/clean_mapper.py,src/cleaning/clean_reducer.py \
  -mapper "python3 clean_mapper.py" -reducer "python3 clean_reducer.py" \
  -input /fraud/raw -output /fraud/clean

# 3. ANALYSE WITH COMBINER + key-field partitioner on fields 1-2 (dimension+bucket), so the 31 keys
#    are spread over the 2 reducers while all values of one key still reach the same reducer
hdfs dfs -rm -r -f /fraud/out_combiner
time hadoop jar "$STREAMING_JAR" \
  -D stream.num.map.output.key.fields=2 \
  -D mapreduce.partition.keypartitioner.options=-k1,2 \
  -D mapreduce.job.reduces=2 \
  -D stream.num.reduce.output.key.fields=2 \
  -files src/analysis/fraud_mapper.py,src/analysis/fraud_combiner.py,src/analysis/fraud_reducer.py \
  -mapper "python3 fraud_mapper.py" -combiner "python3 fraud_combiner.py" \
  -reducer "python3 fraud_reducer.py" \
  -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
  -input /fraud/clean -output /fraud/out_combiner

# 4. SAME JOB WITHOUT COMBINER (for the runtime comparison)
hdfs dfs -rm -r -f /fraud/out_nocombiner
time hadoop jar "$STREAMING_JAR" \
  -D stream.num.map.output.key.fields=2 \
  -D mapreduce.partition.keypartitioner.options=-k1,2 \
  -D mapreduce.job.reduces=2 \
  -D stream.num.reduce.output.key.fields=2 \
  -files src/analysis/fraud_mapper.py,src/analysis/fraud_reducer.py \
  -mapper "python3 fraud_mapper.py" \
  -reducer "python3 fraud_reducer.py" \
  -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
  -input /fraud/clean -output /fraud/out_nocombiner

# 5. VALIDATE: both jobs must give identical, 31-line results (7 amount bands + 24 hours)
hdfs dfs -cat /fraud/out_combiner/part-*   | sort > results/validation/results_combiner.tsv
hdfs dfs -cat /fraud/out_nocombiner/part-* | sort > results/validation/results_nocombiner.tsv
wc -l results/validation/results_combiner.tsv results/validation/results_nocombiner.tsv
if diff results/validation/results_combiner.tsv results/validation/results_nocombiner.tsv > /dev/null; then
  echo "VALIDATION OK: combiner and no-combiner outputs are identical"
  cp results/validation/results_combiner.tsv results/results.tsv
else
  echo "VALIDATION FAILED: outputs differ" >&2
  exit 1
fi

# 6. Short counters summary (safe to commit; the full log stays local)
sleep 2
grep -E "^real|Combine output records|Reduce input groups|Reduce shuffle bytes|duplicate_rows|valid_rows|header_rows" \
  logs/run_log.txt > logs/counters_summary.txt || true
echo "Done. Charts: python3 src/visualize/plot_results.py"
