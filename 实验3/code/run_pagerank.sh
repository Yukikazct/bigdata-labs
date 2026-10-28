#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
source ./common_env.sh
BASE=$LAB3_BASE
RUN=${1:-$(date +%Y%m%d-%H%M%S)}
HDFS_ROOT=$LAB3_HDFS_BASE/runs/$RUN
mkdir -p "$BASE/logs" "$BASE/results/$RUN/mr-iterations"
printf '%s\n' "$HDFS_ROOT" > "$BASE/results/$RUN/hdfs-root.txt"
printf '%s\n' "$RUN" > "$BASE/results/$RUN/run-id.txt"
hdfs dfs -mkdir -p "$HDFS_ROOT/input"
hdfs dfs -put data/graph.tsv "$HDFS_ROOT/input/graph.tsv"
hadoop jar build/pagerank-mr.jar PageRankMR "$HDFS_ROOT/input" "$HDFS_ROOT/mr" 2>&1 | tee "$BASE/logs/mr-$RUN.log"
hdfs dfs -cat "$HDFS_ROOT/mr/result/part-r-*" > "$BASE/results/$RUN/mr.txt"
for i in $(seq -w 1 10); do
    hdfs dfs -cat "$HDFS_ROOT/mr/iter-$i/part-r-*" > "$BASE/results/$RUN/mr-iterations/iter-$i.tsv"
done
"$SPARK_HOME/bin/spark-submit" --master yarn --deploy-mode client --class PageRankSpark \
  --driver-memory 512m --executor-memory 512m --num-executors 1 --executor-cores 1 \
  --conf spark.executor.memoryOverhead=384 --conf spark.driver.memoryOverhead=256 \
  --conf spark.ui.enabled=false --conf spark.yarn.submit.waitAppCompletion=true \
  build/pagerank-spark.jar "$HDFS_ROOT/input" "$HDFS_ROOT/spark" 2>&1 | tee "$BASE/logs/spark-$RUN.log"
hdfs dfs -cat "$HDFS_ROOT/spark/result/part-*" > "$BASE/results/$RUN/spark.txt"
hdfs dfs -cat "$HDFS_ROOT/spark/history/part-*" > "$BASE/results/$RUN/spark-history.json"
yarn application -list -appStates FINISHED > "$BASE/results/$RUN/yarn-applications.txt" 2>&1
echo "RESULT_DIR=$BASE/results/$RUN"
