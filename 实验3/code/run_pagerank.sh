#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
BASE=${LAB3_BASE:-$HOME/lab3-pagerank}
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-arm64}
export HADOOP_HOME=${HADOOP_HOME:-/usr/local/hadoop}
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME=${SPARK_HOME:-$BASE/software/spark-3.5.3-bin-hadoop3}
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH
RUN=${1:-$(date +%Y%m%d-%H%M%S)}
HDFS_ROOT=/user/yuki/lab3/runs/$RUN
mkdir -p "$BASE/logs" "$BASE/results/$RUN/mr-iterations"
printf '%s\n' "$HDFS_ROOT" > "$BASE/results/$RUN/hdfs-root.txt"
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
