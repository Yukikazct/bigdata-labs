#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
export HADOOP_HOME="${HADOOP_HOME:-/usr/local/hadoop}"
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-arm64}"
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$JAVA_HOME/bin:$PATH"
RUN_ID="${1:-$(date +%Y%m%d-%H%M%S)}"
HDFS_BASE="/user/$USER/lab2/$RUN_ID"
LOCAL_RESULT="results/$RUN_ID"
mkdir -p "$LOCAL_RESULT"
bash build.sh
hdfs dfs -mkdir -p "$HDFS_BASE/input"
hdfs dfs -put documents/*.txt "$HDFS_BASE/input/"
hdfs dfs -put stopwords.txt "$HDFS_BASE/stopwords.txt"
hdfs dfs -ls "$HDFS_BASE/input" | tee "$LOCAL_RESULT/hdfs-input.txt"
hadoop jar build/inverted-index.jar InvertedIndex \
  "$HDFS_BASE/input" "$HDFS_BASE/counts" "$HDFS_BASE/output" \
  "$HDFS_BASE/stopwords.txt" 2 2>&1 | tee "$LOCAL_RESULT/job.log"
hdfs dfs -getmerge "$HDFS_BASE/output/part-r-*" "$LOCAL_RESULT/index.txt"
hdfs dfs -getmerge "$HDFS_BASE/counts/part-r-*" "$LOCAL_RESULT/counts.tsv"
python3 verify.py documents stopwords.txt "$LOCAL_RESULT/index.txt" \
  --json "$LOCAL_RESULT/verification.json" | tee "$LOCAL_RESULT/verification.txt"
echo "HDFS results: $HDFS_BASE/output"
echo "Local results: $LOCAL_RESULT"
