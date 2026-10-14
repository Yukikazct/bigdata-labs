#!/usr/bin/env bash
# A document crosses several input splits; repeated words must still merge.
set -euo pipefail
cd "$(dirname "$0")"
export HADOOP_HOME="${HADOOP_HOME:-/usr/local/hadoop}"
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-arm64}"
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$JAVA_HOME/bin:$PATH"
RUN_ID="${1:-$(date +%Y%m%d-%H%M%S)}"
HDFS_BASE="/user/$USER/lab2/edge-$RUN_ID"
LOCAL_RESULT="results/edge-$RUN_ID"
mkdir -p "$LOCAL_RESULT/documents"
cat > "$LOCAL_RESULT/documents/document01.txt" <<'TEXT'
The DATA data, hadoop!
index search; DATA.
MapReduce hadoop data.
TEXT
cat > "$LOCAL_RESULT/documents/document02.txt" <<'TEXT'
data; SEARCH search.
INDEX index! 12345
TEXT
printf 'the and or to in\n' > "$LOCAL_RESULT/documents/document03.txt"
: > "$LOCAL_RESULT/documents/document04.txt"
hdfs dfs -mkdir -p "$HDFS_BASE/input"
hdfs dfs -put "$LOCAL_RESULT"/documents/*.txt "$HDFS_BASE/input/"
hdfs dfs -put stopwords.txt "$HDFS_BASE/stopwords.txt"
hadoop jar build/inverted-index.jar InvertedIndex \
  -Dmapreduce.input.fileinputformat.split.maxsize=24 \
  "$HDFS_BASE/input" "$HDFS_BASE/counts" "$HDFS_BASE/output" \
  "$HDFS_BASE/stopwords.txt" 3 2>&1 | tee "$LOCAL_RESULT/job.log"
hdfs dfs -getmerge "$HDFS_BASE/output/part-r-*" "$LOCAL_RESULT/index.txt"
python3 verify.py "$LOCAL_RESULT/documents" stopwords.txt "$LOCAL_RESULT/index.txt" \
  --json "$LOCAL_RESULT/verification.json"
cat "$LOCAL_RESULT/index.txt"
