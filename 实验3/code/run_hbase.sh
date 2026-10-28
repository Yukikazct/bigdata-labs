#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
BASE=${LAB3_BASE:-$HOME/lab3-pagerank}
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-arm64}
export HBASE_HOME=${HBASE_HOME:-$BASE/software/hbase-2.5.11-hadoop3}
export HBASE_CLASSPATH=$PWD/build/hbase-lab.jar
mkdir -p "$BASE/logs"
printf 'after\n' | "$HBASE_HOME/bin/hbase" HBaseLab 2>&1 | tee "$BASE/logs/hbase-java.log"
python3 prepare_shell.py
"$HBASE_HOME/bin/hbase" shell -n "$PWD/lab3_commands.rb" 2>&1 | tee "$BASE/logs/hbase-shell.log"
