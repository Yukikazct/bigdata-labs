#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
source ./common_env.sh
BASE=$LAB3_BASE
export HBASE_CLASSPATH=$PWD/build/hbase-lab.jar
mkdir -p "$BASE/logs"
printf 'after\n' | "$HBASE_HOME/bin/hbase" HBaseLab 2>&1 | tee "$BASE/logs/hbase-java.log"
python3 prepare_shell.py
"$HBASE_HOME/bin/hbase" shell -n "$PWD/lab3_commands.rb" 2>&1 | tee "$BASE/logs/hbase-shell.log"
