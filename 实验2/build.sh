#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
export HADOOP_HOME="${HADOOP_HOME:-/usr/local/hadoop}"
mkdir -p build/classes
javac -encoding UTF-8 -cp "$("$HADOOP_HOME/bin/hadoop" classpath)" -d build/classes InvertedIndex.java
jar cf build/inverted-index.jar -C build/classes .
echo "Built build/inverted-index.jar"
