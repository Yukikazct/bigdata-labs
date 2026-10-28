#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
source ./common_env.sh
mkdir -p build/mr build/hbase build/spark
javac -encoding UTF-8 -cp "$("$HADOOP_HOME/bin/hadoop" classpath)" -d build/mr PageRankMR.java
jar cf build/pagerank-mr.jar -C build/mr .
javac -encoding UTF-8 -cp "$("$HBASE_HOME/bin/hbase" classpath)" -d build/hbase HBaseLab.java
jar cf build/hbase-lab.jar -C build/hbase .
javac -encoding UTF-8 -cp "$SPARK_HOME/jars/*" -d build/spark PageRankSpark.java
jar cf build/pagerank-spark.jar -C build/spark .
