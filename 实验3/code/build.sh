#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-arm64}
export HADOOP_HOME=${HADOOP_HOME:-/usr/local/hadoop}
export HBASE_HOME=${HBASE_HOME:-$HOME/lab3-pagerank/software/hbase-2.5.11-hadoop3}
export SPARK_HOME=${SPARK_HOME:-$HOME/lab3-pagerank/software/spark-3.5.3-bin-hadoop3}
mkdir -p build/mr build/hbase build/spark
javac -encoding UTF-8 -cp "$("$HADOOP_HOME/bin/hadoop" classpath)" -d build/mr PageRankMR.java
jar cf build/pagerank-mr.jar -C build/mr .
javac -encoding UTF-8 -cp "$("$HBASE_HOME/bin/hbase" classpath)" -d build/hbase HBaseLab.java
jar cf build/hbase-lab.jar -C build/hbase .
javac -encoding UTF-8 -cp "$SPARK_HOME/jars/*" -d build/spark PageRankSpark.java
jar cf build/pagerank-spark.jar -C build/spark .
