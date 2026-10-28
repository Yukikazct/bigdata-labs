#!/usr/bin/env bash
# Source this file from the build/run scripts or an interactive Bash session.
LAB3_CODE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
export LAB3_BASE=${LAB3_BASE:-$(dirname "$LAB3_CODE_DIR")/runtime}
: "${JAVA_HOME:?Set JAVA_HOME to the JDK installation directory}"
: "${HADOOP_HOME:?Set HADOOP_HOME to the Hadoop installation directory}"
export JAVA_HOME HADOOP_HOME
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}
export HBASE_HOME=${HBASE_HOME:-$LAB3_BASE/software/hbase-2.5.11-hadoop3}
export SPARK_HOME=${SPARK_HOME:-$LAB3_BASE/software/spark-3.5.3-bin-hadoop3}
export LAB3_HDFS_BASE=${LAB3_HDFS_BASE:-/user/$(id -un)/lab3}
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH
