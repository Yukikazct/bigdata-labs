#!/usr/bin/env bash
set -euo pipefail
BASE=${LAB3_BASE:-$HOME/lab3-pagerank}
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-arm64}
export HADOOP_HOME=${HADOOP_HOME:-/usr/local/hadoop}
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HBASE_HOME=$BASE/software/hbase-2.5.11-hadoop3
export SPARK_HOME=$BASE/software/spark-3.5.3-bin-hadoop3
mkdir -p "$BASE/software" "$BASE/logs"
for item in 'hbase/2.5.11/hbase-2.5.11-hadoop3-bin.tar.gz' 'spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz'; do
    file=${item##*/}
    if [[ ! -f "$BASE/software/$file" ]]; then
        curl -fL --retry 2 "https://repo.huaweicloud.com/apache/$item" -o "$BASE/software/$file"
    fi
    # Published Apache SHA-512 checksums, verified against archive.apache.org.
    if [[ "$file" == hbase* ]]; then
        expected=e5c3a33892b97c25d7aa2eee53cfc0f86dd591953746df898e7cec7ebd1b3cef8cdcc64c278b745ade2dea9d51e74c46fbd3963b52fc88588985438f45a78a96
    else
        expected=3b041e538282c15ad3b0ab2f05d558e1be2d16e047c2a98ca9c08176c4a50e7101bd2199aec97a66fdabf4b3f490c2c1cefd547bbe35a1b3e3143e066948ee6d
    fi
    printf '%s  %s\n' "$expected" "$BASE/software/$file" | sha512sum -c -
done
[[ -d "$HBASE_HOME" ]] || tar xzf "$BASE/software/hbase-2.5.11-hadoop3-bin.tar.gz" -C "$BASE/software"
[[ -d "$SPARK_HOME" ]] || tar xzf "$BASE/software/spark-3.5.3-bin-hadoop3.tgz" -C "$BASE/software"
cat > "$HBASE_HOME/conf/hbase-site.xml" <<EOF
<configuration>
 <property><name>hbase.cluster.distributed</name><value>true</value></property>
 <property><name>hbase.rootdir</name><value>hdfs://localhost:9000/user/yuki/lab3/hbase</value></property>
 <property><name>hbase.zookeeper.quorum</name><value>localhost</value></property>
 <property><name>hbase.zookeeper.property.dataDir</name><value>$BASE/zookeeper</value></property>
 <property><name>hbase.tmp.dir</name><value>$BASE/hbase-tmp</value></property>
 <property><name>hbase.wal.provider</name><value>filesystem</value></property>
</configuration>
EOF
cat > "$HBASE_HOME/conf/hbase-env.sh" <<EOF
export JAVA_HOME=$JAVA_HOME
export HBASE_MANAGES_ZK=true
export HBASE_HEAPSIZE=256
export HBASE_MASTER_OPTS="-Xmx256m"
export HBASE_REGIONSERVER_OPTS="-Xmx384m"
export HBASE_ZOOKEEPER_OPTS="-Xmx128m"
export HBASE_DISABLE_HADOOP_CLASSPATH_LOOKUP=true
EOF
printf 'localhost\n' > "$HBASE_HOME/conf/regionservers"
echo "HBASE_HOME=$HBASE_HOME"
echo "SPARK_HOME=$SPARK_HOME"
"$HBASE_HOME/bin/start-hbase.sh"
