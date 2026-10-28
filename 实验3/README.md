# 实验三：HBase 操作与 PageRank 算法实现

使用 HBase Shell 和 Java API 完成表管理与数据操作，分别用 Hadoop MapReduce 和 Spark RDD 实现 PageRank。两种实现使用相同输入，连续迭代 10 次，结果保留 10 位小数。

## 目录

```text
code/
  HBaseLab.java          HBase 数据操作与 20 项检查
  lab3_commands.rb      HBase Shell 命令
  PageRankMR.java        MapReduce PageRank
  PageRankSpark.java     Spark RDD PageRank
  data/                 网页链接与关系表数据
  common_env.sh         通用环境配置
  setup_services.sh     安装并配置实验用 HBase、Spark
  build.sh              编译 Java 程序
  run_hbase.sh          执行 HBase 实验
  run_pagerank.sh       执行两种 PageRank
  verify.py             独立校验结果
results/                已执行的计算结果与校验记录
evidence/               软件版本、编译记录和运行日志节选
```

## 环境准备

需要 Linux、JDK 11、Python 3，以及已配置好的 Hadoop 3 单节点伪分布式环境。运行前确认 HDFS、YARN 正常，HDFS 已退出安全模式。

进入 `code` 目录，按实际安装位置设置环境变量。以下路径是占位示例：

```bash
export JAVA_HOME=/path/to/jdk-11
export HADOOP_HOME=/path/to/hadoop
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH"

hdfs dfsadmin -safemode get
yarn node -list
```

`LAB3_BASE` 默认指向本实验目录下的 `runtime/`，用于保存下载的软件、运行日志和结果。HDFS 实验目录默认由当前用户生成，可通过 `LAB3_HDFS_BASE` 指定。已有 HBase、Spark 安装时，可自行设置 `HBASE_HOME`、`SPARK_HOME`。

## 编译与运行

```bash
# 下载并校验 HBase 2.5.11、Spark 3.5.3，配置实验服务。
bash setup_services.sh
bash build.sh
bash run_hbase.sh

# 内存有限时，可在 HBase 实验完成后停止服务，再运行 PageRank。
# source common_env.sh
# "$HBASE_HOME/bin/stop-hbase.sh"

RUN=$(date +%Y%m%d-%H%M%S)
bash run_pagerank.sh "$RUN"

source common_env.sh
python3 verify.py --results "$LAB3_BASE/results/$RUN" --logs "$LAB3_BASE/logs"
```

`setup_services.sh` 为单节点实验生成 HBase 配置，使用 Hadoop 配置中的 HDFS 地址；安装包通过 Apache 发布的 SHA-512 校验。每次 PageRank 运行应使用新的运行标识。

HBase 演示只操作 `lab3_java_`、`lab3_shell_` 前缀下的实验表，重复执行会重建这些表。原始数据表和增删改操作使用的 Scratch 表相互独立。

## 数据与实现

HBase 原始数据包含学生、课程、选课三张表。学生以姓名为行键，课程以课程号为行键，选课以“学号_课程号”为组合行键。题目中重复的学号和没有对应学生的选课记录按原样保留，避免产生有歧义的关联。

`scanColumn` 支持列族及指定列；某行缺少目标列时保留该行并返回 `null`。题目的三参数 `modifyData` 从终端读取新值，同时提供四参数重载，便于自动运行。

PageRank 输入格式为 `网页<TAB>逗号分隔的出链`，空出链仍保留制表符。重复边去重，自环保留，目标网页也加入节点集合。示例图共 10 个节点、10 条唯一边，包含悬挂节点、孤立节点、自环及仅作为目标出现的网页。

采用阻尼系数 `0.85`，初始排名为 `1/N`，每轮将悬挂质量均匀分配给全部网页，使总 PR 约为 1。MapReduce 执行建图、10 轮更新和格式化共 12 个作业；Spark 使用 RDD 完成相同的 10 轮更新。两者均提交至 YARN。

## 结果与校验

在实验目录中运行以下命令，可复核仓库保存的结果：

```bash
python3 code/verify.py --results results --logs evidence/logs
```

两种程序保留 10 位小数后的输出一致。校验程序使用 60 位 Decimal 精度的转移矩阵独立计算参考值，检查 MapReduce 各轮数值与图结构、Spark 每轮节点数和质量守恒、最终结果格式，以及 HBase 数据和 YARN 完成状态。

公开日志为真实运行记录的节选，省略用户名、主机名、网络地址和本机路径，保留作业编号、迭代数值、检查结果与完成状态。`results/verification.json` 记录校验详情。

固定迭代 10 次不代表数值已经收敛；本示例用于验证实现正确性。
