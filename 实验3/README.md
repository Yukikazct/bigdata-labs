# 实验三：HBase 操作与 PageRank 算法实现

本目录覆盖实验要求的两部分：HBase Shell / Java API 和 MapReduce / Spark PageRank。报告由实际运行结果填写，PDF 是提交文件，DOCX 可继续编辑。源要求和原始模板保留在原位置。

## 文件

- `code/HBaseLab.java`：三张原始表、五个指定接口及表管理操作，20 项运行检查。
- `code/lab3_commands.rb`：与 Java 等价的 HBase Shell 操作。
- `code/PageRankMR.java`：建图、10 次同步 PageRank 更新、最终格式化。
- `code/PageRankSpark.java`：基于 Java RDD 的 10 次同步 PageRank 更新。
- `code/data/`：按要求保存的关系数据与链接图。
- `code/verify.py`：60 位 Decimal 独立矩阵计算，以及 HBase / YARN / 输出校验。
- `results/`：最终输出、MapReduce 每轮结果、Spark 迭代记录与校验 JSON。
- `evidence/`：真实环境信息、编译记录与 HBase / MapReduce / Spark 原始日志。
- `tools/build_report.py`：按课程原始模板生成 Word 报告。

## 在现有 Ubuntu 虚拟机复现

环境：Ubuntu 26.04.1 ARM64、OpenJDK 11、Hadoop 3.3.6，HDFS `hdfs://localhost:9000`，单节点 YARN。安装软件和实验文件均位于 `~/lab3-pagerank`，不格式化现有 HDFS。

在 Mac 终端将本地代码复制到虚拟机：

```bash
cd '/Users/a/Desktop/大数据/实验/实验3'
ssh yuki 'mkdir -p ~/lab3-pagerank'
scp -r code yuki:lab3-pagerank/
ssh yuki
```

下面的命令均在 Ubuntu 的 SSH 终端执行：

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH

# 冷启动虚拟机后，如 HDFS / YARN 尚未运行，先执行：
# /usr/local/hadoop/sbin/start-dfs.sh
# /usr/local/hadoop/sbin/start-yarn.sh
hdfs dfsadmin -safemode get
yarn node -list

cd ~/lab3-pagerank/code
bash setup_services.sh
bash build.sh
bash run_hbase.sh

# HBase 数据仍保留在 HDFS；停止进程，给计算任务释放内存。
~/lab3-pagerank/software/hbase-2.5.11-hadoop3/bin/stop-hbase.sh

# 每次使用一个新的运行标识，不覆盖原始结果。
RUN=$(date +%Y%m%d-%H%M%S)
bash run_pagerank.sh "$RUN"
python3 verify.py --results "$HOME/lab3-pagerank/results/$RUN" \
  --logs "$HOME/lab3-pagerank/logs"
```

`setup_services.sh` 下载 HBase 2.5.11 Hadoop3 构建和 Spark 3.5.3 Hadoop3 构建，用 Apache 发布的 SHA-512 校验安装包。软件位于实验专用目录，不修改原 Hadoop 配置。重复执行 HBase 演示会重建 `lab3_java_` 和 `lab3_shell_` 前缀下的实验表。Java 的写入方法限制在自己的实验表前缀内。

如果只复核已保存的证据，在本目录执行：

```bash
python3 code/verify.py --results results --logs evidence/logs
```

## 数据与约定

PageRank 采用 `d=0.85`、初始值 `1/N`、悬挂质量均匀回分，总 PR 约为 1。输出 `(网页,PR值)` 保留 10 位小数。要求中的大于 1 的示例仅用于展示格式；本实现明确使用概率归一化约定。

输入每行为 `网页<TAB>逗号分隔的出链`。无出链时保留制表符，第二字段为空。重复源网页的出链集合取并集，重复边去重，自环保留。目标网页即使未单独作为源网页出现也加入图。本次数据有 10 个节点和 10 条唯一边，含悬挂节点 D/H/I、孤立节点 I、自环 J、重复边 A→B、只作为目标出现的 H。

关系数据原样保留题目中的重复学号 2015003，以及选课表中缺少对应学生的 2015002。Student 按姓名、Course 按课程号、SC 按“学号_课程号”作为行键，不进行有歧义的姓名关联。Scratch 表中的 Zhangsan 三科分数是独立的 API 演示数据，不是三张关系表联接得到的结果。

`scanColumn` 保留每一行，某行缺少指定列时返回 `null`；对列族扫描返回其全部限定符。只使用服务端列过滤会省略缺失列的行，不能满足这一要求。未指定限定符的字段使用 `value` 作为默认限定符。

题目给出的 `modifyData(String tableName, String row, String column)` 没有新值参数；程序保留该签名并从终端读取新值，另外提供四参数重载。运行脚本向三参数版本输入 `after`，并检查读回的值。

HBase Shell 中同一条原始数据占用的行键、列限定符和值与 Java 完全一致。`get` 的 `0 row(s)` 表示没有匹配单元格；Java 将同一情形表示为 `null`。原始数据和破坏性操作演示分别使用主表和 Scratch 表。

## 验证范围

真实计算均在 Linux 虚拟机内执行：12 个 MapReduce YARN 应用（建图 1、排名迭代 10、格式化 1），以及 1 个 Spark YARN 应用。`verification.json` 记录真实编号与检查结果。

独立校验从链接图构造 60 位 Decimal 稠密转移矩阵，逐轮计算参考值；核对全部 10 轮 MapReduce 输出与图结构，检查 Spark 每轮质量和节点数，比较两个最终结果和参考值。另核对两套 HBase 输出与原始关系数据的全部 39 个单元格、Java 的 20 个检查、Shell 各操作结果，以及 YARN 最终状态。

MapReduce 用 10^15 的定点计数器传递悬挂质量，可能带来约 10^-15 量级的舍入；每轮原始结果容差为 10^-13。最终 10 位小数的结果按半个末位单位 5×10^-11 校验。10 轮是固定迭代次数，不代表已经数值收敛。单节点小图实验只支持功能和数值正确性结论。

## 公开代码

[Yukikazct/bigdata-labs 的实验3目录](https://github.com/Yukikazct/bigdata-labs/tree/main/%E5%AE%9E%E9%AA%8C3)

公开仓库包含实验代码、输入数据、结果和日志。个人报告及课程原文留在本地；PDF 按课程要求由本人提交至作业系统。
