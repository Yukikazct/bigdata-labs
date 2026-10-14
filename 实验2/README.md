# 实验二：文档倒排索引算法实现

使用 **Hadoop 3.3.6 / YARN 上的两个 Java MapReduce 作业**，对 12 篇公开英文技术文档建立倒排索引。实际运行平台：VMware Fusion 13.6.4、Ubuntu 26.04.1 LTS ARM64、OpenJDK 11.0.32.1。

## 数据与分词

文档来自 [Apache Hadoop 3.3.6 官方文档](https://hadoop.apache.org/docs/r3.3.6/)。`sources.json` 逐篇记录标题、原始 URL、下载时间、正文大小和 SHA-256；`documents/document01.txt`～`document12.txt` 保存实际输入。

下载脚本提取页面 `contentBox` 内的标题、段落、列表、代码和表格文字，去除网页导航和 HTML 标签。保留文章内部目录和代码示例，因此词频反映完整技术文档正文，并非仅自然语言段落。

统一转为小写，使用 `[a-z]+` 匹配英文词元，数字与标点作为边界；`map-reduce` 会成为 `map` 和 `reduce`。不进行词干提取，`file` 与 `files` 分别统计。`stopwords.txt` 包含 139 个停词，通过 Hadoop 分布式缓存分发给所有 Mapper。

输入采用平铺目录，文件名唯一且不包含制表符；同名文件不能代表不同文档。Hadoop 与校验程序均跳过以 `.` 或 `_` 开头的元数据文件。

## 算法

1. 第一个 Map 从 FileSplit 取得文档名，输出 `((word, document), 1)`；Combiner 与 Reduce 分别求和，得到唯一的 `(word, document, count)`。复合键保证同一文档跨多个输入分片时仍正确汇总。
2. 第二个 Map 输出 `(word, (document, count))`；Reduce 对文档名排序，计算全局总词频，形成倒排表。第二阶段固定使用一个 Reducer，使输出按单词全局有序。

输出格式：`word<TAB><document01.txt,count>;...;<total,count>.`。`total` 是所有文档内该单词的出现次数之和，不是文档数。

## 复现

虚拟机已有 Hadoop 伪分布式环境，首先启动 HDFS 和 YARN：

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
export HADOOP_HOME=/usr/local/hadoop
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$JAVA_HOME/bin:$PATH"
start-dfs.sh
start-yarn.sh
jps
```

使用仓库已有的文档可直接复现实验；需要重新下载时执行 `python3 download_documents.py`（网页更新可能改变词频）：

```bash
cd 实验2
bash run.sh
```

脚本编译 Java、创建新的 HDFS 运行目录、上传输入与停词、运行两个作业、取回结果并执行完整核对。不要重新格式化 NameNode。每次使用新的运行编号，保留既有结果；固定编号对应的输出目录已存在时，作业会报错。

实际主数据运行编号为 `20261014-main`。HDFS 路径为 `/user/yuki/lab2/20261014-main/`，本地结果见 `results/20261014-main/`。

## 实测结果

| 指标 | 数量 |
| --- | ---: |
| 英文文档 | 12 |
| 原始词元 | 54,040 |
| 被过滤的停词词元 | 18,248 |
| 保留词元 | 35,792 |
| 唯一词项 | 3,530 |
| 单词—文档记录 | 7,861 |
| 主作业 Map / Reduce | 12 / 2，随后 2 / 1 |

第一个作业的 Combiner 把 35,792 条 Map 输出合并为 7,861 条记录。`verify.py` 使用独立的 Python 单进程逐文档计数，核对全部文档词频、每个 total、输出排序及格式；核对通过。最终输出中 `hadoop` 总词频为 582，`data` 为 139，`mapreduce` 为 158。

```text
index   <document03.txt,1>;<document11.txt,2>;<total,3>.
search  <document04.txt,1>;<document07.txt,2>;<total,3>.
```

完整索引见 `results/20261014-main/index.txt`，中间计数见 `counts.tsv`，作业日志见 `job.log`，逐文档统计和校验结论见 `verification.json`。

## 跨分片与边界验证

```bash
bash build.sh
bash check_edge_cases.sh
```

验证输入含大小写、标点、重复单词、纯数字、仅停词文档和空文档。最大分片长度设为 24 字节，第一阶段有 7 个 Map、3 个 Reduce，第二阶段有 8 个 Map、1 个 Reduce。该设置迫使同一文档跨分片统计。保存的 `results/edge-20261014/` 验证结果为：

```text
data     <document01.txt,4>;<document02.txt,1>;<total,5>.
hadoop   <document01.txt,2>;<total,2>.
index    <document01.txt,1>;<document02.txt,2>;<total,3>.
mapreduce <document01.txt,1>;<total,1>.
search   <document01.txt,1>;<document02.txt,2>;<total,3>.
```

## 文件说明

| 文件 | 用途 |
| --- | --- |
| `InvertedIndex.java` | 两阶段 MapReduce 实现 |
| `download_documents.py`、`sources.json` | 可追溯的网页采集 |
| `documents/`、`stopwords.txt` | 实际输入与停词表 |
| `build.sh`、`run.sh` | 编译、上传、提交与校验 |
| `verify.py`、`check_edge_cases.sh` | 独立核对与跨分片验证 |
| `results/` | 原始运行日志、完整输出和校验结果 |
| `screenshots/` | 实际 HDFS/YARN 页面截图 |

Linux ARM 平台可能显示 `Unable to load native-hadoop library` 警告；本实验已使用 Java 回退实现完成作业。第二阶段单 Reducer、每个词项的文档列表存于内存，适合教学规模；更大规模需要按词项分区、多文件输出或流式归并。

文档版权归 Apache Software Foundation；原始文档按 Apache License 2.0 发布。来源与许可见 `NOTICE.txt`、`DATA_LICENSE.txt`。
