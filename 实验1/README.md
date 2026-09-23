# 实验一 Hadoop 安装部署与基本操作

大数据管理与分析课程实验一。内容包括 Linux 常用命令练习、Hadoop 伪分布式环境搭建、HDFS Shell 与 Java API 操作，以及 MapReduce 初级编程。

## 实验环境

| 项目 | 版本 |
| --- | --- |
| 虚拟机 | VMware Workstation 16 |
| 操作系统 | Ubuntu 20.04 64 位 |
| 大数据平台 | Hadoop 3.3.6（伪分布式） |
| JDK | OpenJDK 1.8 |

## 文件说明

| 文件 | 对应实验内容 |
| --- | --- |
| `HDFSJavaAPI.java` | HDFS Java API 编程：上传、下载、读取、查看文件信息、递归列目录、创建与删除文件/目录、追加内容、移动文件，共 10 项操作 |
| `Dedup.java` | MapReduce 数据去重：合并两个输入文件并剔除重复内容 |
| `Sort.java` | MapReduce 数据排序：含自定义 Partitioner，借助框架默认的 key 排序机制 |
| `GrandChild.java` | MapReduce 祖孙关系查找：单个作业内用 `+parent` / `-child` 标记双向输出，Reduce 阶段对父母与孩子列表求笛卡尔积 |

## 编译与运行

先加载 Hadoop 环境变量：

```bash
export HADOOP_HOME=/usr/local/hadoop
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
```

### HDFS Java API

```bash
javac -cp $(hadoop classpath) HDFSJavaAPI.java
java -cp .:$(hadoop classpath) HDFSJavaAPI
```

程序按 1～10 的顺序依次执行各项操作，结果直接打印到控制台。

### MapReduce 程序

三个程序用法一致，以 `Dedup` 为例：

```bash
javac -cp $(hadoop classpath) Dedup.java
jar cvf dedup.jar Dedup*.class
hadoop jar dedup.jar Dedup <输入路径> <输出路径>
```

`Sort`、`GrandChild` 同理，替换类名与 jar 名即可。

## 输入样例

**Dedup**（含重复项）

```
hadoop
hello
world
hello
spark
hadoop
```

输出 4 个不重复的单词。

**Sort**（乱序）

```
5
3
8
1
9
2
7
4
10
6
```

输出 1～10 升序排列。

**GrandChild**（每行 `child,parent`）

```
Tom,Jack
Jack,Alice
Jack,Jessie
Alice,Smith
```

输出 3 条祖孙关系：`Jack-Smith`、`Tom-Jessie`、`Tom-Alice`。

## 运行注意事项

- 输出路径必须事先不存在，否则 Job 会直接报错；重跑前先清理：`hdfs dfs -rm -r <输出路径>`。
- `HDFSJavaAPI.java` 中的 `fs.defaultFS`（`hdfs://localhost:9000`）与远程路径（`/user/yuki/...`）为硬编码，换环境运行前需相应修改；其中"移动文件"一步依赖 `/user/yuki/hello.txt` 已存在。
- `Sort.java` 的 Partitioner 按 `maxNumber = 100` 划分区间，输入数值超出该范围时会返回分区号 `-1` 导致作业失败，处理更大数值时需调整该常量。
