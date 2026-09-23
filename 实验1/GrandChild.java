import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 找出所有 (grandchild, grandparent) 祖孙关系
 * 输入格式: child,parent  每行一条父子关系
 * 算法: 单 MapReduce, 用值前缀 "+"/"-" 区分"父母"与"孩子"
 *   Map:   (child, +parent) 和 (parent, -child)
 *   Reduce: 对 key=X, 收集 parents(X) 与 children(X),
 *           对每个 child 和 每个 parent 输出 (child, parent)
 */
public class GrandChild {

  public static class Map extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = value.toString();
      String[] tokens = line.split(",");
      if (tokens.length != 2) {
        return;
      }
      String child = tokens[0].trim();
      String parent = tokens[1].trim();
      // child 是 parent 的孩子
      context.write(new Text(child), new Text("+" + parent));
      // parent 是 child 的父/母
      context.write(new Text(parent), new Text("-" + child));
    }
  }

  public static class Reduce extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      List<String> parents = new ArrayList<String>();
      List<String> children = new ArrayList<String>();

      for (Text val : values) {
        String s = val.toString();
        if (s.startsWith("+")) {
          parents.add(s.substring(1));
        } else if (s.startsWith("-")) {
          children.add(s.substring(1));
        }
      }

      // key 的每个孩子 与 key 的每个父母 -> 祖孙关系 (child, parent)
      for (String child : children) {
        for (String parent : parents) {
          context.write(new Text(child), new Text(parent));
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "GrandChild");
    job.setJarByClass(GrandChild.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}