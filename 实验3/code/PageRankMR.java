import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** Ten synchronous PageRank iterations, including dangling mass and target-only pages. */
public class PageRankMR {
    private static final long SCALE = 1_000_000_000_000_000L;
    enum Stats { NODES, DANGLING_NODES, DANGLING_MASS }
    public static class GraphMap extends Mapper<LongWritable,Text,Text,Text> {
        protected void map(LongWritable k, Text v, Context c) throws IOException, InterruptedException {
            String[] p = v.toString().split("\t", -1);
            if (p.length != 2 || p[0].trim().isEmpty()) throw new IOException("Expected page TAB links: " + v);
            String page = p[0].trim();
            TreeSet<String> links = new TreeSet<>();
            for (String s : p[1].split(",")) if (!s.trim().isEmpty()) links.add(s.trim());
            c.write(new Text(page), new Text("A:" + String.join(",", links)));
            for (String s : links) c.write(new Text(s), new Text("T:"));
        }
    }
    public static class GraphReduce extends Reducer<Text,Text,Text,Text> {
        protected void reduce(Text page, Iterable<Text> vs, Context c) throws IOException, InterruptedException {
            TreeSet<String> links = new TreeSet<>();
            for (Text v : vs) if (v.toString().startsWith("A:"))
                for (String s : v.toString().substring(2).split(",")) if (!s.isEmpty()) links.add(s);
            c.getCounter(Stats.NODES).increment(1);
            if (links.isEmpty()) c.getCounter(Stats.DANGLING_NODES).increment(1);
            c.write(page, new Text("1.0\t" + String.join(",", links)));
        }
    }
    public static class RankMap extends Mapper<LongWritable,Text,Text,Text> {
        protected void map(LongWritable k, Text v, Context c) throws IOException, InterruptedException {
            String[] p = v.toString().split("\t", -1);
            double rank = Double.parseDouble(p[1]);
            if (c.getConfiguration().getBoolean("pr.first", false)) rank /= c.getConfiguration().getInt("pr.n", 1);
            c.write(new Text(p[0]), new Text("S:" + p[2]));
            if (!p[2].isEmpty()) {
                String[] links = p[2].split(",");
                for (String dst : links) c.write(new Text(dst), new Text("C:" + (rank / links.length)));
            }
        }
    }
    public static class RankReduce extends Reducer<Text,Text,Text,Text> {
        protected void reduce(Text page, Iterable<Text> vs, Context c) throws IOException, InterruptedException {
            String links = null;
            double sum = 0.0;
            for (Text v : vs) {
                String s = v.toString();
                if (s.startsWith("S:")) links = s.substring(2);
                else sum += Double.parseDouble(s.substring(2));
            }
            if (links == null) throw new IOException("Missing graph structure for " + page);
            Configuration conf = c.getConfiguration();
            int n = conf.getInt("pr.n", 1);
            double rank = (1.0 - 0.85) / n + 0.85 * (sum + conf.getDouble("pr.dangling", 0) / n);
            if (links.isEmpty()) c.getCounter(Stats.DANGLING_MASS).increment(Math.round(rank * SCALE));
            c.write(page, new Text(Double.toString(rank) + "\t" + links));
        }
    }
    public static class FormatMap extends Mapper<LongWritable,Text,Text,DoubleWritable> {
        protected void map(LongWritable k, Text v, Context c) throws IOException, InterruptedException {
            String[] p = v.toString().split("\t", -1);
            c.write(new Text(p[0]), new DoubleWritable(Double.parseDouble(p[1])));
        }
    }
    public static class FormatReduce extends Reducer<Text,DoubleWritable,Text,NullWritable> {
        protected void reduce(Text page, Iterable<DoubleWritable> ranks, Context c) throws IOException, InterruptedException {
            for (DoubleWritable r : ranks) c.write(new Text(String.format(Locale.ROOT, "(%s,%.10f)", page, r.get())), NullWritable.get());
        }
    }
    private static Job job(Configuration base, String name, Class<? extends Mapper> mapper, Class<? extends Reducer> reducer,
                           Class<?> mapValue, Class<?> outKey, Class<?> outValue, Path in, Path out) throws IOException {
        Job j = Job.getInstance(base, name);
        j.setJarByClass(PageRankMR.class);
        j.setMapperClass(mapper); j.setReducerClass(reducer);
        j.setMapOutputKeyClass(Text.class); j.setMapOutputValueClass(mapValue);
        j.setOutputKeyClass(outKey); j.setOutputValueClass(outValue);
        j.setNumReduceTasks(1);
        FileInputFormat.addInputPath(j, in); FileOutputFormat.setOutputPath(j, out);
        return j;
    }
    private static void run(Job j) throws Exception {
        if (!j.waitForCompletion(true)) throw new IOException("Failed: " + j.getJobName());
        System.out.println("COMPLETED " + j.getJobID() + " " + j.getJobName());
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("PageRankMR <input> <new-output-root>");
        Configuration conf = new Configuration();
        conf.setInt("mapreduce.map.memory.mb", 512); conf.set("mapreduce.map.java.opts", "-Xmx384m");
        conf.setInt("mapreduce.reduce.memory.mb", 512); conf.set("mapreduce.reduce.java.opts", "-Xmx384m");
        conf.setInt("yarn.app.mapreduce.am.resource.mb", 512); conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx384m");
        conf.setBoolean("mapreduce.map.speculative", false); conf.setBoolean("mapreduce.reduce.speculative", false);
        Path root = new Path(args[1]);
        if (root.getFileSystem(conf).exists(root)) throw new IOException("Output already exists: " + root);
        Path state = new Path(root, "initial");
        Job init = job(conf, "lab3-graph", GraphMap.class, GraphReduce.class, Text.class, Text.class, Text.class, new Path(args[0]), state);
        run(init);
        int n = Math.toIntExact(init.getCounters().findCounter(Stats.NODES).getValue());
        if (n == 0) throw new IOException("Empty graph");
        double dangling = (double)init.getCounters().findCounter(Stats.DANGLING_NODES).getValue() / n;
        for (int i = 1; i <= 10; i++) {
            conf.setInt("pr.n", n); conf.setBoolean("pr.first", i == 1); conf.setDouble("pr.dangling", dangling);
            Path next = new Path(root, String.format("iter-%02d", i));
            Job j = job(conf, "lab3-pagerank-" + i, RankMap.class, RankReduce.class, Text.class, Text.class, Text.class, state, next);
            run(j);
            dangling = (double)j.getCounters().findCounter(Stats.DANGLING_MASS).getValue() / SCALE;
            System.out.printf(Locale.ROOT, "ITERATION %d N=%d DANGLING=%.15f%n", i, n, dangling);
            state = next;
        }
        run(job(conf, "lab3-format", FormatMap.class, FormatReduce.class, DoubleWritable.class, Text.class, NullWritable.class, state, new Path(root, "result")));
    }
}
