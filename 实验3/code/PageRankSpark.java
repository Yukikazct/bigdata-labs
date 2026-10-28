import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

/** Java RDD PageRank, with a YARN executor and ten synchronous iterations. */
public class PageRankSpark {
    private static Tuple2<String,Set<String>> parse(String line) {
        String[] p = line.split("\t", -1);
        if (p.length != 2 || p[0].trim().isEmpty()) throw new IllegalArgumentException("Expected page TAB links: " + line);
        Set<String> links = new TreeSet<>();
        for (String s : p[1].split(",")) if (!s.trim().isEmpty()) links.add(s.trim());
        return new Tuple2<>(p[0].trim(), links);
    }
    public static void main(String[] args) {
        if (args.length != 2) throw new IllegalArgumentException("PageRankSpark <input> <new-output-root>");
        try (JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("lab3-pagerank-spark"))) {
            sc.setLogLevel("WARN");
            JavaPairRDD<String,Set<String>> records = sc.textFile(args[0], 2).mapToPair(PageRankSpark::parse).cache();
            JavaRDD<String> nodes = records.flatMap(p -> {
                List<String> all = new ArrayList<>(p._2()); all.add(p._1()); return all.iterator();
            }).distinct().cache();
            JavaPairRDD<String,Set<String>> adjacency = records.reduceByKey((a,b) -> {
                Set<String> union = new TreeSet<>(a); union.addAll(b); return union;
            });
            JavaPairRDD<String,Set<String>> graph = nodes.mapToPair(x -> new Tuple2<>(x, 0))
                .leftOuterJoin(adjacency).mapValues(p -> p._2().orElse(Collections.emptySet()))
                .persist(StorageLevel.MEMORY_AND_DISK());
            final long n = nodes.count();
            if (n == 0) throw new IllegalArgumentException("Empty graph");
            JavaPairRDD<String,Double> ranks = graph.mapValues(x -> 1.0 / n).cache();
            List<String> history = new ArrayList<>();
            for (int i = 1; i <= 10; i++) {
                JavaPairRDD<String,Tuple2<Set<String>,Double>> joined = graph.join(ranks).cache();
                final double dangling = joined.values().filter(x -> x._1().isEmpty()).mapToDouble(Tuple2::_2).sum();
                JavaPairRDD<String,Double> contributions = joined.flatMapToPair(p -> {
                    List<Tuple2<String,Double>> shares = new ArrayList<>();
                    for (String dst : p._2()._1()) shares.add(new Tuple2<>(dst, p._2()._2() / p._2()._1().size()));
                    return shares.iterator();
                }).reduceByKey(Double::sum);
                JavaPairRDD<String,Double> next = nodes.mapToPair(x -> new Tuple2<>(x, 0.0))
                    .leftOuterJoin(contributions).mapValues(p -> 0.15 / n + 0.85 * (p._2().orElse(0.0) + dangling / n)).cache();
                double mass = next.values().mapToDouble(x -> x).sum();
                String entry = String.format(Locale.ROOT,
                    "{\"iteration\":%d,\"nodes\":%d,\"dangling_input\":%.17g,\"rank_sum\":%.17g}", i, n, dangling, mass);
                history.add(entry);
                System.out.println("ITERATION " + entry);
                ranks.unpersist(); joined.unpersist(); ranks = next;
            }
            ranks.sortByKey(true, 1).map(p -> String.format(Locale.ROOT, "(%s,%.10f)", p._1(), p._2()))
                .saveAsTextFile(args[1] + "/result");
            sc.parallelize(Collections.singletonList("[" + String.join(",\n", history) + "]"), 1)
                .saveAsTextFile(args[1] + "/history");
            System.out.println("SPARK_APPLICATION_ID " + sc.sc().applicationId());
        }
    }
}
