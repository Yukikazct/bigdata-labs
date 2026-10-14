import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** Two MapReduce jobs: (word, document) counts, then word posting lists. */
public class InvertedIndex extends Configured implements Tool {
    public enum Stats { RAW_TOKENS, STOP_WORDS, INDEXED_TOKENS, OUTPUT_TERMS }

    public static class TokenMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private static final Pattern WORD = Pattern.compile("[a-z]+");
        private static final LongWritable ONE = new LongWritable(1);
        private final Set<String> stopWords = new HashSet<>();
        private final Text pair = new Text();
        private String document;

        @Override
        protected void setup(Context context) throws IOException {
            document = ((FileSplit) context.getInputSplit()).getPath().getName();
            // Hadoop's distributed cache creates this symlink in each task directory.
            try (BufferedReader reader = new BufferedReader(new FileReader("stopwords.txt"))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim().toLowerCase(Locale.ROOT);
                    if (!line.isEmpty() && !line.startsWith("#")) stopWords.add(line);
                }
            }
        }

        @Override
        protected void map(LongWritable offset, Text line, Context context)
                throws IOException, InterruptedException {
            Matcher matcher = WORD.matcher(line.toString().toLowerCase(Locale.ROOT));
            while (matcher.find()) {
                String word = matcher.group();
                context.getCounter(Stats.RAW_TOKENS).increment(1);
                if (stopWords.contains(word)) {
                    context.getCounter(Stats.STOP_WORDS).increment(1);
                    continue;
                }
                context.getCounter(Stats.INDEXED_TOKENS).increment(1);
                pair.set(word + "\t" + document);
                context.write(pair, ONE);
            }
        }
    }

    /** Associative integer addition is valid both as combiner and reducer. */
    public static class CountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private final LongWritable count = new LongWritable();
        @Override
        protected void reduce(Text pair, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) sum += value.get();
            count.set(sum);
            context.write(pair, count);
        }
    }

    public static class PostingMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text word = new Text();
        private final Text posting = new Text();
        @Override
        protected void map(LongWritable offset, Text line, Context context)
                throws IOException, InterruptedException {
            String[] fields = line.toString().split("\t", -1);
            if (fields.length != 3) throw new IOException("Invalid count record: " + line);
            word.set(fields[0]);
            posting.set(fields[1] + "\t" + fields[2]);
            context.write(word, posting);
        }
    }

    public static class PostingReducer extends Reducer<Text, Text, Text, Text> {
        private final Text result = new Text();
        @Override
        protected void reduce(Text word, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            TreeMap<String, Long> documents = new TreeMap<>();
            for (Text value : values) {
                String[] fields = value.toString().split("\t", -1);
                if (fields.length != 2) throw new IOException("Invalid posting: " + value);
                long count = Long.parseLong(fields[1]);
                documents.merge(fields[0], count, Long::sum);
            }
            StringBuilder postings = new StringBuilder();
            long total = 0;
            for (Map.Entry<String, Long> entry : documents.entrySet()) {
                postings.append('<').append(entry.getKey()).append(',')
                        .append(entry.getValue()).append(">;");
                total += entry.getValue();
            }
            postings.append("<total,").append(total).append(">.");
            result.set(postings.toString());
            context.write(word, result);
            context.getCounter(Stats.OUTPUT_TERMS).increment(1);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 4 || args.length > 5) {
            System.err.println("Usage: InvertedIndex <input> <counts> <output> <stopwords> [countReducers]");
            return 2;
        }
        int reducers = args.length == 5 ? Integer.parseInt(args[4]) : 2;
        if (reducers < 1) throw new IllegalArgumentException("countReducers must be positive");
        Path input = new Path(args[0]);
        Path counts = new Path(args[1]);
        Path output = new Path(args[2]);
        for (Path path : new Path[] {counts, output}) {
            if (path.getFileSystem(getConf()).exists(path)) {
                throw new IOException("Output path already exists: " + path);
            }
        }

        Job first = Job.getInstance(getConf(), "lab2-word-document-counts");
        first.setJarByClass(InvertedIndex.class);
        first.addCacheFile(new URI(new Path(args[3]).toUri().toString() + "#stopwords.txt"));
        first.setMapperClass(TokenMapper.class);
        first.setCombinerClass(CountReducer.class);
        first.setReducerClass(CountReducer.class);
        first.setOutputKeyClass(Text.class);
        first.setOutputValueClass(LongWritable.class);
        first.setNumReduceTasks(reducers);
        FileInputFormat.addInputPath(first, input);
        FileOutputFormat.setOutputPath(first, counts);
        if (!first.waitForCompletion(true)) return 1;

        Job second = Job.getInstance(getConf(), "lab2-inverted-index");
        second.setJarByClass(InvertedIndex.class);
        second.setMapperClass(PostingMapper.class);
        second.setReducerClass(PostingReducer.class);
        second.setOutputKeyClass(Text.class);
        second.setOutputValueClass(Text.class);
        second.setNumReduceTasks(1); // Produces one globally sorted word index.
        FileInputFormat.addInputPath(second, counts);
        FileOutputFormat.setOutputPath(second, output);
        return second.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new InvertedIndex(), args));
    }
}
