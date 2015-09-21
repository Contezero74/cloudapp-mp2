import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PopularityLeague extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
       
	return everything.toString();
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }
  

    @Override
    public int run(String[] args) throws Exception {
	Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mp2/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Popularity League Count");
        jobA.setOutputKeyClass(IntWritable.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(LinkCountMap.class);
        jobA.setReducerClass(LinkCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(PopularityLeague.class);
	jobA.waitForCompletion(true);

	Job jobB = Job.getInstance(conf, "Popularity League Rank");
	jobB.setOutputKeyClass(IntWritable.class);
        jobB.setOutputValueClass(IntWritable.class);

	jobB.setMapOutputKeyClass(NullWritable.class);
	jobB.setMapOutputValueClass(IntArrayWritable.class);

	jobB.setMapperClass(LinkRankMap.class);
	jobB.setReducerClass(LinkRankReduce.class);
	jobB.setNumReduceTasks(1);

	FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

	jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

	jobB.setJarByClass(PopularityLeague.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
	List<String> pageIds;

	@Override
        protected void setup(Context context) throws IOException,InterruptedException {
		Configuration conf = context.getConfiguration();

		String pageIdsPath = conf.get("league");
		pageIds = Arrays.asList(readHDFSFile(pageIdsPath, conf).split("\n"));	
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                final String line = value.toString();
                final String pages[] =  line.split("[:]");

                final String links[] = pages[1].split("[ ]");
                for(String l : links) {
                        if (!l.trim().isEmpty() && pageIds.contains(l)) {
                                Integer linkId = Integer.parseInt(l.trim());
                                context.write(new IntWritable(linkId), new IntWritable(1));
                        }
                }
        }
    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                int numLink = 0;
                for (IntWritable val : values) {
                        numLink += val.get();
                }

                context.write(key, new IntWritable(numLink));
        }
    }

    public static class LinkRankMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
        TreeSet<Pair<Integer, Integer>> countToRankMap = new TreeSet<Pair<Integer, Integer>>();

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
                Integer count = Integer.parseInt(value.toString());
                Integer id = Integer.parseInt(key.toString());

                countToRankMap.add(new Pair<Integer, Integer>(count, id));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
		int rank = -1;
		Integer lastScore = null;
                for (Pair<Integer, Integer> item : countToRankMap) {
			LOG.info("map: <" + item.second + ": " + item.first + ">; rank: " + rank + "; lastScore: " + lastScore + "; item.first ! =lastScore: " + (!item.first.equals(lastScore)));

			if (null == lastScore || !item.first.equals(lastScore)) {
				lastScore = item.first;
				++rank;
			}
			
                        Integer[] integers = { item.second, rank, item.first };
                        IntArrayWritable val = new IntArrayWritable(integers);
                        context.write(NullWritable.get(), val);
                }
        }
    }

    public static class LinkRankReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
                for (IntArrayWritable val: values) {
                        IntWritable[] pair = (IntWritable[]) val.toArray();
                        IntWritable id = pair[0];
                        IntWritable rank = pair[1];
			IntWritable score = pair[2];

			LOG.info("reduce: id: " + id.get() + "; score: " + score.get() + "; rank: " + rank.get());

			context.write(id, rank);
                }
        }
    }

    private static final Log LOG = LogFactory.getLog(PopularityLeague.class);
}

class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}

