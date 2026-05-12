import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LeastNFrequentWords {

    // Mapper
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    // Reducer
    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private Map<String, Integer> map = new HashMap<>();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            map.put(key.toString(), sum);
        }

        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            List<Map.Entry<String, Integer>> list =
                    new ArrayList<>(map.entrySet());

            // Ascending order
            list.sort((a, b) -> a.getValue() - b.getValue());

            int N = 3; // Least N words

            for (int i = 0; i < Math.min(N, list.size()); i++) {
                Map.Entry<String, Integer> entry = list.get(i);

                context.write(
                        new Text(entry.getKey()),
                        new IntWritable(entry.getValue())
                );
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Least N Frequent Words");

        job.setJarByClass(LeastNFrequentWords.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}