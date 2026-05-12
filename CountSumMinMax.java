import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GenericDatasetAnalysis {

    // ==================================
    // JOB 1 : COUNT CATEGORY FREQUENCY
    // ==================================

    public static class CountMapper
            extends Mapper<LongWritable, Text,
            Text, DoubleWritable> {

        private boolean headerSkipped = false;

        public void map(LongWritable key,
                        Text value,
                        Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            if (!headerSkipped) {
                headerSkipped = true;
                return;
            }

            String[] fields = line.split(",");

            try {

                // CHANGE THIS COLUMN INDEX
                // Example:
                // fields[1] = Product
                // fields[2] = Region
                // fields[3] = Category

                String category = fields[1].trim();

                context.write(
                        new Text(category),
                        new DoubleWritable(1)
                );

            } catch (Exception e) {
            }
        }
    }

    public static class CountReducer
            extends Reducer<Text, DoubleWritable,
            Text, DoubleWritable> {

        public void reduce(Text key,
                           Iterable<DoubleWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            double sum = 0;

            for (DoubleWritable val : values) {
                sum += val.get();
            }

            context.write(key,
                    new DoubleWritable(sum));
        }
    }

    // ================================
    // JOB 2 : SUM NUMERIC COLUMN
    // ================================

    public static class SumMapper
            extends Mapper<LongWritable, Text,
            Text, DoubleWritable> {

        private boolean headerSkipped = false;

        public void map(LongWritable key,
                        Text value,
                        Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            if (!headerSkipped) {
                headerSkipped = true;
                return;
            }

            String[] fields = line.split(",");

            try {

                // CHANGE CATEGORY COLUMN INDEX
                String category = fields[1].trim();

                // CHANGE NUMERIC COLUMN INDEX
                // Example:
                // fields[3] = Sales
                // fields[4] = Revenue
                // fields[5] = ROI

                double number =
                        Double.parseDouble(fields[3].trim());

                context.write(
                        new Text(category),
                        new DoubleWritable(number)
                );

            } catch (Exception e) {
            }
        }
    }

    public static class SumReducer
            extends Reducer<Text, DoubleWritable,
            Text, DoubleWritable> {

        public void reduce(Text key,
                           Iterable<DoubleWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            double total = 0;

            for (DoubleWritable val : values) {
                total += val.get();
            }

            context.write(
                    key,
                    new DoubleWritable(total)
            );
        }
    }

    // ================================
    // JOB 3 : MAX MIN AVG
    // ================================

    public static class StatsMapper
            extends Mapper<LongWritable, Text,
            Text, DoubleWritable> {

        private boolean headerSkipped = false;

        public void map(LongWritable key,
                        Text value,
                        Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            if (!headerSkipped) {
                headerSkipped = true;
                return;
            }

            String[] fields = line.split(",");

            try {

                // CHANGE CATEGORY COLUMN INDEX
                String category = fields[1].trim();

                // CHANGE NUMERIC COLUMN INDEX
                double num =
                        Double.parseDouble(fields[3].trim());

                context.write(
                        new Text(category),
                        new DoubleWritable(num)
                );

            } catch (Exception e) {
            }
        }
    }

    public static class StatsReducer
            extends Reducer<Text, DoubleWritable,
            Text, Text> {

        public void reduce(Text key,
                           Iterable<DoubleWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            double max = Double.MIN_VALUE;
            double min = Double.MAX_VALUE;
            double sum = 0;

            int count = 0;

            for (DoubleWritable val : values) {

                double num = val.get();

                if (num > max)
                    max = num;

                if (num < min)
                    min = num;

                sum += num;
                count++;
            }

            double avg = sum / count;

            context.write(
                    key,
                    new Text(
                            "Max=" + max +
                            " Min=" + min +
                            " Avg=" + avg
                    )
            );
        }
    }

    // ================================
    // MAIN FUNCTION
    // ================================

    public static void main(String[] args)
            throws Exception {

        Configuration conf = new Configuration();

        // ===== JOB 1 =====

        Job job1 = Job.getInstance(conf, "Count");

        job1.setJarByClass(GenericDatasetAnalysis.class);

        job1.setMapperClass(CountMapper.class);
        job1.setReducerClass(CountReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(
                job1,
                new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
                job1,
                new Path(args[1])
        );

        job1.waitForCompletion(true);

        // ===== JOB 2 =====

        Job job2 = Job.getInstance(conf, "Sum");

        job2.setJarByClass(GenericDatasetAnalysis.class);

        job2.setMapperClass(SumMapper.class);
        job2.setReducerClass(SumReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(
                job2,
                new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
                job2,
                new Path(args[2])
        );

        job2.waitForCompletion(true);

        // ===== JOB 3 =====

        Job job3 = Job.getInstance(conf, "Stats");

        job3.setJarByClass(GenericDatasetAnalysis.class);

        job3.setMapperClass(StatsMapper.class);
        job3.setReducerClass(StatsReducer.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(
                job3,
                new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
                job3,
                new Path(args[3])
        );

        System.exit(
                job3.waitForCompletion(true)
                        ? 0 : 1
        );
    }
}

/*

==================================================
HOW TO CHANGE FOR ANY DATASET
==================================================

Suppose Dataset Columns Are:

ID,Product,Region,Sales

Then:

fields[1] = Product
fields[2] = Region
fields[3] = Sales

Change only these lines:

String category = fields[1].trim();

double number =
Double.parseDouble(fields[3].trim());

==================================================
HOW TO EXECUTE
==================================================

1. START HADOOP

start-dfs.sh
start-yarn.sh
jps


2. CREATE HDFS DIRECTORY

hdfs dfs -mkdir /generic


3. PUT DATASET

hdfs dfs -put dataset.csv /generic


4. COMPILE JAVA FILE

mkdir classes

javac -classpath $(hadoop classpath) \
-d classes GenericDatasetAnalysis.java


5. CREATE JAR FILE

jar -cvf generic.jar -C classes/ .


6. RUN PROGRAM

hadoop jar generic.jar GenericDatasetAnalysis \
/generic/dataset.csv \
/generic/output1 \
/generic/output2 \
/generic/output3


7. VIEW OUTPUTS

--- COUNT OUTPUT ---

hdfs dfs -cat /generic/output1/part-r-00000


--- SUM OUTPUT ---

hdfs dfs -cat /generic/output2/part-r-00000


--- MAX MIN AVG OUTPUT ---

hdfs dfs -cat /generic/output3/part-r-00000

==================================================
WHAT THIS PROGRAM DOES
==================================================

1. Counts category frequency
2. Sums numeric values
3. Calculates Max Min Avg

Works for almost any CSV dataset
by changing only column indexes.

A tiny attempt to make Hadoop less annoying.
Only partially successful.

*/