import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class RelationalAlgebra {

    public static class SelectionMapper
            extends Mapper<LongWritable, Text, Text, NullWritable> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");

            if (fields.length == 3 && fields[2].equals("CS")) {
                context.write(value, NullWritable.get());
            }
        }
    }

    public static class SelectionReducer
            extends Reducer<Text, NullWritable, Text, NullWritable> {

        public void reduce(Text key, Iterable<NullWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            context.write(key, NullWritable.get());
        }
    }

    public static class ProjectionMapper
            extends Mapper<LongWritable, Text, Text, NullWritable> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");

            if (fields.length >= 2)
                context.write(new Text(fields[1]), NullWritable.get());
        }
    }

    public static class ProjectionReducer
            extends Reducer<Text, NullWritable, Text, NullWritable> {

        public void reduce(Text key, Iterable<NullWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            context.write(key, NullWritable.get());
        }
    }

    public static class JoinMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String fileName =
                    ((FileSplit) context.getInputSplit())
                            .getPath().getName();

            String[] fields = value.toString().split(",");

            if (fileName.contains("Student")) {

                context.write(
                        new Text(fields[0]),
                        new Text("S," + fields[1] + "," + fields[2])
                );

            } else {

                context.write(
                        new Text(fields[0]),
                        new Text("M," + fields[1])
                );
            }
        }
    }

    public static class JoinReducer
            extends Reducer<Text, Text, Text, NullWritable> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context)
                throws IOException, InterruptedException {

            String nameDept = "";
            String marks = "";

            for (Text val : values) {

                String[] parts = val.toString().split(",");

                if (parts[0].equals("S"))
                    nameDept = parts[1] + "," + parts[2];
                else
                    marks = parts[1];
            }

            context.write(
                    new Text(key + " " + nameDept + "," + marks),
                    NullWritable.get()
            );
        }
    }

    public static class IntersectionMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            context.write(value, new IntWritable(1));
        }
    }

    public static class IntersectionReducer
            extends Reducer<Text, IntWritable, Text, NullWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            int count = 0;

            for (IntWritable val : values)
                count++;

            if (count > 1)
                context.write(key, NullWritable.get());
        }
    }

    public static class DifferenceReducer
            extends Reducer<Text, IntWritable, Text, NullWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            int count = 0;

            for (IntWritable val : values)
                count++;

            if (count == 1)
                context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Relational Algebra");

        job.setJarByClass(RelationalAlgebra.class);

        String operation = args[0];

        switch (operation) {

            case "selection":

                job.setMapperClass(SelectionMapper.class);
                job.setReducerClass(SelectionReducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(NullWritable.class);

                break;

            case "projection":

                job.setMapperClass(ProjectionMapper.class);
                job.setReducerClass(ProjectionReducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(NullWritable.class);

                break;

            case "join":

                job.setMapperClass(JoinMapper.class);
                job.setReducerClass(JoinReducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                break;

            case "intersection":

                job.setMapperClass(IntersectionMapper.class);
                job.setReducerClass(IntersectionReducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);

                break;

            case "difference":

                job.setMapperClass(IntersectionMapper.class);
                job.setReducerClass(DifferenceReducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);

                break;

            default:

                System.out.println("Invalid operation");
                System.exit(1);
        }

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

/*

=========== EXECUTION COMMANDS ===========

1. Start Hadoop

start-dfs.sh
start-yarn.sh
jps

2. Create HDFS Directory

hdfs dfs -mkdir /ra

3. Put Dataset

hdfs dfs -put Student.txt /ra
hdfs dfs -put Marks.txt /ra

4. Compile

mkdir classes

javac -classpath $(hadoop classpath) -d classes RelationalAlgebra.java

5. Create JAR

jar -cvf relational.jar -C classes/ .

6. Run Selection

hadoop jar relational.jar RelationalAlgebra selection /ra/Student.txt /output/selection

7. View Output

hdfs dfs -cat /output/selection/part-r-00000

*/