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

public class WordCountLeastNFreq {

    // =========================
    // MAPPER
    // =========================

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one =
                new IntWritable(1);

        private Text word = new Text();

        public void map(Object key,
                        Text value,
                        Context context)
                throws IOException, InterruptedException {

            StringTokenizer itr =
                    new StringTokenizer(
                            value.toString()
                    );

            while (itr.hasMoreTokens()) {

                word.set(
                        itr.nextToken().toLowerCase()
                );

                context.write(word, one);
            }
        }
    }

    // =========================
    // REDUCER
    // =========================

    public static class IntSumReducer
            extends Reducer<Text, IntWritable,
            Text, IntWritable> {

        // Store frequencies
        private Map<String, Integer> wordMap =
                new HashMap<>();

        public void reduce(Text key,
                           Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable val : values) {

                sum += val.get();
            }

            wordMap.put(
                    key.toString(),
                    sum
            );
        }

        // =========================
        // FIND Nth LEAST FREQUENT
        // =========================

        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            // Change this value for Nth least word
            int N = 10;

            // Convert map to list
            List<Map.Entry<String, Integer>> list =
                    new ArrayList<>(
                            wordMap.entrySet()
                    );

            // Sort ASCENDING
            Collections.sort(
                    list,
                    (a, b) ->
                            a.getValue() - b.getValue()
            );

            // Print all words
            context.write(
                    new Text("WORD"),
                    new IntWritable(-1)
            );

            for (Map.Entry<String, Integer> entry : list) {

                context.write(
                        new Text(entry.getKey()),
                        new IntWritable(
                                entry.getValue()
                        )
                );
            }

            // Find Nth least frequent word
            if (list.size() >= N) {

                Map.Entry<String, Integer> nth =
                        list.get(N - 1);

                context.write(
                        new Text(
                                N +
                                "th Least Frequent Word: "
                                + nth.getKey()
                        ),
                        new IntWritable(
                                nth.getValue()
                        )
                );

            } else {

                context.write(
                        new Text(
                                "Less than "
                                + N
                                + " unique words"
                        ),
                        new IntWritable(0)
                );
            }
        }
    }

    // =========================
    // DRIVER CODE
    // =========================

    public static void main(String[] args)
            throws Exception {

        Configuration conf =
                new Configuration();

        Job job = Job.getInstance(
                conf,
                "Nth Least Frequent Word"
        );

        job.setJarByClass(
                WordCountLeastNFreq.class
        );

        job.setMapperClass(
                TokenizerMapper.class
        );

        job.setReducerClass(
                IntSumReducer.class
        );

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(
                IntWritable.class
        );

        FileInputFormat.addInputPath(
                job,
                new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1])
        );

        System.exit(
                job.waitForCompletion(true)
                        ? 0 : 1
        );
    }
}

/*

========================================
PREREQUISITES
========================================

in VS Code terminal:

java -version

8 should come


echo $JAVA_HOME

8 should come


if not:

nano $HADOOP_HOME/etc/hadoop/hadoop-env.sh

set JAVA_HOME to java 8 path


check hadoop:

hadoop version


========================================
START HADOOP
========================================

start-dfs.sh

start-yarn.sh


check:

jps

should show:
NameNode
DataNode
SecondaryNameNode
ResourceManager
NodeManager


========================================
IF HADOOP NOT WORKING
========================================

stop-all.sh

start-all.sh


still broken:

hdfs namenode -format

start-all.sh


leave safe mode:

hdfs dfsadmin -safemode leave


========================================
NOW RUN PROGRAM
========================================

Write program in VS Code

Save as:

WordCountLeastNFreq.java


Go to folder:

cd Desktop

cd BDA


========================================
CREATE INPUT FILE
========================================

nano words.txt


Example:

hello world hello java
java python java hello
hadoop spark spark sql
sql hive hive hive


Save:
CTRL + X
Y
ENTER


========================================
CREATE HDFS DIRECTORY
========================================

hdfs dfs -mkdir /exp5


check:

hdfs dfs -ls /


========================================
PUT FILE IN HDFS
========================================

hdfs dfs -put words.txt /exp5


verify:

hdfs dfs -ls /exp5


========================================
COMPILE PROGRAM
========================================

rm -rf classes

mkdir classes


javac -classpath $(hadoop classpath) \
-d classes WordCountLeastNFreq.java


check class files:

ls classes


========================================
CREATE JAR
========================================

jar -cvf exp5.jar -C classes/ .


verify:

ls


========================================
RUN MAPREDUCE
========================================

hadoop jar exp5.jar \
WordCountLeastNFreq \
/exp5/words.txt \
/output/leastfreq


========================================
VIEW OUTPUT
========================================

hdfs dfs -cat /output/leastfreq/part-r-00000


========================================
IF OUTPUT DIRECTORY EXISTS
========================================

hdfs dfs -rm -r /output/leastfreq


Run again:

hadoop jar exp5.jar \
WordCountLeastNFreq \
/exp5/words.txt \
/output/leastfreq


========================================
CHECK OUTPUT FILES
========================================

hdfs dfs -ls /output/leastfreq


========================================
STOP HADOOP (OPTIONAL)
========================================

stop-dfs.sh

stop-yarn.sh


========================================
IMPORTANT
========================================

Inside code:

int N = 1;

means:
1st least frequent word


int N = 2;

means:
2nd least frequent word


int N = 10;

means:
10th least frequent word

========================================

*/