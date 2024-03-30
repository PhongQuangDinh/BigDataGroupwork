package task_1_3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.example.KHMT1_06;

import java.io.IOException;
import java.util.*;

public class task_1_3 {
        public static class CustomFileOutputFormat<K, V> extends TextOutputFormat<K, V> {
        @Override
        public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
            // Customize the file name here
            String customFileName = "task_1_3.txt";
            if (extension != null) {
                customFileName += extension;
            }

            // Create a new Path object with the custom file name
            Path outputDir = getOutputPath(context);
            return new Path(outputDir, customFileName);
        }
    }
    public static class TermFrequencyMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text term = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+");
            if (parts.length == 3) {
                term.set(parts[0]); // termid is the first part
                int frequency = Integer.parseInt(parts[2]);
                context.write(term, new IntWritable(frequency));
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private TreeMap<Integer, List<Text>> frequencyMap = new TreeMap<>(Collections.reverseOrder());

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            frequencyMap.computeIfAbsent(sum, k -> new ArrayList<>()).add(new Text(key));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int counter = 0;
            for (Map.Entry<Integer, List<Text>> entry : frequencyMap.entrySet()) {
                for (Text term : entry.getValue()) {
                    if (counter++ < 10) {
                        context.write(term, new IntWritable(entry.getKey()));
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "top terms");
        job.setJarByClass(task_1_3.class);
        job.setMapperClass(TermFrequencyMapper.class);
        job.setReducerClass(IntSumReducer.class);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(new Path(args[1]))){
            fs.delete(new Path(args[1]),true);
        }
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(task_1_3.CustomFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
