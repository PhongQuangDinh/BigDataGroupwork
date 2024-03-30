package task_1_5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.awt.datatransfer.FlavorEvent;
import java.io.IOException;
import java.util.*;

public class task_1_5 {
    public static class CustomFileOutputFormat<K, V> extends TextOutputFormat<K, V> {
        @Override
        public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
            // Customize the file name here
            String customFileName = "task_1_5.txt";
            if (extension != null) {
                customFileName += extension;
            }

            // Create a new Path object with the custom file name
            Path outputDir = getOutputPath(context);
            return new Path(outputDir, customFileName);
        }
    }
    public static class TermFrequencyMapper
            extends Mapper<LongWritable, Text, Text, FloatWritable> {
        private Text term = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+");
            if (parts.length == 3) {
                term.set(parts[0] + " " + parts[1]);
                float frequency = Float.parseFloat(parts[2]);
                context.write(term, new FloatWritable(frequency));
            }
        }
    }

    public static class FloatSumReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        private final Map<String, TreeMap<Float, List<Text>>> topFrequencies = new HashMap<>();

        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0.0f;
            for (FloatWritable val : values) {
                sum += val.get();
            }

            String[] parts = key.toString().split(" ");
            String type = parts[1];

            if (!topFrequencies.containsKey(type)) {
                topFrequencies.put(type, new TreeMap<>(Collections.reverseOrder()));
            }

            TreeMap<Float, List<Text>> frequencyMap = topFrequencies.get(type);
            frequencyMap.computeIfAbsent(sum, k -> new ArrayList<>()).add(new Text(key));

            if (frequencyMap.size() > 5) {
                frequencyMap.remove(frequencyMap.lastKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, TreeMap<Float, List<Text>>> typeEntry : topFrequencies.entrySet()) {
                String type = typeEntry.getKey();
                TreeMap<Float, List<Text>> frequencyMap = typeEntry.getValue();

                int counter = 0;
                for (Map.Entry<Float, List<Text>> entry : frequencyMap.entrySet()) {
                    for (Text term : entry.getValue()) {
                        if (counter++ < 5) {
                            context.write(new Text(term + " " + type), new FloatWritable(entry.getKey()));
                        }
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "top terms");
        job.setJarByClass(task_1_5.class);
        job.setMapperClass(TermFrequencyMapper.class);
        //job.setReducerClass(task_1_5.TopPerTypeReducer.class);
        job.setReducerClass(FloatSumReducer.class);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(new Path(args[1]))){
            fs.delete(new Path(args[1]),true);
        }
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setOutputFormatClass(task_1_5.CustomFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
