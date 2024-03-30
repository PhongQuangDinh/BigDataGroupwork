package task_1_5;
import org.apache.commons.math3.util.Precision;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Hashtable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class avg_tfidf {
    public static class CustomFileOutputFormat<K, V> extends TextOutputFormat<K, V> {
        @Override
        public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
            // Customize the file name here
            String customFileName = "avg_tfidf.txt";
            if (extension != null) {
                customFileName += extension;
            }
            Path outputDir = getOutputPath(context);
            return new Path(outputDir, customFileName);
        }
    }

    public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {
        //        private Text word = new Text();
        private String input;

        static Hashtable<String, Integer> file_num = new Hashtable<>();

        protected void setup(Mapper.Context context)
                throws IOException,
                InterruptedException {
            if (context.getInputSplit() instanceof FileSplit) {
                this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
            } else {
                this.input = context.getInputSplit().toString();
            }
            Configuration config = context.getConfiguration();
            if (config.getBoolean("file", false)) {
                URI[] localPaths = context.getCacheFiles();
                addFile(localPaths[0]);
            }
        }
        private void addFile(URI patternsURI) {
            try {
                BufferedReader fis = new BufferedReader(new FileReader(new File(patternsURI.getPath()).getName()));
                String pattern;
                while ((pattern = fis.readLine()) != null) {
                    String[] parts = pattern.toString().split("\\s+");
                    file_num.put(parts[0], Integer.parseInt(parts[1]));
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + patternsURI + "' : " + StringUtils.stringifyException(ioe));
            }
        }
        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            String[] parts = lineText.toString().split("\\s+");
            String[] part1 = parts[1].split("/");

            String key = parts[0] + " " + part1[0];

            Integer fileNumValue = file_num.get(part1[0]); // Replace "key" with your desired key from the Hashtable

            if (fileNumValue != null) {
                float tfidf = Float.parseFloat(parts[2]);
                float result = tfidf / fileNumValue;
                context.write(new Text(key), new FloatWritable(result));
            }
        }
    }
    public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            float sum = 0.0f;
            for (FloatWritable val : values) {
                sum += val.get();
            }
            context.write(key, new FloatWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "wordcount");
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(new Path(args[1]))){
            fs.delete(new Path(args[1]),true);
        }
        for (int i = 0; i < args.length; i += 1) {
            if ("-skip".equals(args[i])) {
                job.getConfiguration().setBoolean("file", true);
                i += 1;
                job.addCacheFile(new Path(args[i]).toUri());
            }
        }
        job.setJarByClass(avg_tfidf.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        job.setOutputFormatClass(CustomFileOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
