package task_1_4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import java.util.Hashtable;

public class task_1_4_1 {
    public static class CustomFileOutputFormat extends FileOutputFormat<Text, Text> {

        @Override
        public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job)
                throws IOException, InterruptedException {
            Configuration conf = job.getConfiguration();
            String customFileName = "task_1_4_1.mtx";
            Path outputDir = FileOutputFormat.getOutputPath(job);
            Path fullOutputPath = new Path(outputDir, customFileName);

            FileSystem fs = fullOutputPath.getFileSystem(conf);
            FSDataOutputStream fileOut = fs.create(fullOutputPath, false);

            return new RecordWriter<Text, Text>() {
                @Override
                public void write(Text key, Text value) throws IOException, InterruptedException {
                    String line = key.toString() + " " + value.toString() + "\n"; // Write key-value pair as a line
                    fileOut.writeBytes(line);
                }

                @Override
                public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                    fileOut.close();
                }
            };
        }
    }
    public static class Map
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+");
            context.write(new Text(parts[1]), new Text(parts[0]+ " "+parts[2]));
        }
    }
    public static class Reduce
            extends Reducer<Text, Text, Text, Text> {
        static Hashtable<String, Integer> file = new Hashtable<String, Integer>();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (Text val: values){
                String[] parts = val.toString().split("\\s+");
                int val_num = Integer.parseInt(parts[1]);
                sum+=val_num;
            }
            context.write(key, new Text(String.valueOf(sum)));
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MTX Filter Job");
        job.setJarByClass(task_1_4_1.class);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(new Path(args[1]))){
            fs.delete(new Path(args[1]),true);
        }
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(CustomFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
