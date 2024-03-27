package task_1_2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.example.KHMT1_06;

public abstract class task_1_2{

    public static class CustomFileOutputFormat extends FileOutputFormat<Text, IntWritable> {

        @Override
        public RecordWriter<Text, IntWritable> getRecordWriter(TaskAttemptContext job)
                throws IOException, InterruptedException {
            Configuration conf = job.getConfiguration();
            // Customizing the file name by using the job name and task id
            //String customFileName = "task_1_1" + job.getTaskAttemptID().getTaskID().getId() + ".mtx";
            String customFileName = "task_1_2.mtx";
            // Customize the output directory and file name as needed
            Path outputDir = FileOutputFormat.getOutputPath(job);
            Path fullOutputPath = new Path(outputDir, customFileName);

            // Create the file in the file system
            FileSystem fs = fullOutputPath.getFileSystem(conf);
            FSDataOutputStream fileOut = fs.create(fullOutputPath, false);

            return new RecordWriter<Text, IntWritable>() {
                @Override
                public void write(Text key, IntWritable value) throws IOException, InterruptedException {
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
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+");
            if (parts.length == 3) {
                int frequency = Integer.parseInt(parts[2]);
                if (frequency >= 3) {
                    word.set(parts[0] + " " + parts[1]);
                    context.write(word, new IntWritable(frequency));
                }
            }
        }
    }
    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MTX Filter Job");
        job.setJarByClass(task_1_2.class);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(new Path(args[1]))){
            fs.delete(new Path(args[1]),true);
        }
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(task_1_2.CustomFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}