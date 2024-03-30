package task_1_4;
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
import java.text.DecimalFormat;
import java.util.Hashtable;
import java.util.List;

import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

public class task_1_4 {
    private static final Logger LOG = Logger.getLogger(tf.class);
    public static class CustomFileOutputFormat extends FileOutputFormat<Text, Text> {

        @Override
        public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job)
                throws IOException, InterruptedException {
            Configuration conf = job.getConfiguration();
            String customFileName = "task_1_4.mtx";
            // Customize the output directory and file name as needed
            Path outputDir = FileOutputFormat.getOutputPath(job);
            Path fullOutputPath = new Path(outputDir, customFileName);

            // Create the file in the file system
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

    public static class Map extends Mapper<Object, Text, Text, Text> {
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
            LOG.info("Added file to the distributed cache: " + patternsURI);
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

        public void map(Object offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            String[] parts = lineText.toString().split("\\s+");
            int frequency = Integer.parseInt(parts[2]);
            int wordsPerFile = Integer.parseInt(parts[3]);
            int filesPerWord = Integer.parseInt(parts[4]);
            int filesNum = (file_num.get("file"));
//            DecimalFormat df = new DecimalFormat("0.000000");
//            float tf_idf = (float) (((float)(frequency/wordsPerFile)) * Math.log10(filesNum/filesPerWord));

            float tf_idf = (float) ((float)(frequency*1000000/wordsPerFile)* Math.log10(filesNum/filesPerWord))/1000000;
//            String formattedNum = df.format(tf_idf);
            double roundedNum = Precision.round(tf_idf, 6);
            context.write(new Text(parts[0] + " "+parts[1]), new Text(String.valueOf(roundedNum)));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Text value, Context context)
                throws IOException, InterruptedException {
//            String[] parts = value.toString().split("\\s+");
//            int frequency = Integer.parseInt(parts[0]);
//            int wordsPerFile = Integer.parseInt(parts[1]);
//            int filesPerWord = Integer.parseInt(parts[2]);
//            int filesNum = Integer.parseInt(parts[3]);
//            float tf_idf = (float)((frequency/wordsPerFile) * Math.log10(filesNum/filesPerWord));
            context.write(key, value);
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
                LOG.info("Added file to the distributed cache: " + args[i]);
            }
        }
        job.setJarByClass(task_1_4.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(CustomFileOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
