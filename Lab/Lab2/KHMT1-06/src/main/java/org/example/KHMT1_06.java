
package org.example;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordWriter;

public abstract class KHMT1_06 extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(KHMT1_06.class);


    public static class CustomFileOutputFormat extends FileOutputFormat<Text, IntWritable> {

        @Override
        public RecordWriter<Text, IntWritable> getRecordWriter(TaskAttemptContext job)
                throws IOException, InterruptedException {
            Configuration conf = job.getConfiguration();
            // Customizing the file name by using the job name and task id
            //String customFileName = "task_1_1" + job.getTaskAttemptID().getTaskID().getId() + ".mtx";
            String customFileName = "task_1_1.mtx";
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

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private boolean caseSensitive = false;
        private String input;
        private Set<String> patternsToSkip = new HashSet<String>(); //stop words to be removed from the final result
        private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

        protected void setup(Mapper.Context context)
                throws IOException,
                InterruptedException {
            if (context.getInputSplit() instanceof FileSplit) {
                this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
            } else {
                this.input = context.getInputSplit().toString();
            }
            Configuration config = context.getConfiguration();
            this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);

            if (config.getBoolean("wordcount.skip.patterns", false)) {
                URI[] localPaths = context.getCacheFiles();
                parseSkipFile(localPaths[0]);
            }
        }
        private void parseSkipFile(URI patternsURI) {
            LOG.info("Added file to the distributed cache: " + patternsURI);
            try {
                BufferedReader fis = new BufferedReader(new FileReader(new File(patternsURI.getPath()).getName()));
                String pattern;
                while ((pattern = fis.readLine()) != null) {
                    patternsToSkip.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + patternsURI + "' : " + StringUtils.stringifyException(ioe));
            }
        }

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            String fileName = context.getInputSplit().toString().split("/")[context.getInputSplit().toString().split("/").length - 2] + '/' + context.getInputSplit().toString().split("/")[context.getInputSplit().toString().split("/").length - 1];

            String line = lineText.toString();
            if (!caseSensitive) {
                line = line.toLowerCase();
            }
            Text currentWord = new Text();
            for (String word : WORD_BOUNDARY.split(line)) {
                if (word.isEmpty()) {
                    continue;
                }
                if((word.charAt(0) < 48 || (word.charAt(0) > 57 && word.charAt(0) < 97) || word.charAt(0) > 122)  ) {
                    continue;
                }
                if (patternsToSkip.contains(word.toLowerCase())) {
                    continue;
                }
                int index = fileName.indexOf(":");
                word += " ";
                word += fileName.substring(0, index);
                currentWord = new Text(word);
                context.write(currentWord,one);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text word, Iterable<IntWritable> counts, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count : counts) {
                sum += count.get();
            }
            context.write(word, new IntWritable(sum));
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
                job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
                i += 1;
                job.addCacheFile(new Path(args[i]).toUri());
                LOG.info("Added file to the distributed cache: " + args[i]);
            }
        }
        job.setJarByClass(KHMT1_06.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(CustomFileOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

