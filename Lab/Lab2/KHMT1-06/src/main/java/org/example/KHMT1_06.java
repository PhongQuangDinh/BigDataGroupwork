/*
package org.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Set;
import java.util.HashSet;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IOUtils;
import java.io.InputStreamReader;
import org.apache.log4j.Logger;

public class KHMT1_06 {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{


        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private static final Logger logger = Logger.getLogger(TokenizerMapper.class);
        private HashSet<String> stopWords;
        private String directoryName;

        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            directoryName = context.getConfiguration().get("directoryName");
            Configuration conf = context.getConfiguration();
            Path[] files = DistributedCache.getLocalCacheFiles(conf);
            Path stopWordsPath = files[0];

            // Populate stopwords set
            stopWords = new HashSet<String>();
            BufferedReader reader = new BufferedReader(new FileReader(stopWordsPath.toString()));
            String line;
*/
/*            while ((line = reader.readLine()) != null) {
                stopWords.add(line.trim());
            }*//*

            int i = 0;
            while ((line = reader.readLine()) != null) {
                System.out.println("Stopword[$i]: " + line);
                if (i++ > 10) {
                    break;
                }
            }
            reader.close();
*/
/*            Configuration conf = context.getConfiguration();
            Path[] stopWordsFiles = DistributedCache.getLocalCacheFiles(conf);
            if (stopWordsFiles != null && stopWordsFiles.length > 0) {
                for (Path stopWordsFile : stopWordsFiles) {
                    try (BufferedReader br = new BufferedReader(new FileReader(stopWordsFile.toString()))) {
                        String line;
                        while ((line = br.readLine()) != null) {
                            stopWords.add(line.trim());
                        }
                    }
                }
            }*//*


*/
/*            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);

            Path stopWordsPath = new Path("/stopwords/stopwords.txt");
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(stopWordsPath)));

            String line;
            while ((line = reader.readLine()) != null) {
                stopWords.add(line);
            }
            reader.close();*//*

        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String fileName = context.getInputSplit().toString().split("/")[context.getInputSplit().toString().split("/").length - 1];
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String tmp_word = itr.nextToken();
                if(tmp_word.charAt(0) == '\"'){
                    tmp_word = tmp_word.replace("\"", "");
                }
                if(tmp_word.charAt(tmp_word.length() - 1) == '.') {
                    tmp_word = tmp_word.replace(".", "");
                }
                if(tmp_word.charAt(tmp_word.length() - 1) == '"') {
                    tmp_word = tmp_word.replace("\"", "");
                }
                if(tmp_word.charAt(tmp_word.length() - 1) == '\'') {
                    tmp_word = tmp_word.replace("'", "");
                }
                if(tmp_word.charAt(tmp_word.length() - 1) == ',') {
                    tmp_word = tmp_word.replace(",", "");
                }
//                int index = fileName.indexOf(":");
//                tmp_word += "\t";
//                tmp_word += fileName.substring(0, index);
                word.set(tmp_word);
                context.write(word, one);
            }
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                String lowercaseToken = token.toLowerCase();
                if (!stopWords.contains(token)) {
                    word.set(token);
                    context.write(word, one);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        // avoid already exist output folder
        if(fs.exists(new Path(args[1]))){
            fs.delete(new Path(args[1]),true);
        }
        Job job = Job.getInstance(conf, "word count");
        Path stopWordsPath = new Path("/stopwords/stopwords.txt");
        job.addCacheFile(stopWordsPath.toUri());
        job.setJarByClass(KHMT1_06.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < args.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
*/



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
import org.apache.hadoop.util.StringUtils; //working with strings in Hadoop
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public abstract class KHMT1_06 extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(KHMT1_06.class);

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
        job.setOutputFormatClass(MTXOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    public static class MTXOutputFormat extends FileOutputFormat<Text, IntWritable> {

        @Override
        public RecordWriter<Text, IntWritable> getRecordWriter(TaskAttemptContext job)
                throws IOException, InterruptedException {
            Configuration conf = job.getConfiguration();
            Path outputFilePath = getDefaultWorkFile(job, ".mtx");
            FileSystem fs = outputFilePath.getFileSystem(conf);
            FSDataOutputStream fileOut = fs.create(outputFilePath, false);

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
        private Text word = new Text();
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
//parseSkipFile method
            if (config.getBoolean("wordcount.skip.patterns", false)) {
                URI[] localPaths = context.getCacheFiles();
                parseSkipFile(localPaths[0]);
            }
        }
        //Getting file from the HDFS and to read until EOL
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
}

