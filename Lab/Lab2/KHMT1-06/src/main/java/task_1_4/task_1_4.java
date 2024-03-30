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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import java.util.Hashtable;
import java.util.List;

import org.apache.log4j.Logger;

public class task_1_4 {
    public static final Logger logger = Logger.getLogger(task_1_4.class);
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
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {
        public static MultipleOutputs<Text, Text> writer;
        @Override
        protected void setup(Context context){
            writer = new MultipleOutputs<>(context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            writer.close();
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+");
            context.write(new Text(parts[0]), new Text(parts[0] + " " + parts[1] + " " + parts[2] + " " + parts[3]));
            writer.write("HKT", new Text(parts[0]), new Text(parts[0] + " " + parts[1] + " " + parts[2] + " " + parts[3]));
        }
    }

    public static class Combiner
            extends Reducer<Text, Text, Text, Text> {
        static Hashtable<Text, Integer> word_file = new Hashtable<>();
//        static String word = new String();
        static List<String> all_doc;
        static int numDoc;

        @Override
        protected void cleanup(Context context) {
            numDoc = all_doc.size();
            context.getConfiguration().set("all_doc", numDoc + "");
            StringBuilder sb = new StringBuilder();
            word_file.forEach((key, value) -> {
                sb.append(key).append(":").append(value).append(", ");
            });
            String result = sb.toString();
            context.getConfiguration().set("word_file", result);
        }
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            if ((word_file.containsKey(key))==false){
                word_file.put(key, 1);
            }
            else{
                int value_key = word_file.get(key);
                word_file.put(key, value_key+1);
            }
            for (Text val : values) {
                String[] parts = val.toString().split("\\s+");
                if((all_doc.contains(parts[1]))==false){
                    all_doc.add(parts[1]);
                }
            }
        }
    }
    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {
        static Hashtable<String, Integer> word_file = new Hashtable<String, Integer>();
        private Text word = new Text();
        private Text docName = new Text();
        private float tf_idf;

        static int num_all_doc;
        protected void setup(Context context){
            num_all_doc = Integer.parseInt(context.getConfiguration().get("all_doc"));
            for (String pair : context.getConfiguration().get("word_file").split(", ")) {

                String[] keyValue = pair.split(":");
                word_file.put(keyValue[0], Integer.parseInt(keyValue[1]));
            }
//            num_all_doc = Combiner.numDoc;
//            word_file.putAll(Combiner.word_file);
        }

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (Text val : values) {
                String[] parts = val.toString().split("\\s+");
                tf_idf = (float) (Float.parseFloat(parts[2])/Float.parseFloat(parts[3]));

//                tf_idf = (float) (Float.parseFloat(parts[2])/Float.parseFloat(parts[3]) * Math.log10(num_all_doc/(word_file.get(key.toString()))));
                word.set(parts[0]);
                docName.set(parts[1]);
//                tf_idf = tf_idf.setScale(6, RoundingMode.HALF_UP);

            }
            context.write(new Text(word + " "+ docName), new Text(String.valueOf(tf_idf)));
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
//        conf.set('');
        Job job = Job.getInstance(conf, "MTX Filter Job");
        job.setJarByClass(task_1_4.class);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(new Path(args[1]))){
            fs.delete(new Path(args[1]),true);
        }
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(Combiner.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(CustomFileOutputFormat.class);

        MultipleOutputs.addNamedOutput(job, "HKT", TextOutputFormat.class, Text.class, Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
