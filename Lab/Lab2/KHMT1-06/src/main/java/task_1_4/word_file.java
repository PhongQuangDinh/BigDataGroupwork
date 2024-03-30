package task_1_4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

public class word_file {

    public static final Logger logger = Logger.getLogger(task_1_4.class);
    public static class CustomFileOutputFormat extends FileOutputFormat<Text, Text> {

        @Override
        public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job)
                throws IOException, InterruptedException {
            Configuration conf = job.getConfiguration();
            String customFileName = "word_file.mtx";
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
        static final Logger logMap = Logger.getLogger(TokenizerMapper.class);
//        public static MultipleOutputs<Text, Text> writer;
//        @Override
//        protected void setup(Context context){
//            writer = new MultipleOutputs<>(context);
//        }
//
//        @Override
//        protected void cleanup(Context context) throws IOException, InterruptedException {
//            writer.close();
//        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+");
            context.write(new Text(parts[0]), new Text(parts[0] + " " +parts[1] + " " + parts[2] + " " + parts[3]));
            logMap.info((parts[0] + " " +parts[1] + " " + parts[2] + " " + parts[3]));
//            writer.write("HKT", new Text(parts[0]), new Text(parts[0] + " " + parts[1] + " " + parts[2] + " " + parts[3]));
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {
        static Hashtable<String, Integer> word_file = new Hashtable<String, Integer>();
        static List<Text> listValue = new ArrayList<>();
        static Set<String> all_doc = new HashSet<>();
        static int numDoc;
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (Text val : values) {
                String[] parts = val.toString().split("\\s+");
                all_doc.add(parts[1]);
                listValue.add(val);
                sum++;
//                if (word_file.containsKey(parts[0])){
//                    word_file.put(parts[0],  word_file.get(parts[0]) + 1);
//                }
//                else{
//                    word_file.put(parts[0], 1);
////                    word_file.computeIfPresent((k, v) -> v += 1);
////                    word_file.put(parts[0],  word_file.get(parts[0]) + 1);
//                }
            }
            word_file.put(String.valueOf(key), sum);

        }
        // writable
        public void cleanup(Context context) throws IOException, InterruptedException {
            numDoc = all_doc.size();

            // Duyệt qua tất cả các cặp key-value
            for (Text val : listValue) {
                String[] parts = val.toString().split("\\s+");
                int numWord = word_file.get(parts[0]);
                context.write(val, new Text(numWord + " " + numDoc));
//                bw.write(val + " " + numWord + " "+ numDoc);
            }

            // Đóng `BufferedWriter`
//            bw.close();
        }
    }
//    public static class IntSumReducer
//            extends Reducer<Text, Text, Text, Text> {
//        static Hashtable<String, Integer> word_file = new Hashtable<String, Integer>();
//        private Text word = new Text();
//        private Text docName = new Text();
//        private float tf_idf;
//
//        static int num_all_doc;
//        protected void setup(Context context){
//            num_all_doc = Integer.parseInt(context.getConfiguration().get("all_doc"));
//            for (String pair : context.getConfiguration().get("word_file").split(", ")) {
//
//                String[] keyValue = pair.split(":");
//                word_file.put(keyValue[0], Integer.parseInt(keyValue[1]));
//            }
////            num_all_doc = Combiner.numDoc;
////            word_file.putAll(Combiner.word_file);
//        }
//
//        public void reduce(Text key, Iterable<Text> values,
//                           Context context
//        ) throws IOException, InterruptedException {
//            for (Text val : values) {
//                String[] parts = val.toString().split("\\s+");
//                tf_idf = (float) (Float.parseFloat(parts[2])/Float.parseFloat(parts[3]));
//
////                tf_idf = (float) (Float.parseFloat(parts[2])/Float.parseFloat(parts[3]) * Math.log10(num_all_doc/(word_file.get(key.toString()))));
//                word.set(parts[0]);
//                docName.set(parts[1]);
////                tf_idf = tf_idf.setScale(6, RoundingMode.HALF_UP);
//
//            }
//            context.write(new Text(word + " "+ docName), new Text(String.valueOf(tf_idf)));
//        }
//    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
//        conf.set('');
        Job job = Job.getInstance(conf, "MTX Filter Job");
        job.setJarByClass(word_file.class);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(new Path(args[1]))){
            fs.delete(new Path(args[1]),true);
        }
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(CustomFileOutputFormat.class);

//        MultipleOutputs.addNamedOutput(job, "HKT", TextOutputFormat.class, Text.class, Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
