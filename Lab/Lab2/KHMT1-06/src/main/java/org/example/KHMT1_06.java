package org.example;

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


public class KHMT1_06 {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{


        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

//        private String directoryName;
//
//        protected void setup(Context context) throws IOException, InterruptedException {
//            super.setup(context);
//            directoryName = context.getConfiguration().get("directoryName");
//        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
//            String fileName = context.getInputSplit().toString().split("/")[context.getInputSplit().toString().split("/").length - 1];
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
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
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
        job.setJarByClass(KHMT1_06.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        for (int i = 0; i < args.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
