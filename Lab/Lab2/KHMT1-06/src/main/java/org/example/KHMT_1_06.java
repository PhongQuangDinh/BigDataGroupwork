package org.example;

import java.awt.*;
import java.io.IOException;
import java.util.Scanner;
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


public class KHMT_1_06 {
    public static class DistacncePoint
            extends Mapper<Object, Text, IntWritable, Text>{
        public static int value_inp = 0;
        //        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
//                throws IOException,
//                InterruptedException{
//            Scanner input = new Scanner(System.in);
//            System.out.println("Enter the value: ");
//            value_inp = input.nextInt();
//        }
        private IntWritable dis = new IntWritable();
        private Text poi = new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split("\t");
            int coordinate = Integer.parseInt(words[1]);
            String point = words[0];
//            int queryPoint = Integer.parseInt(context.getConfiguration().get("queryPoint"));
            int queryPoint = 10;
            int distance= coordinate - queryPoint;
            distance = Math.abs(distance);
            dis.set(distance);
            context.write(dis, new Text(point));
        }
    }

    public static class DistacneAbs
            extends Reducer<IntWritable, Text, IntWritable, Text> {
        private IntWritable result = new IntWritable();
        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            String pointList = new String();
            Boolean first = true;
            for(Text point: values){
                if(first){
                    first = false;
                }
                else {
                    pointList += " ";
                }
                pointList += point.toString();
            }
            context.write(key, new Text(pointList));
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
        job.setJarByClass(KHMT_1_06.class);
        job.setMapperClass(DistacncePoint.class);
        job.setCombinerClass(DistacneAbs.class);
        job.setReducerClass(DistacneAbs.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
