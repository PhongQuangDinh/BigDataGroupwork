package task_2_2;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser; // for InputFilePath

import org.apache.log4j.Logger; // for debug
public class KMean2_2 {
    private static final Logger logger = Logger.getLogger(task_2_2.KMean2_2.KMeansMapper.class); // for debug

    private static boolean isConvergence(String[] oldCentroids, String[] newCentroids) {
        for(int i = 0; i < oldCentroids.length; i++) {
            boolean check = oldCentroids[i].equals(newCentroids[i]);
            logger.info(oldCentroids[i] + " COMPARE " + newCentroids[i] + " = " + (check));
            if(!oldCentroids[i].equals(newCentroids[i])) {
                return false;
            }
        }
        return true;
    }
    private static List<double[]> point_list = new ArrayList<>();
    private static String[] readPointsFromCSV(Configuration conf,String filePath) throws IOException {
        List<String> points = new ArrayList<>();
        Path path = new Path(filePath);
        FileSystem hdfs = FileSystem.get(conf);
        FSDataInputStream in = hdfs.open(path);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line;
        while ((line = reader.readLine()) != null) {
            if (!line.contains("class")) {
                String[] line_str = line.split(",");
                points.add(line_str[1] + "," + line_str[2]);
            }
        }
        String[] res = new String[points.size()];
        int cnt = 0;
        for (String point : points)
        {
            conf.set("point" + cnt, point);
            res[cnt] = point;
            cnt++;
        }
        conf.set("pointSize",cnt + "");
        return res;
    }
    private static String[] readCentroidsFromOutput(Configuration conf,String filePath, int K) throws IOException {
        String[] points = new String[K];
        Path path = new Path(filePath);
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus[] status = hdfs.listStatus(new Path(filePath));
        for (int i = 0; i < status.length; i++) {
            //Read the centroids from the hdfs
            if(status[i].getPath().toString().endsWith("part-r-00000")) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(status[i].getPath())));
                String line;
                int cnt = 0;
                while ((line = reader.readLine()) != null) {
                    String[] centroid_str = line.split("\t");
                    points[cnt] = centroid_str[1];
                    cnt++;
                }
                reader.close();
                break;
            }
        }
        // delete the temporary directory for new run
        hdfs.delete(new Path(filePath), true);
        return points;
    }
    private static String[] selectRandomPoints(String[] points, int numRandomPoints) {
        String[] randomPoints = new String[numRandomPoints];
        Random random = new Random();
        for (int i = 0; i < numRandomPoints; i++) {
            int randomIndex = random.nextInt(points.length);
            randomPoints[i] = points[randomIndex];
        }
        return randomPoints;
    }
    private static String[] initK_Centroids(Configuration conf, String InputPath, int K) throws IOException {
        String[] points = readPointsFromCSV(conf,InputPath);
        String[] randomPoints = selectRandomPoints(points, K);
        String[] newPoints = new String[K];
        int cnt = 0;
        for (int i = 0; i < K; i++) {
            newPoints[i] = randomPoints[i];
            String[] row = randomPoints[i].split(",");
            double[] centroid = {Double.parseDouble(row[0]), Double.parseDouble(row[1])};
            // save this one to cache files so mapper and reducer can read from it
            conf.set("centroid" + cnt, centroid[0] + "," + centroid[1]);
            cnt++;
        }
        conf.set("k", K + ""); // this one to cache as well
        return newPoints;
    }

    //------------------------------------MAPPER----------------------------------------
    public static class KMeansMapper extends Mapper<Object, Text, IntWritable, Text> {
        private static MultipleOutputs<IntWritable, Text> multipleOutputs;
        // private Text centroid_txt = new Text();
        private static List<double[]> centroids = new ArrayList<>();
        private static int K = 0;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            K = Integer.parseInt(context.getConfiguration().get("k"));
            for (int i = 0; i < K; i++)
            {
                String[] centroid_str = context.getConfiguration().get("centroid" + i).split(",");
                double[] point = {Double.parseDouble(centroid_str[0]) , Double.parseDouble(centroid_str[1])};
                centroids.add(point);
            }
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // Skip the first line (column labels)
            if (value.toString().contains("class")) {
                return;
            }
            String[] point = value.toString().split(",");
            int label_class = Integer.parseInt(point[0]);
            double x1 = Double.parseDouble(point[1]);
            double x2 = Double.parseDouble(point[2]);

            // Calculate the distance to centroids and find the nearest one
            double minDistance = Double.MAX_VALUE;
            int minIndex = -1;
            for (int i = 0; i < centroids.size(); i++) {
                double[] centroid = centroids.get(i);
                // Euclidean distance
                double distance = Math.sqrt(Math.pow(x1 - centroid[0], 2) + Math.pow(x2 - centroid[1], 2));
                if (distance < minDistance) {
                    minDistance = distance;
                    minIndex = i;
                }
            }

            // Emit the nearest centroid and the point
            context.write(new IntWritable(minIndex), new Text(x1 + "," + x2));
            // key is the closest centroid, value is the point that close to it
        }
    }

    //------------------------------------REDUCER----------------------------------------
    public static class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // Calculate the new centroid
            double sumX1 = 0;
            double sumX2 = 0;
            int count = 0;
            for (Text point : values) {
                String[] point_str = point.toString().split(",");
                sumX1 += Double.parseDouble(point_str[0]);
                sumX2 += Double.parseDouble(point_str[1]);
                count++;
            }
            Text newCentroid = new Text((sumX1 / count) + "," + (sumX2 / count));
            context.write(key, newCentroid);
        }
    }
    private static void writeOutput(Configuration conf, String[] centroids, String outputPath) throws Exception
    {
        // task_2_1.clusters
        FileSystem hdfs = FileSystem.get(conf);
        FSDataOutputStream dos = hdfs.create(new Path(outputPath + "/task_2_1.clusters"), true);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(dos));

        for(int i = 0; i < centroids.length; i++) {
            writer.write((i + 1) + " | (" + centroids[i] + ")");
            writer.newLine();
        }
        writer.close();

        // task_2_1.classes, too long and slow
        FSDataOutputStream dosClass = hdfs.create(new Path(outputPath + "/task_2_1.classes"), true);
        BufferedWriter writerClass = new BufferedWriter(new OutputStreamWriter(dosClass));
        int point_size = Integer.parseInt(conf.get("pointSize"));
        for(int i = 0; i < point_size; i++) {
            String[] point = conf.get("point" + i).split(",");
            double x = Double.parseDouble(point[0]);
            double y = Double.parseDouble(point[1]);

            double minDistance = Double.MAX_VALUE;
            int minIndex = -1;
            for (int j = 0; j < centroids.length; j++) {
                String[] centroid = centroids[j].split(",");
                double centroid_x = Double.parseDouble(centroid[0]);
                double centroid_y = Double.parseDouble(centroid[1]);

                // Euclidean distance
                double distance = Math.sqrt(Math.pow(x - centroid_x, 2) + Math.pow(y - centroid_y, 2));
                if (distance < minDistance) {
                    minDistance = distance;
                    minIndex = j;
                }
            }
            writerClass.write((minIndex + 1) + " | (" + point[0] + "," + point[1] + ")");
            writerClass.newLine();
        }
        writerClass.close();

        hdfs.close();
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        // avoid already exist output folder
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        // get input file
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        int K = 3;
        int Max_iteration = 20; // create 20 jobs for 20 runs
        int iter_cnt = 0; // iteration counter
        boolean isDone = false;
        conf.set("isDone", isDone + "");

        String[] oldCentroids = new String[K];
        String[] newCentroids = initK_Centroids(conf, otherArgs[0],K);

        // start a for loop here of 20 iterations mean 20 jobs over and over again until convergence
        while (!isDone) {
            iter_cnt++;
            for (int i = 0; i < K; i++)
                logger.info("Centroid " + i + " : " + newCentroids[i]);
            logger.info("ITERATION NUMBER : " + iter_cnt);

            Job job = Job.getInstance(conf, "KMeans" + iter_cnt);
            job.setJarByClass(task_2_2.KMean2_2.class);

            job.setMapperClass(task_2_2.KMean2_2.KMeansMapper.class);
            job.setCombinerClass(task_2_2.KMean2_2.KMeansReducer.class);
            job.setReducerClass(task_2_2.KMean2_2.KMeansReducer.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1] + "/temp"));

            if (!job.waitForCompletion(true))
            {
                isDone = true;
                System.exit(1);
            }

            for(int i = 0; i < K; i++) {
                oldCentroids[i] = newCentroids[i];
            }
            newCentroids = readCentroidsFromOutput(conf, otherArgs[1] + "/temp", K);

            isDone = isConvergence(oldCentroids, newCentroids);
            conf.set("isDone", isDone + "");
            logger.info("ITERATION " + iter_cnt + " is convergence = " + isDone);

            if(isDone || iter_cnt == (Max_iteration -1)) {
                writeOutput(conf, newCentroids, otherArgs[1]);
                logger.info("Successfully written file to " + otherArgs[1] + "/task_2_1.clusters and " + otherArgs[1] + "/task_2_1.classes");
            } else {
                // write new centroids to cache
                for(int i = 0; i < K; i++) {
                    conf.unset("centroid" + i);
                    conf.set("centroid" + i, newCentroids[i]);
                }
            }
        }
        System.exit(0);
    }
}