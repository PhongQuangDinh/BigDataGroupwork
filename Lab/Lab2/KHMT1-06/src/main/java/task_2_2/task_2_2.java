package task_2_2;

import task_2_2.*;
import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser; // for InputFilePath

import org.apache.log4j.Logger; // for debug
public class task_2_2 {
    public static class CustomFileOutputFormat extends FileOutputFormat<IntWritable, Text> {

        private double calculateCosineSimilarity(String[] point, double[] centroid) {
            double dotProduct = 0.0;
            double point_mag = 0.0;
            double centroid_mag = 0.0;
            for (int i = 0; i < point.length; i++) {
                dotProduct += Double.parseDouble(point[i]) * centroid[i];
                point_mag += Math.pow(Double.parseDouble(point[i]), 2);
                centroid_mag += Math.pow(centroid[i], 2);
            }
            return dotProduct / (Math.sqrt(point_mag) * Math.sqrt(centroid_mag));
        }

        @Override
        public RecordWriter<IntWritable, Text> getRecordWriter(TaskAttemptContext job)
                throws IOException, InterruptedException {
            Configuration conf = job.getConfiguration();
            String customFileName = "task_2_2.clusters";
            Path outputDir = FileOutputFormat.getOutputPath(job);
            Path fullOutputPath = new Path(outputDir, customFileName);

            FileSystem fs = fullOutputPath.getFileSystem(conf);
            FSDataOutputStream fileOut = fs.create(fullOutputPath, false);

            return new RecordWriter<IntWritable, Text>() {
                @Override
                public void write(IntWritable key, Text value) throws IOException, InterruptedException {
                    String add = "Cluster_" + (key.get() + 1) + "|";
                    String line = add + "\t" + value.toString() + "\n"; // Write key-value pair as a line
                    fileOut.writeBytes(line);
                }
                @Override
                public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                    fileOut.close();
                }
            };
        }
    }
    private static final Logger logger = Logger.getLogger(KMeansMapper.class); // for debug

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
                points.add(line);
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
            if(status[i].getPath().toString().endsWith("task_2_2.clusters")) { // part-r-00000
                BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(status[i].getPath())));
                String line;
                int cnt = 0;
                while ((line = reader.readLine()) != null) {
                    String[] centroid_str = line.split("\t");
                    points[cnt] = centroid_str[1];
                    // logger.info(line); // for debug
                    cnt++;
                }
                reader.close();
                break;
            }
        }
        return points;
    }
    private static String[] initK_Centroids(Configuration conf, String InputPath, int K) throws IOException {
        String[] points = readPointsFromCSV(conf,InputPath);
        String[] randomPoints = new String[K];
        Random random = new Random();
        int cnt = 0;
        for (int i = 0; i < K; i++) {
            int randomIndex = random.nextInt(points.length);
            randomPoints[i] = points[randomIndex];
            conf.set("centroid" + cnt, points[randomIndex]);
            cnt++;
        }
        conf.set("k", K + ""); // this one to cache as well
        return randomPoints;
    }

    //------------------------------------MAPPER----------------------------------------
    public static class KMeansMapper extends Mapper<Object, Text, IntWritable, Text> {
        private static List<String[]> centroids = new ArrayList<>();
        private static int K = 0;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            K = Integer.parseInt(context.getConfiguration().get("k"));
            for (int i = 0; i < K; i++)
            {
                if (!context.getConfiguration().get("centroid" + i).contains("nothing")) {
                    String[] centroid_str = context.getConfiguration().get("centroid" + i).split("\\s");
                    centroids.add(centroid_str);
                }
            }
        }
        double EuclidDistance(String[] point, String[] centroid)
        {
            //-----split
            String termid = point[0];
            String docid = point[1];
            double tfidf = Double.parseDouble(point[2]);

            //----subtraction
            double sub1 = (termid.equals(centroid[0])) ? 1 : 0; // Jaccard Similarity

            String docid0 = docid.split("/")[0];
            String c_docid0 = centroid[1].split("/")[0];
            double sub2 = (docid0.equals(c_docid0)) ? 1 : 0;

            double distance = Math.sqrt(Math.pow(sub1, 2) + Math.pow(sub2, 2) + Math.pow(tfidf - Double.parseDouble(centroid[2]),2));

            return distance;
        }

        double CosineSimilarity(String[] point, String[] centroid)
        {
            //-----split
            String termid = point[0];
            String docid = point[1];
            double tfidf = Double.parseDouble(point[2]);

            //----subtraction
            double sub1 = (termid.equals(centroid[0])) ? 1 : 0; // Jaccard Similarity

            String docid0 = docid.split("/")[0];
            String c_docid0 = centroid[1].split("/")[0];
            double sub2 = (docid0.equals(c_docid0)) ? 1 : 0;

            double distance = Math.sqrt(Math.pow(sub1, 2) + Math.pow(sub2, 2) + Math.pow(tfidf - Double.parseDouble(centroid[2]),2));

            return distance;
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] point = value.toString().split("\\s");
            // Calculate the distance to centroids and find the nearest one
            double minDistance = Double.MAX_VALUE;
            int minIndex = -1;
            for (int i = 0; i < centroids.size(); i++) {
                String[] centroid = centroids.get(i);

                // Euclidean distance
                double distance = EuclidDistance(point, centroid);
                if (distance < minDistance) {
                    minDistance = distance;
                    minIndex = i;
                }
            }
            // Emit the nearest centroid and the point
            context.write(new IntWritable(minIndex), value);
            // key is the closest centroid, value is the point that close to it
        }
    }
    //------------------------------------COMBINER----------------------------------------
    public static class KMeansCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {
        private static MultipleOutputs<IntWritable, Text> writer;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            writer = new MultipleOutputs<>(context);
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            writer.close();
        }
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // Calculate the new centroid
            String new_termID = "";
            String new_docID = "";
            String new_folderName = "";

            double sumTFIDF = 0;

            int count = 0;

            int maxCount_term = 0;
            int maxCount_folder = 0;

            Map<String, Integer> file_cnt = new HashMap<>();
            Map<String, Integer> folder_cnt = new HashMap<>();
            Map<String, Integer> wordCounts = new HashMap<>();
            for (Text point : values) {
                String[] point_str = point.toString().split("\\s");

                // for avg_TFIDF
                sumTFIDF += Double.parseDouble(point_str[2]);
                count++;

                // for avg_termID
                int term_cnt = wordCounts.getOrDefault(point_str[0], 0) + 1;
                wordCounts.put(point_str[0], term_cnt);
                if (term_cnt > maxCount_term) {
                    maxCount_term = term_cnt;
                    new_termID = point_str[0];
                }

                // for avg_folder
                String[] directory = point_str[1].split("/");
                String folder = directory[0];
                int add_id = Integer.parseInt(directory[1].split("\\.")[0]);

                int currentCount = file_cnt.getOrDefault(folder, 0);
                file_cnt.put(folder, currentCount + add_id);

                int count_folder = folder_cnt.getOrDefault(folder, 0) + 1;
                folder_cnt.put(folder, count_folder);
                if (count_folder > maxCount_folder) {
                    maxCount_folder = count_folder;
                    new_folderName = folder;
                }

                int key_val = Integer.parseInt(key.toString()) + 1;
                writer.write("task22classes",new Text("Cluster_" + key_val + "|"), point);
            }
            new_docID = new_folderName + "/" + (int)(file_cnt.getOrDefault(new_folderName,0) / maxCount_folder) + ".txt";
            Text newCentroid = new Text(new_termID + " " + new_docID + " " + (sumTFIDF / count));
            context.write(key, newCentroid);
        }
    }
    //------------------------------------REDUCER----------------------------------------
    public static class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // Calculate the new centroid
            String new_termID = "";
            String new_docID = "";
            String new_folderName = "";

            double sumTFIDF = 0;

            int count = 0;

            int maxCount_term = 0;
            int maxCount_folder = 0;

            Map<String, Integer> file_cnt = new HashMap<>();
            Map<String, Integer> folder_cnt = new HashMap<>();
            Map<String, Integer> wordCounts = new HashMap<>();
            for (Text point : values) {
                String[] point_str = point.toString().split("\\s");

                // for avg_TFIDF
                sumTFIDF += Double.parseDouble(point_str[2]);
                count++;

                // for avg_termID
                int term_cnt = wordCounts.getOrDefault(point_str[0], 0) + 1;
                wordCounts.put(point_str[0], term_cnt);
                if (term_cnt > maxCount_term) {
                    maxCount_term = term_cnt;
                    new_termID = point_str[0];
                }

                // for avg_folder
                String[] directory = point_str[1].split("/");
                String folder = directory[0];
                int add_id = Integer.parseInt(directory[1].split("\\.")[0]);

                int currentCount = file_cnt.getOrDefault(folder, 0);
                file_cnt.put(folder, currentCount + add_id);

                int count_folder = folder_cnt.getOrDefault(folder, 0) + 1;
                folder_cnt.put(folder, count_folder);
                if (count_folder > maxCount_folder) {
                    maxCount_folder = count_folder;
                    new_folderName = folder;
                }
            }
            new_docID = new_folderName + "/" + (int)(file_cnt.getOrDefault(new_folderName,0) / maxCount_folder) + ".txt";
            Text newCentroid = new Text(new_termID + " " + new_docID + " " + (sumTFIDF / count));
            context.write(key, newCentroid);
        }
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
        int K = 5;
        int Max_iteration = 10; // create 10 jobs for 10 runs
        int iter_cnt = 0; // iteration counter
        boolean isDone = false;
        conf.set("isDone", isDone + "");

        String[] oldCentroids = new String[K];
        String[] newCentroids = initK_Centroids(conf, otherArgs[0],K);

        // start a for loop here of 20 iterations mean 20 jobs over and over again until convergence
        while (!isDone) {
            iter_cnt++;
            for (int i = 0; i < K; i++)
                logger.info("Centroid " + (i+1) + " : " + newCentroids[i]);
            logger.info("ITERATION NUMBER : " + iter_cnt);

            Job job = Job.getInstance(conf, "KMeans" + iter_cnt);
            job.setJarByClass(task_2_2.class);

            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
            job.setReducerClass(KMeansReducer.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            job.setOutputFormatClass(CustomFileOutputFormat.class); // my custom output

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            MultipleOutputs.addNamedOutput(job, "task22classes", TextOutputFormat.class,IntWritable.class,Text.class);

            if (!job.waitForCompletion(true))
            {
                isDone = true;
                System.exit(1);
            }

            for(int i = 0; i < K; i++) {
                oldCentroids[i] = newCentroids[i];
            }
            newCentroids = readCentroidsFromOutput(conf, otherArgs[1], K);
            isDone = isConvergence(oldCentroids, newCentroids);
            conf.set("isDone", isDone + "");

            logger.info("ITERATION " + iter_cnt + " is convergence = " + isDone);

            if(isDone || iter_cnt == (Max_iteration)) {
                FileSystem hdfs = FileSystem.get(conf);
                Path reducerOutputFile = new Path(otherArgs[1], "task22classes-m-00000");
                Path newOutputFile = new Path(otherArgs[1], "task_2_2.classes");
                hdfs.rename(reducerOutputFile, newOutputFile);
                hdfs.close();

                logger.info("Successfully written file to " + otherArgs[1] + "/task_2_2.clusters and " + otherArgs[1] + "/task_2_2.classes");
                break;
            } else {
                // write new centroids to cache
                for(int i = 0; i < K; i++) {
                    conf.unset("centroid" + i);
                    if (newCentroids[i] == null)
                        conf.set("centroid" + i, "nothing");
                    else conf.set("centroid" + i, newCentroids[i]);

                    // if not convergence then delete the old save to write a new one
                    FileSystem hdfs = FileSystem.get(conf);
                    hdfs.delete(new Path(otherArgs[1]), true);
                    hdfs.close();
                }
            }
        }
        System.exit(0);
    }
}