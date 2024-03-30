package task_1_4;
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
import java.io.InputStreamReader;
public class test {
    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.err.println("Usage: SkipFileReader <inputPath> <outputPath> -skip <skipFilePath>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];
        String skipFilePath = null;

        // Parsing command-line arguments to find the skip file
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals("-skip")) {
                skipFilePath = args[i + 1];
                break;
            }
        }

        if (skipFilePath == null) {
            System.err.println("Skip file path not provided.");
            System.exit(1);
        }

        // Initialize Hadoop configuration
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(skipFilePath), conf);

        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(skipFilePath))))) {
            String line;
            while ((line = br.readLine()) != null) {
                // Process each line of the skip file as needed
                System.out.println(line); // Example: Just printing for demonstration
            }
        }

        // Continue with your Hadoop job using inputPath and outputPath
        // For demonstration purposes, simply print input and output paths
        System.out.println("Input Path: " + inputPath);
        System.out.println("Output Path: " + outputPath);
    }
}
