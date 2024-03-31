
package task_1_5;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

public class numDocPerFolder {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);

            // Specify the base directory
            Path baseDir = new Path(args[0]);
            FileStatus[] subDirs = fs.listStatus(baseDir);

            // Create an output file in HDFS
            Path outputPath = new Path(args[1] + "/filePerFolder.txt");

            FSDataOutputStream outputStream = fs.create(outputPath);

            for (FileStatus subDir : subDirs) {
                if (subDir.isDirectory()) {
                    Path subDirPath = subDir.getPath();

                    FileStatus[] files = fs.listStatus(subDirPath);

                    int fileCount = files.length;

                    String outputLine = subDirPath.getName() + " " + fileCount + "\n";
                    outputStream.writeBytes(outputLine);
                    System.out.println("File counts written to " + outputPath.toString());
                }
            }

            outputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}