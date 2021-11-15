package naive_kmeans;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;

// Driver Class
public class KMeansDriver {

    private static Point[] loadCentroids(FileSystem fs, Path path, int k) throws IOException {
        Point[] centroids = new Point[k];
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));

        String line = reader.readLine();
        while (line != null) {
            String[] centroidString = line.split(",");
            int pos = Integer.parseInt(centroidString[0]) - 1;
            centroids[pos] = new Point(centroidString[1], centroidString[2], 1);
            line = reader.readLine();
        }
        reader.close();

        return centroids;
    }

    public static void main(String[] args)
            throws Exception{

        if (args.length != 3) {
            System.err.println("Usage: KMeansDriver <input path> <output path> <number of clusters>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        Path dataPath = new Path(args[0]);
        Path centroidsPath = new Path(args[1] + "/centroids.txt");
        Path outputPath = new Path(args[1] + "/output_q2");
        int k = Integer.parseInt(args[2]);
        conf.setInt("k", k);
        conf.set("centroidsPath", centroidsPath.toString());
        conf.set("outputPath", outputPath.toString());

        // Pick five random points as initial centroids
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(centroidsPath)) {
            fs.delete(centroidsPath, true);
        }

        // Need some modifications
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(centroidsPath)));
        writer.write("1,50.197031637442876,32.94048164287042");
        writer.newLine();
        writer.write("2,43.407412339767056,6.541037020010927");
        writer.newLine();
        writer.write("3,1.7885358732482017,19.666057053079573");
        writer.newLine();
        writer.write("4,32.6358540480337,4.03843047564191");
        writer.newLine();
        writer.write("5,11.128704030740206,16.091153937413665");
        writer.newLine();
        writer.close();

        // MapReduce flow of control
        int iter = 1;
        boolean converged = false;
        while (!converged && iter <= 20) {
            Point[] prevCentroids, newCentroids;

            // Store old centroids
            prevCentroids = loadCentroids(fs, centroidsPath, k);
            fs.delete(outputPath, true);

            Job job = Job.getInstance(conf, "K-Means Clustering Iteration: " + iter);

            job.setJarByClass(KMeansDriver.class);
            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
            job.setReducerClass(KMeansReducer.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Point.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.setInputPaths(job, dataPath);
            FileOutputFormat.setOutputPath(job, outputPath);

            if (!job.waitForCompletion(true)) {
                System.err.println("Iteration: " + iter + " failed");
                System.exit(-1);
            }

            // Get new centroids
            newCentroids = loadCentroids(fs, centroidsPath, k);

            // Compare old and new centroids
            for (int i = 0; i < k; i++) {
                if (newCentroids[i].distance(prevCentroids[i]) < 0.0001) {
                    converged = true;
                }
            }
            iter++;
        }
    }
}
