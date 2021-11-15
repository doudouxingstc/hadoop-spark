package naive_kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

// Mapper Class
public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Point> {

    private Point[] centroids;
    private Point point;

    @Override
    public void setup(Context context)
            throws IOException, InterruptedException {

        super.setup(context);

        Configuration conf = context.getConfiguration();
        int k = conf.getInt("k", 5);
        centroids = new Point[k];

        // create a filesystem object and file reader
        Path centroidsPath = new Path(conf.get("centroidsPath"));
        FileSystem fs = FileSystem.get(conf);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(centroidsPath)));

        String line = reader.readLine();
        while (line != null) {
            String[] centroidString = line.split(",");
            int pos = Integer.parseInt(centroidString[0]) - 1;
            centroids[pos] = new Point(centroidString[1], centroidString[2], 1);
            line = reader.readLine();
        }
        reader.close();
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] pointString = value.toString().split(",");
        point = new Point(pointString[0], pointString[1], 1);

        // Calculate and compare the distance to other centroids
        int nearest = 1;
        double nearestDist = point.distance(centroids[0]);
        for (int i = 1; i < centroids.length; i++) {
            double dist = point.distance(centroids[i]);
            if (dist < nearestDist) {
                nearest = i + 1;
                nearestDist = dist;
            }
        }

        // Write Result
        context.write(new IntWritable(nearest), point);
    }
}

