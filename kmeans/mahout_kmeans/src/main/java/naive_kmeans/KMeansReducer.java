package naive_kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

// Reducer Class
public class KMeansReducer extends Reducer<IntWritable, Point, Text, Text> {

    private Point[] newCentroids;

    @Override
    public void setup(Context context)
            throws IOException, InterruptedException {

        super.setup(context);
        int k = context.getConfiguration().getInt("k", 5);
        newCentroids = new Point[k];
    }

    @Override
    public void reduce(IntWritable centroid, Iterable<Point> points, Context context)
            throws IOException, InterruptedException {

        // Calculate the new centroid
        double xSumAll = 0, ySumAll = 0;
        int numSumAll = 0;
        for (Point each : points) {
            xSumAll += each.getX();
            ySumAll += each.getY();
            numSumAll += each.getNum();
        }

        // Update centroid for each class
        Point point = new Point(xSumAll / numSumAll, ySumAll / numSumAll, 1);
        newCentroids[centroid.get() - 1] = point;

        Text centroidClass = new Text();
        Text centroidValue = new Text();
        centroidClass.set(centroid.toString());
        centroidValue.set("(" + point.getX() + ", " + point.getY()+ ")");
        context.write(centroidClass, centroidValue);
    }

    @Override
    public void cleanup(Context context)
            throws IOException, InterruptedException {

        // If not all partition completed, skip this section
        for (int i = 0; i < newCentroids.length; i++) {
            if (newCentroids[i] == null) {
                return;
            }
        }

        // Delete the old centroids
        Configuration conf = context.getConfiguration();
        Path centroidsPath = new Path(conf.get("centroidsPath"));
        FileSystem fs = FileSystem.get(conf);

        if (!fs.exists(centroidsPath)) {
            System.err.println("Error: Previous centroid file does not exist");
            System.exit(-1);
        }

        fs.delete(centroidsPath, true);

        // Write new centroids
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(centroidsPath)));
        for (int i = 0; i < newCentroids.length; i++) {
            int pos = i + 1;
            writer.write(pos + "," + newCentroids[i].toString());
            writer.newLine();
        }
        writer.close();
    }
}