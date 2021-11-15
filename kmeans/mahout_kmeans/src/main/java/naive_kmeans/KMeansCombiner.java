package naive_kmeans;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

// Combiner Class
public class KMeansCombiner extends Reducer<IntWritable, Point, IntWritable, Point> {

    @Override
    public void reduce(IntWritable centroid, Iterable<Point> points, Context context)
            throws IOException, InterruptedException {

        // Sum over each class in current batch
        double xSum = 0, ySum = 0;
        int numSum = 0;
        for (Point each : points) {
            xSum += each.getX();
            ySum += each.getY();
            numSum += 1;
        }
        Point pointSum = new Point(xSum, ySum, numSum);
        context.write(centroid, pointSum);
    }
}
