package naive_kmeans;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Point implements Writable {

    private double x;
    private double y;
    private int num;

    public Point() {
    }

    public Point(Double xVal, Double yVal, int num) {
        this.x = xVal;
        this.y = yVal;
        this.num = num;
    }

    public Point(String xVal, String yVal, int num) {
        this(Double.parseDouble(xVal), Double.parseDouble(yVal), num);
    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

    public int getNum() {
        return num;
    }

    public double distance(Point another) {
        return Math.pow(this.x - another.x, 2) + Math.pow(this.y - another.y, 2);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(x);
        dataOutput.writeDouble(y);
        dataOutput.writeInt(num);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.x = dataInput.readDouble();
        this.y = dataInput.readDouble();
        this.num = dataInput.readInt();
    }

    @Override
    public String toString() {
        return this.x + "," + this.y;
    }
}
