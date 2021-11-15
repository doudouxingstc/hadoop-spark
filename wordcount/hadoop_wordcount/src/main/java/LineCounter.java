import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LineCounter {

    // Mapper class
    static class LineCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            if (line != null && !line.isEmpty()) {
                value.set("Non-empty Line Count: ");
            } else {
                value.set("Empty Line Count: ");
            }
            context.write(value, new IntWritable(1));

        }
    }

    // Reducer class
    static class LineCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable line : values) {
                sum += line.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    // Driver class
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: LineCounter <input path> <output path>");
            System.exit(-1);
        }

        Configuration config = new Configuration();
        Job job = Job.getInstance(config, "Line Counter");

        job.setJarByClass(LineCounter.class);
        job.setMapperClass(LineCountMapper.class);
        job.setReducerClass(LineCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "/output_q1"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
