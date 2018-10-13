package tutorials.edureka.mapreduce.maxtemprature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author Jitendra Singh.
 */
public class MaxTempMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] tuple = value.toString().split(" ");
        int year = Integer.parseInt(tuple[0]);
        int temp = Integer.parseInt(tuple[1]);
        context.write(
                new IntWritable(year),
                new IntWritable(temp)
        );

    }
}
