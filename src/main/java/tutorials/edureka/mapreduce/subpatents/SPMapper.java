package tutorials.edureka.mapreduce.subpatents;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author Jitendra Singh.
 */
public class SPMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value != null) {
            String[] touples = value.toString().split(" ");
            if(touples.length > 1) {
                context.write(new Text(touples[0]), new IntWritable(1));
            }
        }
    }
}
