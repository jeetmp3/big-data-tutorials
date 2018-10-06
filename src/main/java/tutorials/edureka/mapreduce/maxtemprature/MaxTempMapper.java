package tutorials.edureka.mapreduce.maxtemprature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Jitendra Singh.
 */
public class MaxTempMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value != null) {
            String[] touples = value.toString().split(" ");
            if (touples.length > 1) {
                try {
                    context.write(
                            new IntWritable(Integer.parseInt(touples[0])),
                            new IntWritable(Integer.parseInt(touples[1]))
                    );
                } catch (Exception ex) {
                }
            }
        }
    }
}
