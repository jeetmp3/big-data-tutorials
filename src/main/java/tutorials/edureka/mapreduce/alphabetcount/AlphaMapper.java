package tutorials.edureka.mapreduce.alphabetcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author Jitendra Singh.
 */
public class AlphaMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value != null) {
            Arrays.stream(value.toString().split(" "))
                    .forEach(word -> writeWord(word, context));
        }
    }

    private void writeWord(String word, Context context) {
        try {
            context.write(new IntWritable(word.length()), new IntWritable(1));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
