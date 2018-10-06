package tutorials.edureka.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author Jitendra Singh.
 */
public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value != null) {
            Arrays.stream(value.toString().split(" "))
                    .forEach(word -> writeWord(word, context));
        }
    }

    private void writeWord(String word, Context context) {
        try {
            context.write(new Text(word), new IntWritable(1));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
