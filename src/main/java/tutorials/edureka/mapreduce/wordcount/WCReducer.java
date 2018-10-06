package tutorials.edureka.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.StreamSupport;


/**
 * @author Jitendra Singh.
 */
public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        if(key != null && values != null) {
            int sum = StreamSupport.stream(values.spliterator(), true)
                    .mapToInt(IntWritable::get).sum();
            context.write(key, new IntWritable(sum));
        }
    }
}
