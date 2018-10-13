package tutorials.edureka.mapreduce.maxtemprature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


/**
 * @author Jitendra Singh.
 */
public class MaxTempReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        if (key != null && values != null) {
            List<Integer> data = new ArrayList<>();
            values.iterator().forEachRemaining(iw -> data.add(iw.get()));
            int max = data
                    .stream()
                    .reduce(Integer::max)
                    .orElse(0);
            context.write(key, new IntWritable(max));
        }
    }
}
