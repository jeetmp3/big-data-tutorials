package tutorials.edureka.mapreduce.hotandcold;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Jitendra Singh.
 */
public class HCMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        if(line != null && line.length() > 0) {
            String date = line.substring(6, 14).trim();
            String maxTemp = line.substring(41, 45).trim();
            String minTemp = line.substring(49, 54).trim();
            float max = Float.parseFloat(maxTemp);
            float min = Float.parseFloat(minTemp);
            String dayType = null;
            if(max > 40) {
                dayType = "Hot Day";
            } else if(min < 10) {
                dayType = "Cold Day";
            }
            if(dayType != null) {
                context.write(
                        new Text(date),
                        new Text(dayType)
                );
            }
        }

    }
}
