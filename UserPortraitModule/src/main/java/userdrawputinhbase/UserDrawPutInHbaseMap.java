package userdrawputinhbase;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Create by fengqijie
 * 2019/2/27 14:04
 */
public class UserDrawPutInHbaseMap extends Mapper<LongWritable, Text, Text, Text> {

    Text k2 = new Text();
    Text v2 = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] splited = line.split("\\|");

        k2.set(splited[1]);
        v2.set(line);

        context.write(k2, v2);
    }
}
