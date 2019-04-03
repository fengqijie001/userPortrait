package userdrawputinhbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Create by fengqijie
 * 2019/2/27 14:04
 */
public class UserDrawPutInHbaseReduce extends TableReducer<Text, Text, NullWritable>{

    @Override
    protected void reduce(Text k2, Iterable<Text> val,
                          Reducer<Text, Text, NullWritable, Mutation>.Context context)
            throws IOException, InterruptedException {
        for (Text v2 : val) {
            String[] splited = v2.toString().split("\\|");
            //rowkey
            if (k2.toString().length() != 0) {
                Put put = new Put(Bytes.toBytes(k2.toString()));

                //跳过写入Hlog，提高写入速度
                put.setDurability(Durability.SKIP_WAL);
                put.add(Bytes.toBytes("draw"), Bytes.toBytes("mdn"), Bytes.toBytes(splited[1]));
                put.add(Bytes.toBytes("draw"), Bytes.toBytes("male"), Bytes.toBytes(splited[2]));
                put.add(Bytes.toBytes("draw"), Bytes.toBytes("female"), Bytes.toBytes(splited[3]));
                put.add(Bytes.toBytes("draw"), Bytes.toBytes("age1"), Bytes.toBytes(splited[4]));
                put.add(Bytes.toBytes("draw"), Bytes.toBytes("age2"), Bytes.toBytes(splited[5]));
                put.add(Bytes.toBytes("draw"), Bytes.toBytes("age3"), Bytes.toBytes(splited[6]));
                put.add(Bytes.toBytes("draw"), Bytes.toBytes("age4"), Bytes.toBytes(splited[7]));
                put.add(Bytes.toBytes("draw"), Bytes.toBytes("age5"), Bytes.toBytes(splited[8]));
                context.write(NullWritable.get(), put);
            }

        }

    }
}
