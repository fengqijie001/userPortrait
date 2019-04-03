package userdrawputinhbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import util.Config;

import java.io.IOException;

/**
 * Create by fengqijie
 * 2019/2/27 18:09
 *
 * 手动创建hbase表
 * create 'user_draw', 'draw'
 */
public class UserDrawPutInHBase2 {

    static final String OUT_PATH2 = "file:///E:/3-学习视频/0-Hadoop/大数据视频/16-用户画像/用户画像/data/out2";
    static Config config = new Config();

    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", "D:\\Program Files\\hadoop-2.6.0");//这行我是本地运行所需指定的hadoop home
        Configuration conf = HBaseConfiguration.create();

        // 设置zookeeper
        conf.set(config.consite, config.hbaseip);
        // 将该值该大，防止hbase超时退出
        conf.set(config.coftime, config.time);
        // 设置hbase表名称
        conf.set(TableOutputFormat.OUTPUT_TABLE, config.tableDraw);

        Job job = Job.getInstance(conf, "UserDrawPutInHBase2");
        job.setJarByClass(UserDrawPutInHBase2.class);

        job.setMapperClass(UserDrawPutInHbaseMap2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(UserDrawPutInHbaseReduce2.class);

        FileInputFormat.setInputPaths(job, new Path(OUT_PATH2));
        job.setOutputFormatClass(TableOutputFormat.class);

        TableMapReduceUtil.initTableReducerJob(config.tableDraw, UserDrawPutInHbaseReduce2.class, job,
                null, null, null, null, false);

        // 提交到jobTracker
        System.out.println("true：表示完成；false：表示没有完成---------" + job.waitForCompletion(true));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class UserDrawPutInHbaseMap2 extends Mapper<LongWritable, Text, Text, Text> {

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

    public static class UserDrawPutInHbaseReduce2 extends TableReducer<Text, Text, NullWritable> {

        @Override
        protected void reduce(Text k2, Iterable<Text> val,
                              Reducer<Text, Text, NullWritable, Mutation>.Context context)
                throws IOException, InterruptedException {
            for (Text v2 : val) {
                String[] splited = v2.toString().split("\\|");
                //rowkey
                if (k2.toString().length() != 0) {
                    Put put = new Put(Bytes.toBytes(k2.toString()));
                    System.out.println("k2=" + k2.toString());
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
}
