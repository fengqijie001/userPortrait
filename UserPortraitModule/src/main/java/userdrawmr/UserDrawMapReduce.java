package userdrawmr;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import userdrawmr.UserDrawMapReduce2.MyMap2;
import userdrawmr.UserDrawMapReduce2.MyReduce2;
import userdrawputinhbase.UserDrawPutInHbaseMap;
import userdrawputinhbase.UserDrawPutInHbaseReduce;
import util.Config;
import util.TextArrayWritable;

/**
 * Create by fengqijie
 * 2019/2/27 10:56
 */
public class UserDrawMapReduce {

    static final String INPUT_PATH = "hdfs://hadoop0:8020/input/userDraw/data";
    static final String OUT_PATH = "hdfs://hadoop0:8020/out/userDraw/out";
    static final String OUT_PATH2 = "hdfs://hadoop0:8020/out/userDraw/out2";
    static Config config = new Config();

    public static class MyMap extends Mapper<LongWritable, Text, Text, TextArrayWritable> {
        Text k = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] dataArray = line.split(config.Separator);
            String uiqkey = dataArray[Integer.parseInt(config.MDN)]
                    + dataArray[Integer.parseInt(config.appID)]; // MDN + appID
            String[] val = new String[5];
            String timenow = dataArray[Integer.parseInt(config.Date)];
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            val[0] = sdf.format(Long.parseLong(timenow));//时间
            val[1] = dataArray[Integer.parseInt(config.MDN)];// 手机号
            val[2] = dataArray[Integer.parseInt(config.appID)];// appID
            val[3] = "1";// 计数
            val[4] = dataArray[Integer.parseInt(config.ProcedureTime)];// 使用时长
            k.set(uiqkey);
            context.write(k, new TextArrayWritable(val));
        }

    }

    public static class MyReduce extends Reducer<Text, TextArrayWritable, Text, Text> {
        Text v = new Text();

        public void reduce(Text key, Iterable<TextArrayWritable> values,
                           Context context) throws IOException, InterruptedException {
            long sum = 0;
            int count = 0;
            String[] res = new String[5];
            boolean flg = true;
            for (TextArrayWritable t : values) {
                String[] vals = t.toStrings();
                if (flg) {
                    res = vals;
                }
                if (vals[3] != null) {
                    count = count + 1;

                }
                if (vals[4] != null) {
                    sum += Long.valueOf(vals[4]);
                }
            }
            res[3] = String.valueOf(count);
            res[4] = String.valueOf(sum);

            StringBuffer sb = new StringBuffer();
            sb.append(res[0]).append("|");// 时间
            sb.append(res[1]).append("|");// 手机号
            sb.append(res[2]).append("|");// appID
            sb.append(res[3]).append("|");// 计数
            sb.append(res[4]);// 使用时长
            v.set(sb.toString());
            context.write(null, v);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
        final Path outPath = new Path(OUT_PATH);
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true);
        }

        final Path outPath2 = new Path(OUT_PATH2);
        if (fileSystem.exists(outPath2)) {
            fileSystem.delete(outPath2, true);
        }

        Job job1 = Job.getInstance(conf, "UserDrawMRJob1");
        job1.setJarByClass(UserDrawMapReduce.class);

        job1.setMapperClass(MyMap.class);
        job1.setReducerClass(MyReduce.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(TextArrayWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job1, new Path(OUT_PATH));

        Boolean state1 = job1.waitForCompletion(true);
        System.out.println("job1执行成功！！！");

        if (state1) {
            conf = new Configuration();
            Job job2 = Job.getInstance(conf, "UserDrawMRJob2");
            job2.setJarByClass(UserDrawMapReduce.class);

            job2.setMapperClass(MyMap2.class);
            job2.setReducerClass(MyReduce2.class);

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job2, new Path(OUT_PATH));
            FileOutputFormat.setOutputPath(job2, new Path(OUT_PATH2));

            Boolean state2 = job2.waitForCompletion(true);
            System.out.println("job2执行成功！！！");
            if (state2) {
                conf = new Configuration();
                // 设置zookeeper
                conf.set(config.consite, config.hbaseip);
                // 设置hbase表名称
                conf.set(TableOutputFormat.OUTPUT_TABLE, config.tableDraw);
                // 将该值该大，防止hbase超时退出
                conf.set(config.coftime, config.time);

                Job job3 = Job.getInstance(conf, "UserDrawPutInHbase");
                job3.setJarByClass(UserDrawMapReduce.class);
//                TableMapReduceUtil.addDependencyJars(job3);

                FileInputFormat.setInputPaths(job3, new Path(OUT_PATH2));

                job3.setMapperClass(UserDrawPutInHbaseMap.class);
                job3.setMapOutputKeyClass(Text.class);
                job3.setMapOutputValueClass(Text.class);

                job3.setReducerClass(UserDrawPutInHbaseReduce.class);
                job3.setOutputFormatClass(TableOutputFormat.class);

                TableMapReduceUtil.initTableReducerJob(config.tableDraw, UserDrawPutInHbaseReduce.class, job3,
                        null, null, null, null, false);

                // 提交到jobTracker
                System.out.println("true：表示完成；false：表示没有完成---------" + job3.waitForCompletion(true));
                System.exit(job3.waitForCompletion(true) ? 0 : 1);
            }

        }

    }
}




























