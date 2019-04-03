package userdrawmr;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import userdraw.UserDraw;
import util.LoadHdfsTable;

public class UserDrawMapReduce2 {

    public static class MyMap2 extends Mapper<LongWritable, Text, Text, Text> {
        Text k = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] dataArray = line.split("\\|");
            String newkey = dataArray[1] ; // MDN
            k.set(newkey);
            context.write(k, value);
        }
    }

    public static class MyReduce2 extends Reducer<Text, Text, Text, Text> {
        Map<String, String[]> appMap = LoadHdfsTable.getAppMap();
        Text v = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Map<String, UserDraw> userDrawMap = new HashMap<>();
            Set<String> keySet = userDrawMap.keySet();
            String keyMDN = null;
            for (Text t : values) {

                String[] dataArray = t.toString().split("\\|");
                keyMDN = dataArray[1]; // 用户MDN
                String appID = dataArray[2]; // APPID
                // 根据appID获取对应的标签信息
                if (appID.length() > 0) { // appID不能为空
                    if (appMap.get(appID) == null) {
                        continue;
                    }
                    String favourite = appMap.get(appID)[2];
                    float male = Float.parseFloat(appMap.get(appID)[1]);
                    float female = Float.parseFloat(appMap.get(appID)[2]);
                    float age1 = Float.parseFloat(appMap.get(appID)[3]);
                    float age2 = Float.parseFloat(appMap.get(appID)[4]);
                    float age3 = Float.parseFloat(appMap.get(appID)[5]);
                    float age4 = Float.parseFloat(appMap.get(appID)[6]);
                    float age5 = Float.parseFloat(appMap.get(appID)[7]);

                    long times = Long.parseLong(dataArray[4]);
                    if (userDrawMap.containsKey(keyMDN) == true) {
                        UserDraw userDraw = userDrawMap.get(keyMDN);
                        // 性别权重
                        userDraw.protraitSex(male, female, times);
                        // 年龄段权重
                        userDraw.protraitAge(age1, age2, age3, age4, age5, times);

                    } else {
                        userDrawMap.put(keyMDN, createDrawData(dataArray, favourite, male, female, age1, age2, age3, age4, age5, times));
                    }
                }

            }
            for (String keys : keySet) {
                v.set(userDrawMap.get(keys).toString());
                context.write(null, v);

            }
        }
    }


    // 创建画像数据
    private static  UserDraw createDrawData(String[] dataArray, //
                                            String favourite, //兴趣爱好
                                            float male, float female, //性别
                                            float age1, float age2, float age3, float age4, float age5, //年龄
                                            long times) {

        UserDraw userDraw = new UserDraw();
        userDraw.setStartTimeDay(dataArray[0]);
        userDraw.setMDN(dataArray[1]);


        // 初始化
        userDraw.initAge(age1, age2, age3, age4, age5);
        userDraw.initSex(male, female);

        return userDraw;
    }
}


















