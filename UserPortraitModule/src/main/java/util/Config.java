package util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Create by fengqijie
 * 2019/2/27 10:48
 */
public class Config {

    static Properties properties;
    static {
        properties = new Properties();
        InputStream inStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("UserDraw.properties");
        try {
            properties.load(inStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // UserDraw
    public String Separator = properties.getProperty("Separator");
    public String Date = properties.getProperty("Date");
    public String MDN = properties.getProperty("MDN");
    public String appID = properties.getProperty("appID");
    public String count = properties.getProperty("count");
    public String ProcedureTime = properties.getProperty("ProcedureTime");

    // Hbase
    public String consite = properties.getProperty("consite");
    public String hbaseip = properties.getProperty("hbaseip");
    public String coftime = properties.getProperty("coftime");
    public String time = properties.getProperty("time");
    public String tableDraw = properties.getProperty("tableDraw");
}
