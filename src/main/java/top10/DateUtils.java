package top10;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 日期工具类
 *
 */
public class DateUtils {
    private static SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd");
    private static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");

    /**
     * 转换日期格式
     * 从yyyyMMdd转换为yyyy-MM-dd
     * @param dt
     * @return
     */
    public static String transDataFormat(String dt){
        String res = "1970-01-01";
        try {
            Date date = sdf1.parse(dt);
            res = sdf2.format(date);
        }catch (Exception e){
            System.out.println("日期转换失败："+dt);
        }
        return res;
    }
}
