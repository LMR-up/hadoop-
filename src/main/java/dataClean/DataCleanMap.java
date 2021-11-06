package dataClean;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

/**
 * 实现自定义Map类，在里面实现具体的清洗逻辑
 *
 */
public class DataCleanMap extends Mapper<LongWritable,Text,Text,Text>{

    /**
     * 1: 从原始数据中过滤出来需要的字段
     * 2：针对核心字段进行异常值判断
     * @param k1
     * @param v1
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable k1, Text v1, Context context)
            throws IOException, InterruptedException {
        //  获取到每一行内容
        String line = v1.toString();
        //  把json字符串数据转成json对象
        JSONObject jsonObj = JSON.parseObject(line);
        //  从json对象中获取需要的字段信息[注意：在获取数值的时候建议使用getIntValue，如果字段缺失，则返回0]
        String id = jsonObj.getString("uid");
        int gold = jsonObj.getIntValue("gold");
        int watchnumpv = jsonObj.getIntValue("watchnumpv");
        int follower = jsonObj.getIntValue("follower");
        int length = jsonObj.getIntValue("length");
        //  过滤掉异常数据
        if(!id.equals("null") && gold>=0 && watchnumpv >=0 && follower >=0 && length >=0){
            //  组装k2,v2
            Text k2 = new Text();
            k2.set(id);//key
            Text v2 = new Text();
            v2.set(gold+"\t"+watchnumpv+"\t"+follower+"\t"+length);//value
            context.write(k2,v2);
        }

    }
}
