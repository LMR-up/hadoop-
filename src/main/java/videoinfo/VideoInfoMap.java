package videoinfo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 实现自定义Map类，在这里实现核心字段的拼装
 *
 */
public class VideoInfoMap extends Mapper<LongWritable,Text,Text,VideoInfoWritable>{

    @Override
    protected void map(LongWritable k1, Text v1, Context context)
            throws IOException, InterruptedException {
        //  读取清洗之后的每一行数据
        String line = v1.toString();
        //  使用制表符对数据进行切割
        String[] fields = line.split("\t");
        String id = fields[0];
        long gold = Long.parseLong(fields[1]);
        long watchnumpv = Long.parseLong(fields[2]);
        long follower = Long.parseLong(fields[3]);
        long length = Long.parseLong(fields[4]);

        //  组装k2 v2
        Text k2 = new Text();
        k2.set(id);

        VideoInfoWritable v2 = new VideoInfoWritable();
        v2.set(gold,watchnumpv,follower,length);

        context.write(k2,v2);

    }
}
