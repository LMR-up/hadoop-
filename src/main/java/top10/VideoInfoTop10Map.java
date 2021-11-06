package top10;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 实现自定义map类，在这里实现核心字段的拼装
 *
 */
public class VideoInfoTop10Map extends Mapper<LongWritable,Text,Text,LongWritable>{
    @Override
    protected void map(LongWritable k1, Text v1, Context context)
            throws IOException, InterruptedException {
        //  读取清洗之后的每一行数据
        String line = v1.toString();
        String[] fields = line.split("\t");
        String id = fields[0];
        long length = Long.parseLong(fields[4]);

        //  组装k2 v2
        Text k2 = new Text();
        k2.set(id);
        LongWritable v2 = new LongWritable();
        v2.set(length);

        context.write(k2,v2);

    }
}
