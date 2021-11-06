package top10;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class VideoInfoTop10Reduce extends Reducer<Text,LongWritable,Text,LongWritable> {
    //  保存主播的id和开播总时长
    HashMap<String, Long> map = new HashMap<>();
    @Override
    protected void reduce(Text k2, Iterable<LongWritable> v2s, Context context)
            throws IOException, InterruptedException {

        long lengthsum = 0;
        for (LongWritable v2: v2s) {
            lengthsum += v2.get();
        }
        map.put(k2.toString(),lengthsum);
    }

    /**
     * 任务初始化的时候执行一次，仅执行一次，一般在里面做一些初始化资源链接的动作
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        super.setup(context);
    }

    /**
     * 任务结束的时候执行一次，仅执行一次，做一些关闭资源的操作
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        //  从配置类中获取dt参数
        String dt = conf.get("dt");
        //  根据map中的value进行排序
        Map<String, Long> sortedMap = MapUtils.sortValue(map);
        Set<Map.Entry<String, Long>> entries = sortedMap.entrySet();
        Iterator<Map.Entry<String, Long>> it = entries.iterator();//迭代
        int count = 1;
        while(count <=10 && it.hasNext()){
            Map.Entry<String, Long> entry = it.next();
            String key = entry.getKey();
            Long value = entry.getValue();
            //  封装k3,v3
            Text k3 = new Text();
            k3.set(dt+"\t"+key);
            LongWritable v3 = new LongWritable();
            v3.set(value);
            context.write(k3,v3);
            count++;
        }
    }
}
