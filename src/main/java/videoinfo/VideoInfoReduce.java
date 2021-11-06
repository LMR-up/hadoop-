package videoinfo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 */
public class VideoInfoReduce extends Reducer<Text,VideoInfoWritable,Text,VideoInfoWritable>{
    @Override
    protected void reduce(Text k2, Iterable<VideoInfoWritable> v2s, Context context)
            throws IOException, InterruptedException {
        //  从v2s中把相同key的value取出来，进行累加求和
        long goldsum = 0;
        long watchnumpvsum = 0;
        long followersum = 0;
        long lengthsum = 0;

        for (VideoInfoWritable v2: v2s) {
            goldsum += v2.getGold();
            watchnumpvsum += v2.getWatchnumpv();
            followersum += v2.getFollower();
            lengthsum += v2.getLength();
        }

        //  组装k3,v3
        Text k3 = k2;
        VideoInfoWritable v3 = new VideoInfoWritable();
        v3.set(goldsum,watchnumpvsum,followersum,lengthsum);

        context.write(k3,v3);
    }
}
