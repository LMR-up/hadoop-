package videoinfo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 数据指标统计作业
 * 需求：
 * 1：基于主播进行统计，统计每个主播在当天收到的总金币数量，总观看PV，总粉丝关注量，总视频开播时长
 *
 * 分析：
 * 1：为了方便统计主播的指标数据，最好是把这些字段整合到一个对象中，这样维护起来比较方便，这样就需要自定义writable了
 * 2：由于在这里需要以主播维度进行数据的聚合，所以需要以主播ID作为key进行聚合统计
 * 3：所以Map节点的<k2,v2> 为 <Text,自定义Writable>
 * 4：由于需要聚合，所以Reduce阶段也需要有
 *
 *
 *
 */
public class VideoInfoJob {

    public static void main(String[] args) {
        try {
            if(args.length!=2){
                //  如果传递的参数不够，程序直接退出
                System.exit(100);
            }
            //  job需要的配置参数
            Configuration conf = new Configuration();
            //  创建一个job
            Job job = Job.getInstance(conf);

            //  注意：这一行必须设置，否则在集群中执行的是找不到WordCountJob这个类
            job.setJarByClass(VideoInfoJob.class);

            //  指定输入路径(可以是文件，也可以是目录)
            FileInputFormat.setInputPaths(job,new Path(args[0]));
            //  指定输出路径(只能指定一个不存在的目录)
            FileOutputFormat.setOutputPath(job,new Path(args[1]));

            //  指定map相关的代码
            job.setMapperClass(VideoInfoMap.class);
            //  指定k2的类型
            job.setMapOutputKeyClass(Text.class);
            //  指定v2的类型
            job.setMapOutputValueClass(VideoInfoWritable.class);

            //  指定reduce相关的代码
            job.setReducerClass(VideoInfoReduce.class);
            //  指定k3的类型
            job.setOutputKeyClass(Text.class);
            //  指定v3的类型
            job.setOutputValueClass(VideoInfoWritable.class);


            //  提交job
            job.waitForCompletion(true);


        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
