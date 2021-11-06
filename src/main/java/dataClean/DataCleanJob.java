package dataClean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 数据清洗作业
 * 需求：
 * 1：从原始数据(JSON格式)中过滤出来需要的字段
 *  主播ID(uid) 、金币数量(gold)、总观看PV(watchnumpv)、粉丝关注数量(follower) 、视频总开播时长(length)
 * 2: 针对核心字段进行异常值判断
 *  金币数量、总观看PV、粉丝关注数量、视频总开播时长
 *  以上四个字段正常情况下都不应该是复制，也不应该缺失
 *  如果这些字段值为负值，则认为是异常数据，直接丢弃，如果这些字段值个别有缺失，则认为字段的值为0即可
 *
 * 分析：
 * 1：由于原始数据是json格式的，所以可以使用fastjson包对原始数据进行解析，获取指定字段的内容
 * 2：然后对获取到的数据进行判断，只保留满足条件的数据即可
 * 3：由于不需要聚合过程，只是一个简单的过滤操作，所以只需要map阶段即可，reduce阶段就不需要了
 * 4：其中map阶段的k1,v1的数据类型是固定的：<LongWritable,Text>
 *     k2,v2的数据类型为：<Text,Text> k2 存储主播ID，v2 存储核心字段，多个字段中间用\t分割
 *
 *
 *
 */
public class DataCleanJob {
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
            job.setJarByClass(DataCleanJob.class);

            //  指定输入路径(可以是文件，也可以是目录)
            FileInputFormat.setInputPaths(job,new Path(args[0]));
            //  指定输出路径(只能指定一个不存在的目录)
            FileOutputFormat.setOutputPath(job,new Path(args[1]));

            //  指定map相关的代码
            job.setMapperClass(DataCleanMap.class);
            //  指定k2的类型
            job.setMapOutputKeyClass(Text.class);
            //  指定v2的类型
            job.setMapOutputValueClass(Text.class);

            //  禁用reduce
            job.setNumReduceTasks(0);

            //  提交job
            job.waitForCompletion(true);


        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
