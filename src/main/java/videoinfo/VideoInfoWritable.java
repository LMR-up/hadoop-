package videoinfo;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义数据类型
 * 主要是为了保存主播相关的核心字段，方便后期进行累加操作
 *
 */
public class VideoInfoWritable implements Writable{
    private long gold;
    private long watchnumpv;
    private long follower;
    private long length;

    public void set(long gold,long watchnumpv,long follower,long length){
        this.gold = gold;
        this.watchnumpv = watchnumpv;
        this.follower = follower;
        this.length = length;
    }

    public long getGold() {
        return gold;
    }

    public long getWatchnumpv() {
        return watchnumpv;
    }

    public long getFollower() {
        return follower;
    }

    public long getLength() {
        return length;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {//按顺序读写
        this.gold = dataInput.readLong();
        this.watchnumpv = dataInput.readLong();
        this.follower = dataInput.readLong();
        this.length = dataInput.readLong();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(gold);
        dataOutput.writeLong(watchnumpv);
        dataOutput.writeLong(follower);
        dataOutput.writeLong(length);
    }

    @Override
    public String toString() {
        return gold+"\t"+watchnumpv+"\t"+follower+"\t"+length;
    }//写入hdfs时会调用tostring方法
}
