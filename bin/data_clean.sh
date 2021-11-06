#!/bin/bash

# 需要把这行内容添加到crontab中 vi /etc/crontab 定时调度 定期执行命令
# 30 00 * * * root /bin/sh /usr/local/jobs/data_clean.sh >> /data/soft/jobs/data_clean.log  (>>代表将日志文件放到指定目录下）

# 判断用户是否输入日期参数，如果没有输入则默认获取昨天日期
if [ "X$1" = "X" ]
then
    yes_time=`date +%Y%m%d --date="1 days ago"`
else
    yes_time=$1
fi


cleanjob_input=hdfs://hadoop0:9000/data/videoinfo/${yes_time}
cleanjob_output=hdfs://hadoop0:9000/data/videoinfo_clean/${yes_time}

videoinfojob_input=${cleanjob_output}
videoinfojob_output=hdfs://hadoop0:9000/res/videoinfojob/${yes_time}

videoinfojobtop10_input=${cleanjob_output}
videoinfojobtop10_output=hdfs://hadoop0:9000/res/videoinfojobtop10/${yes_time}


jobs_home=/usr/local/jobs

# 删除输出目录 为了兼容脚本重跑的情况
hdfs dfs -rm -r ${cleanjob_output}
hdfs dfs -rm -r ${videoinfojob_output}
hdfs dfs -rm -r ${videoinfojobtop10_output}

# 执行数据清洗任务
hadoop jar \
${jobs_home}/hadoopDemo-1.0-SNAPSHOT-jar-with-dependencies.jar \
dataClean.DataCleanJob \
${cleanjob_input} \
${cleanjob_output}

# 判断数据清洗任务是否执行成功
hdfs dfs -ls ${cleanjob_output}/_SUCCESS
if [ "$?" = "0" ]
then
    echo "cleanJob execute success..."
    # 执行指标统计任务1
    hadoop jar \
    ${jobs_home}/hadoopDemo-1.0-SNAPSHOT-jar-with-dependencies.jar \
    videoinfo.VideoInfoJob \
    ${videoinfojob_input} \
    ${videoinfojob_output}
    hdfs dfs -ls ${videoinfojob_output}/_SUCCESS
    if [ "$?" != "0" ]
    then
        echo "VideoInfoJob execute faild..."
        # 给管理员发短信、发邮件
        # 可以调用接口或者脚本发送短信
        #$?=1运行失败=0运行成功
    fi

    # 执行指标统计任务2

    hadoop jar \
    ${jobs_home}/hadoopDemo-1.0-SNAPSHOT-jar-with-dependencies.jar \
    top10.VideoInfoJobTop10 \
    ${videoinfojobtop10_input} \
    ${videoinfojobtop10_output}
    hdfs dfs -ls ${videoinfojobtop10_output}/_SUCCESS
    if [ "$?" != "0" ]
    then
        echo "videoInfoJobTop10 execute faild..."
        # 给管理员发短信、发邮件
        # 可以调用接口或者脚本发送短信
    fi
else
    echo "cleanJob execute faild ... date time is ${yes_time}"
    # 给管理员发短信、发邮件
    # 可以调用接口或者脚本发送短信

fi