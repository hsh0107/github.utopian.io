---
layout:     post
title:      pyspark设置 checkpoint
subtitle:   spark streaming中checkpoint分析
date:       2019-12-19
author:     hsh0107 
header-img: img/earth.jpg
catalog: true
tags:
    - spark streaming 
    - 流计算
    - pyspark
---


**学习目标**:

* 学习了解spark streaming checkpoint：

## 背景
Spark Streaming 是一套优秀的实时计算框架。其良好的可扩展性、高吞吐量以及容错机制能够满足我们很多的场景应用，
其优秀的特点给我们带来很多的应用场景，医疗监测、异常监控、网页点击、用户行为、用户迁移....等诸多方面

## 概念概览

Checkpoint的产生就是为了相对而言更加可靠的持久化数据，在Checkpoint 可以指定把数据放在本地并且是多副本的方式，但是在正常生产环境下放在HDFS上，这就天然的借助HDFS 高可靠的特征来完成最大化的可靠的持久化数据的方式


## 官方文档描述
### When to enable Checkpointing

1.Usage of stateful transformations - If either updateStateByKey or reduceByKeyAndWindow (with inverse function) is used in the application, then the checkpoint directory must be provided to allow for periodic RDD checkpointing.

2.Recovering from failures of the driver running the application - Metadata checkpoints are used to recover with progress information.

### How to configure Checkpointing
Checkpointing can be enabled by setting a directory in a fault-tolerant, reliable file system (e.g., HDFS, S3, etc.) to which the checkpoint information will be saved. This is done by using streamingContext.checkpoint(checkpointDirectory). This will allow you to use the aforementioned stateful transformations. Additionally, if you want to make the application recover from driver failures, you should rewrite your streaming application to have the following behavior.

When the program is being started for the first time, it will create a new StreamingContext, set up all the streams and then call start().
When the program is being restarted after failure, it will re-create a StreamingContext from the checkpoint data in the checkpoint directory.

```python
def functionToCreateContext():
    sc = SparkContext(...)  # new context
    ssc = StreamingContext(...)
    lines = ssc.socketTextStream(...)  # create DStreams
    ...
    ssc.checkpoint(checkpointDirectory)  # set checkpoint directory
    return ssc

# Get StreamingContext from checkpoint data or create a new one
context = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext)

# Do additional setup on context that needs to be done,
# irrespective of whether it is being started or restarted
context. ...

# Start the context
context.start()
context.awaitTermination()
```
## 一个完整的实例
```python
check_point_path = "hdfs://master:9000/streaming/checkpoint/"

def createContext():
    sc = SparkContext(appName="operation log")
    sc.setLogLevel("INFO")
    ssc = StreamingContext(sparkContext=sc, batchDuration=1)
    kafkaStreams = KafkaUtils.createDirectStream(ssc, [topic], kafkaParams={"metadata.broker.list": brokers})
    lines = kafkaStreams.map(lambda x: x[1])
    running_counts = lines.map(lambda line: (line.split(",")[0], line.split(",")[1:])).updateStateByKey(updateFunc, initialRDD=ssc.sparkContext.parallelize([]))
    running_counts.foreachRDD(backend)
    return ssc


if __name__ == "__main__":
    ssc = StreamingContext.getOrCreate(checkpointPath=check_point_path, setupFunc=lambda: createContext())
    ssc.start()
    ssc.awaitTermination()
```

![](https://utopian.oss-cn-beijing.aliyuncs.com/blog/1.jpeg)


以上实例， 以kafka为数据源,spark读取数据计算写入redis 


