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

* 学习了解spark streaming 检查点：


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



TensorFlow **指令**会创建、销毁和操控张量。典型 TensorFlow 程序中的大多数代码行都是指令。

TensorFlow **图**（也称为**计算图**或**数据流图**）是一种图数据结构。很多 TensorFlow 程序由单个图构成，但是 TensorFlow 程序可以选择创建多个图。图的节点是指令；图的边是张量。张量流经图，在每个节点由一个指令操控。一个指令的输出张量通常会变成后续指令的输入张量。TensorFlow 会实现**延迟执行模型**，意味着系统仅会根据相关节点的需求在需要时计算节点。

张量可以作为**常量**或**变量**存储在图中。您可能已经猜到，常量存储的是值不会发生更改的张量，而变量存储的是值会发生更改的张量。不过，您可能没有猜到的是，常量和变量都只是图中的一种指令。常量是始终会返回同一张量值的指令。变量是会返回分配给它的任何张量的指令。

要定义常量，请使用 `tf.constant` 指令，并传入它的值。例如：

```python
  x = tf.constant([5.2])
```

同样，您可以创建如下变量：

```python
  y = tf.Variable([5])
```

或者，您也可以先创建变量，然后再如下所示地分配一个值（注意：您始终需要指定一个默认值）：

```python
  y = tf.Variable([0])
  y = y.assign([5])
```

定义一些常量或变量后，您可以将它们与其他指令（如 `tf.add`）结合使用。在评估 `tf.add` 指令时，它会调用您的 `tf.constant` 或 `tf.Variable` 指令，以获取它们的值，然后返回一个包含这些值之和的新张量。

图必须在 TensorFlow **会话**中运行，会话存储了它所运行的图的状态：

```python
with tf.Session() as sess:
  initialization = tf.global_variables_initializer()
  print(y.eval())
```

在使用 `tf.Variable` 时，您必须在会话开始时调用 `tf.global_variables_initializer`，以明确初始化这些变量，如上所示。

**注意：**会话可以将图分发到多个机器上执行（假设程序在某个分布式计算框架上运行）。有关详情，请参阅[分布式 TensorFlow](https://www.tensorflow.org/deploy/distributed)。

### 总结

TensorFlow 编程本质上是一个两步流程：

1. 将常量、变量和指令整合到一个图中。
2. 在一个会话中评估这些常量、变量和指令。

## 创建一个简单的 TensorFlow 程序

我们来看看如何编写一个将两个常量相加的简单 TensorFlow 程序。

### 添加 import 语句

与几乎所有 Python 程序一样，您首先要添加一些 `import` 语句。
当然，运行 TensorFlow 程序所需的 `import` 语句组合取决于您的程序将要访问的功能。至少，您必须在所有 TensorFlow 程序中添加 `import tensorflow` 语句：

```python
import tensorflow as tf
```

**请勿忘记执行前面的代码块（`import` 语句）。**

其他常见的 import 语句包括：

```python
import matplotlib.pyplot as plt # 数据集可视化。
import numpy as np              # 低级数字 Python 库。
import pandas as pd             # 较高级别的数字 Python 库。
```

TensorFlow 提供了一个**默认图**。不过，我们建议您明确创建自己的 `Graph`，以便跟踪状态（例如，您可能希望在每个单元格中使用一个不同的 `Graph`）。

```python
import tensorflow as tf

# 创建一个图
g = tf.Graph()

# 将图形设置为“默认”图形。
with g.as_default():
    # 组装包含以下三个操作的图形：
    #   * 两个tf.constant操作来创建操作数
    #   * 一个tf.add操作添加两个操作数。
    x = tf.constant(8, name="x_const")
    y = tf.constant(5, name="y_const")
    sum = tf.add(x, y, name="x_y_sum")
    # 现在创建一个回话
    # 这个回话将运行默认图
    with tf.Session() as sess:
        print(sum.eval())
```
