# RDD总结
## 基本概念
### RDD的5大特性
1. **分区列表(a list of partitions)**
- Spark Rdd是被分区的，每一个分区都会被一个计算任务(Task)处理,分区数决定了并行计算的数量
- RDD的并行度默认传递个子分区。默认情况下，对于HDFS，一个数据分片(Block)就是一个partition。对于集合创建的RDD，默认分区数为该应用程序所分配到的资源的cpu核数, 如从HDFS文件创建，默认为文件的block数。建议每个Core承载2～4个partition

2. **每个分区都有一个计算函数(a funtion for compute each split)**
3. **依赖与其他RDD的列表(a list of dependency on other RDDs)**
4. **key-value类型的RDD分区器(a Partitioner for ke-value RDDs)控制分区策略和分区数**
   - HashPartitioner/RangePartitioner
5. **每个分区都有一个优先位置列表(a list of prefered locations to compute each split on**
  - 优先列表会存储每个partition的优先位置，对于HDFS文件，就是每个partition块的位置
  - 数据不动，代码动。代码运算前一知道要运行的数据在什么地方。Spakr任务调度是近可能的将任务分配到处理数据的数据块所在位置。

### RDD弹性特性的7个方面
1. 自动进行内存和磁盘数据储存的切换
2. 基于Lineage(血统)的高效容错机制
  - Lineage是基于Spark RDD的依赖关系来完成（宽依赖和窄依赖）,这种前后依赖关系可恢复失去的数据。出现错误时只需要回复单个split的特定部分.
  - 常规容错的两种方式：*数据检查点*和*记录数据更新*
    - 数据检查点:每次操作都要复制数据集,即拷贝，网络带宽是瓶颈，也消耗存储资源
    - 记录数据更新：每次数据变化就就记录一下，不需要重新复制一份数据,但比较复杂，耗性能。Spark RDD通过记录更新的方式很高效。
3. Task如果失败会自动进行特定次数的重试
  - 默认重试4次。源代码参见TaskSchedulerImpl,它是任务调度接口TaskScheduler的实现,这些Schedulers从每个Stage中的DAGScheduler中获取TaskSet,运行它们。
  - 
4. Stage如果失败会自动进行特定次数的重试
  - 默认重试4次，可直接运行计算失败的阶段，只计算数据失败的分片。
  - Stage对象可以跟踪对歌StageInfo（储存SparkListeners监听到的Stage信息，将Stage信息传递给Listeners或者web UI），参见Stage类
5. CheckPoint(检查点)和Persist(持久化),可主动或别动触发
  - 某些在transformation算子中Spark自己生成的RDD是不能被用户直接cache，例如reduceByKey()中生成的shuflleRDD，MapPartitionRDD是不能被用户cache的
6. 数据调度弹性(DAGScheduler, TaskScheduler和资源管理无关)
  > spark将执行模型抽象为通用的有向无环图计划(DAG),这可以将多Stage的任务串联或并行执行，从而无需将Stage的中间结果输出到HDFS中，当发生运行节点故障时可有其他可用节点代替该故障节点运行
7. 数据分片的高度弹性(coalesce)
  - Spark进行数据分片时，默认将数据放在内存，如果内存放不下，一部分会放在磁盘上进行保存。参见Coalesce类
  - 如果数据分片较多碎片，可将多个小碎片合并调整一个较大的Partition去处理; 如果一个分片较大，也可以将其分割为更小的数据分片，这让Spark处理更多的批次而避免OOM内存溢出
# RDD功能解析
## RDD的创建方式
1. 通过已存在的Scala集合创建
```
val data = Array(1 to 10 by 2)
val rdd = sc.parallelize(data)
```
2. 通过HDFS和本地文件系统创建
   > 建议阅读sc.textFile()源码,详细了解HadoopRDD创建过程,对照RDD的5大特性，重点了解创建过程中对应的5大方法
3. 通过其他RDD的转换
  >通过父RDD转换可得到新的RDD. Transformation
4. 其他RDD的创建
  - 基于DB创建RDD。详见JdbcRDD类
  - 基于S3（公开的云储存服务）
  
## RDD算子
### 要点算子总结
- 默认情况下，没一个Transformation过的RDD会在每次Action时重新计算一次，除非该RDD使用persist或者cache后，可进行复用
- 根据action算子的输出空间将Action算子进行分类：无输出, HDFS, Scala集合和数据类型。
- RDD模型适合粗粒度的全局数据并行计算，不支持细粒度的异步更新操作和增量迭代计算
- 基于RDD的整个计算过程都是发送在Worker中的executor中
### 算子的三大类型
1. Transformation算子
   - map(func) fielter flatMap mapPartitions sample intersection distinct
   - goupByKey reduceByKey aggregateByKey sortBykey join
2. Action算子
   - reduce collect count first take(n)

# RDD运行机制
## RDD的计算过程
  > 计算节点执行计算逻辑时，复用位于Executor中的线程池中的线程，线程中运行的任务是调用具体Task中的run方法进行计算，此时如果调用具体Task的run方法，需要考虑不同Stage内部具体Task的类型，Spark规定最后一个Stage中的task类型为resultTask，因为需要或者最后的结果，前面所有stage的Task都是shuffleMapTask
  - 过程要点
     1. Driver发送消息给Executor，让其启动Task
     2. Executor成功启动Task后，通过消息机制回报启动成功消息给Driver
        - ShuffleMapTask:
          - compute计算partition
          - shuffleWriter写入具体文件
          - 将MapStatus发送给Driver MapOutputTask
        - ResultTask
          - 根据前面Stage的执行结果进行shuffle后产生整个job的最后结果
     3. 执行器主工作过程要点:
        - TaskRunner在ThreadPool运行具体的Task
          - 向Driver汇报状态
          - 反序列化; Task依赖等
          - 通过网络获取需要的文件
        - 运行Thread的run方法，导致Task的抽象方法runTask被调用执行具体的业务逻辑处理
          - 在Task的runTask内部会调用RDD的iterator()方法，该方法就是我们正对当前Task所对应的Partition进行计算的关键之所在
          - 在处理的处理内部会迭代Partition的元素并交给我们自定义的function进行处理

  - 具体计算过程
    1. Driver中的CoarseGrainedSchedulerBackend给CoarseGrainedExecutorBackend发送LaunchTask消息。
    2. 首先反序列化TaskDescription
```scala
      override def receive: PartialFunction[Any, Unit] = {
        // ...
        case LaunchTask(data) =>
          if (executor == null) {
            logError("Received LaunchTask command but executor was null")
            System.exit(1)
          } else {
            val taskDesc = ser.deserialize[TaskDescription](data.value) //反序列化Task
            logInfo("Got assigned task " + taskDesc.taskId)
            //调用executor的launchTask，分配线程给Task
            executor.launchTask(this, taskId = taskDesc.taskId, attemptNumber = taskDesc.attemptNumber,
              taskDesc.name, taskDesc.serializedTask)
          }
         // ... 
      }
 ```  
     
     3. Executor会通过LaunchTask来执行Task
     4. Executor的LaunchTask方法中通过TaskRunner对象在threadPool运行具体的Task
        - 通过statusUpdate给Driver发送信息回报自己的状态
        - TaskRunner内部做一些准备工作，例如反序列化Task的依赖，通过网络获取Jar等
        - 反序列化Task本身
        - 详见Executor.scala的run方法
     5. 调用反序列化后的Task.run方法来执行任务，并获取执行结果
      


## 依赖关系
### 窄依赖
### 宽依赖
