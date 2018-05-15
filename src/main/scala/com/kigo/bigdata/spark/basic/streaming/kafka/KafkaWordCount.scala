package com.kigo.bigdata.spark.basic.streaming.kafka

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Kigo on 18-4-20.
  * 从kafka中读数据，Spark Streaming进行单词数量的计算
  */
object KafkaWordCount {

  val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
    iter.flatMap { case (x, y, z) => Some(y.sum + z.getOrElse(0)).map(i => (x, i)) }
  }
  def main(args: Array[String]): Unit = {

    val checkPointPath = "/tmp/checkPointPath/"
    //    val checkPointPath=args(0)
    val outputPath = "/tmp/sparkstreamingfile_save/"
    //    val outputPath=args(1)

    //参数用一个数组来接收：zookeeper集群、组、kafka的组、线程数量
    val Array(zkQuorum, group, topics, numThreads) = Array("localhost:2181","group_test","tp_wordcount","1")
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    sparkConf.setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    //注意，使用updateStateByKey需要配置checkpoint目录
    ssc.checkpoint(checkPointPath)
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    //保存到内存和磁盘，并且进行序列化
    val data: ReceiverInputDStream[(String, String)] =
      //创建Kafka Dstream
      KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)

    //从kafka中写数据其实也是(key,value)形式的，这里的_._2就是value
    val words = data.map(_._2).flatMap(_.split(" "))

    val wordCounts = words.map((_, 1)).updateStateByKey(updateFunc,
      new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
    wordCounts.foreachRDD((a,b)=> println(s"count time:${b},${a.collect().toList}"))
    wordCounts.saveAsTextFiles(outputPath)
    ssc.start()
    ssc.awaitTermination()
  }

}
