package com.kigo.bigdata.spark.basic.streaming.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Kigo on 18-4-20.
  * 从kafka中读数据，Spark Streaming进行单词数量的计算
  */
object KafkaWordCount {



  def main(args: Array[String]): Unit = {
    val checkPointPath = "/tmp/checkPointPath/"
    val outputPath = "/tmp/sparkstreamingfile_save/"

    val Array(zkQuorum, group, topics, numThreads) = Array("localhost:2181", "group_test", "tp_wordcount", "1")
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    sparkConf.setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // 设置 checkpoint 目录
    ssc.checkpoint(checkPointPath)

    // Kafka 参数配置
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> zkQuorum,  // 如果使用的是 Kafka 0.10+，请替换为 Kafka 的 broker 地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    // 创建 Kafka DStream
    val data: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics.split(",").toSet, kafkaParams)
//    , new StringDecoder
//    , new StringDecoder
    )


    // 提取 value, 从kafka中写数据其实也是(key,value)形式的
//    val words = data.map(_._2).flatMap(_.split(" "))
    val words = data.map(_.value()).flatMap(_.split(" "))

    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentSum = values.sum
      val newState = state.getOrElse(0) + currentSum
      Some(newState)
    }
    // 计算单词数量
    val wordCounts = words.map((_, 1)).updateStateByKey(updateFunc)

    // 打印和保存结果
    wordCounts.foreachRDD { (rdd, time) =>
      println(s"count time: $time, ${rdd.collect().toList}")
    }

    wordCounts.saveAsTextFiles(outputPath)

    ssc.start()
    ssc.awaitTermination()
  }

  // 自定义 StringDecoder（Spark 2.x 中的 KafkaUtils 需要此解码器）
//  class StringDecoder extends org.apache.spark.streaming.kafka010.Decoder[String] with Serializable {
//    override def fromBytes(bytes: Array[Byte]): String = new String(bytes)
//  }
}