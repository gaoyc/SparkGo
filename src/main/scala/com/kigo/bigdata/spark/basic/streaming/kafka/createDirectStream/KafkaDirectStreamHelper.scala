package com.kigo.bigdata.spark.basic.streaming.kafka.createDirectStream

/*import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkException
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.clients.consumer.ConsumerConfig

import scala.collection.JavaConverters._
import scala.collection.mutable*/

/**
  *
  * Spark1.5版本，升级Spark2待修改兼容
  *
  * KafkaDirectStreamHelper类提供两个共有方法，一个用来创建direct方式的DStream，另一个用来更新Kafka中的消费偏移量
  *
  * @param kafkaParams Kafka配置参数
  * @param topic       消费主题
  * @param groupId     消费组ID
  *
  */
class KafkaDirectStreamHelper(kafkaParams: Map[String, Object], topic: String, groupId: String) extends Serializable {

  /**
    * 获取指定主题的分区信息
    *
    * @return
    */
/*  private def getTopicPartitions(): Map[TopicPartition, Long] = {
    val topicMetadata = KafkaUtils.getPartitions(kafkaParams.asJava, topic)
    if (topicMetadata.isEmpty) {
      throw new SparkException(s"Could not find any partitions for topic $topic")
    }
    val partitions = topicMetadata.get(topic).asScala
    val earliestOffsets = KafkaUtils.earliestLeaderEpoch(kafkaParams.asJava, partitions.asJava).asScala
    val latestOffsets = KafkaUtils.latestLeaderEpoch(kafkaParams.asJava, partitions.asJava).asScala

    val zkPath = s"${topic}/${groupId}"
    val zkClient = new ZkClient(kafkaParams("bootstrap.servers").toString, 30000, 30000)

    val children = zkClient.countChildren(zkPath)
    var fromOffsets: Map[TopicPartition, Long] = Map()

    for (partition <- 0 until partitions.size()) {
      val tp = new TopicPartition(topic, partition)
      val zkOffsetPath = s"${zkPath}/${partition}"
      val zkOffset = if (children > 0 && zkClient.exists(zkOffsetPath)) {
        zkClient.readData[String](zkOffsetPath).toLong
      } else {
        earliestOffsets(tp).offset
      }

      val earliestOffset = earliestOffsets(tp).offset
      val latestOffset = latestOffsets(tp).offset

      if (zkOffset >= earliestOffset && zkOffset <= latestOffset) {
        fromOffsets += (tp -> zkOffset)
      } else {
        fromOffsets += (tp -> earliestOffset)
      }
    }

    zkClient.close()
    fromOffsets
  }*/

  /**
    * 创建DStream
    *
    * @param ssc
    * @return
    */
/*  def createDirectStream(ssc: StreamingContext): InputDStream[(String, String)] = {
    // 获取topic分区信息
    val topicPartitions = getTopicPartitions()

    // 创建自定义的locationStrategy和messageHandler
    val locationStrategy = LocationStrategies.PreferConsistent
    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())

    // 创建direct stream
    KafkaUtils.createDirectStream[String, String](
      ssc,
      locationStrategy,
      (tp: TopicPartition) => topicPartitions(tp).toInt,
      (key: String, value: String) => (key, value)
    )
  }*/

  /**
    * 更新zookeeper中的偏移量
    *
    * @param offsetRanges
    */
/*  def updateZkOffsets(offsetRanges: Array[OffsetRange]) = {
    val zkPath = s"${topic}/${groupId}"
    val zkClient = new ZkClient(kafkaParams("bootstrap.servers").toString, 30000, 30000)

    for (o <- offsetRanges) {
      val newZkPath = s"${zkPath}/${o.partition}"
      // 将该 partition 的 offset 保存到 zookeeper
      ZkUtils.updatePersistentPath(zkClient, newZkPath, o.untilOffset.toString)
    }

    zkClient.close()
  }*/
}