package com.kigo.bigdata.spark.basic.streaming.kafka

import kafka.api.OffsetRequest
import kafka.serializer.{DefaultDecoder, StringDecoder, StringEncoder}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Kigo on 18-4-21.
  * spark2.1 支持kafka0.8.2.1以上的jar,样例基于:
  *   1) 基于是spark2.0.2,下载的kafka_2.11-0.10.2.0
  *   2) 支持spark1.5.1,下载的spark-streaming-kafka_2.10
  *
  *  该样例基于Spark Streaming直连的方式消费Kafka中的数据(createDirectStream接口)
  *
  *
  */
object SparkStreamKaflaWordCount {
  def main(args: Array[String]): Unit = {
    //创建streamingContext
    var conf=new SparkConf().setMaster("spark://127.0.0.1:7077")
      .setAppName("SparkStreamKaflaWordCount_Demo");
    var ssc=new StreamingContext(conf,Seconds(4));
    //创建topic
    //var topic=Map{"test" -> 1}
    var topic=Array("test");
    //指定zookeeper
    //创建消费者组
    var group="group_consumer"
    //消费者配置
    val kafkaParam = Map(
      //zookeeper.connect 如果使用高级消费,必须配置指定该配置项! 与bootstrap.servers无关
      //metadata.broker.list 如是低级消费,需要指定该配置, 或者bootstrap.servers?
      //ProducerConfig.BOOTSTRAP_SERVERS_CONFIG
      "bootstrap.servers" -> "127.0.0.1:9092,localhost:9092",//用于初始化链接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      ConsumerConfig.GROUP_ID_CONFIG -> group,
        //"group.id" -> group,
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> OffsetRequest.LargestTimeString,
        //"auto.offset.reset" -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
        //注意: spark-streaming-kafka_2.10 配置类型必须是(String, String). 待确定版本无该如下配置?
      "enable.auto.commit" -> (false: java.lang.Boolean)
    );

    //创建DStream，返回接收到的输入数据
    // spark2.0.2 kafka_2.11-0.10.2.0 版本
      //var stream=KafkaUtils.createDirectStream[String,String](ssc, PreferConsistent,Subscribe[String,String](topic,kafkaParam))
      //每一个stream都是一个ConsumerRecord
      //stream.map(s =>(s.key(),s.value())).print();

    // spark-streaming-kafka_2.10
      // 该版本的API的kafkaParams类型必须是 Map[String, String], 需要转换kafkaParam
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParam.map(t=>(t._1 -> t._2.toString)), topic.toSet)

    // 如需要返回的消息为原始的字节数组, 可使用如下类型及解码器
//   val byteStream = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
//        ssc, kafkaParam.map(t=>(t._1 -> t._2.toString)), topic.toSet)

    //每一个stream都是一个ConsumerRecord
    stream.map(s =>(s._1, s._2)).print();
    ssc.start();
    ssc.awaitTermination();
  }
}

/*
说明

1. 在提交spark应用程序的时候，抛出类找不到, 错误信息如下:
Exception in thread "main" java.lang.NoClassDefFoundError: org/apache/kafka/common/serialization/StringDeserializer
解决:
这个需要你将【spark-streaming-kafka-0-10_2.11-2.1.0】，【kafka-clients-0.10.2.0】这两个jar添加到 spark_home/jar/路径下就可以了。（这个只是我这个工程里缺少的jar）

 */
