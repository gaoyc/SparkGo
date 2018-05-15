package com.kigo.bigdata.spark.basic.streaming.kafka.write

import java.util.Properties
import java.util.concurrent.Future

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.KafkaUtils


/**
  * Created by Kigo on 18-4-22.
  */
class KafkaSink[K, V](createProducer: () => KafkaProducer[K, V]) extends Serializable {
  /* This is the key idea that allows us to work around running into
     NotSerializableExceptions. */
  lazy val producer = createProducer() //KafkaProducer利用lazy val的方式进行包装
  def send(topic: String, key: K, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, key, value))
  def send(topic: String, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, value))
}

object KafkaSink {
  import scala.collection.JavaConversions._
  def apply[K, V](config: Map[String, Object]): KafkaSink[K, V] = {
    val createProducerFunc = () => {
      val producer = new KafkaProducer[K, V](config)
      sys.addShutdownHook {
        // Ensure that, on executor JVM shutdown, the Kafka producer sends
        // any buffered messages to Kafka before shutting down.
        producer.close()
      }
      producer
    }
    new KafkaSink(createProducerFunc)
  }
  def apply[K, V](config: java.util.Properties): KafkaSink[K, V] = apply(config.toMap)
}

object KafkaSinkDemo{

  val sparkConf = new SparkConf()
  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Seconds(10))
  val kafkaParams = Map(
    //用于标识这个消费者属于哪个消费团体
    ConsumerConfig.GROUP_ID_CONFIG -> "group_demo",
    "zookeeper.connect" -> "127.0.0.1:2181",
    "key.serializer" -> classOf[StringSerializer].getName,
    "value.serializer" -> classOf[StringSerializer].getName
  )
  val topics = Map(
    "test"-> 1
  )
  val brokers = "127.0.0.1:6667, localhost:6667"

  //之后我们利用广播变量的形式，将KafkaProducer广播到每一个executor，如下：
  val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
    val kafkaProducerConfig = {
      val p = new Properties()
      //待确认broker的key是否: metadata.broker.list
      p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
      p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
      p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
      p
    }
    println("kafka producer init done!")
    ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
  }

  val kafkaStreaming = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics, StorageLevel.MEMORY_AND_DISK)


  kafkaStreaming.foreachRDD(rdd=>{
    if (!rdd.isEmpty) {
      rdd.foreach(record => {
        kafkaProducer.value.send(topics.head._1+"_out", record._1.toString, record._2)
        // do something else
      })
    }
  })


  kafkaStreaming.foreachRDD(rdd =>
    // 不能在这里创建KafkaProducer.
    // 并不能将KafkaProducer的新建任务放在foreachPartition外边，因为KafkaProducer是不可序列化的（not serializable）。显然这种做法是不灵活且低效的，因为每条记录都需要建立一次连接
    rdd.foreachPartition(partition =>
      partition.foreach{
        case x:(String, String)=>{
          val props = new java.util.HashMap[String, Object]()
          props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
          props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer")
          props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer")
          println(x)
          val producer = new KafkaProducer[String,String](props)
          val message=new ProducerRecord[String, String]("output",null,x._2)
          producer.send(message)
        }
      }
    )
  )




}
