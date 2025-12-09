package com.kigo.bigdata.spark.basic.streaming.kafka.write

import java.util.Properties
import java.util.concurrent.Future
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext

/**
  * Created by Kigo on 18-4-22.
  */
class KafkaSink[K, V](createProducer: () => KafkaProducer[K, V]) extends Serializable {
  /* 延迟初始化，避免NotSerializableExceptions异常. */
  lazy val producer = createProducer() //KafkaProducer利用lazy val的方式进行包装
  def send(topic: String, key: K, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, key, value))

  def send(topic: String, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, value))
}

object KafkaSink {

  import scala.collection.JavaConversions._

  def apply[K, V](config: java.util.Properties): KafkaSink[K, V] = {
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

  def apply[K, V](config: Map[String, Object]): KafkaSink[K, V] = {
    val createProducerFunc = () => {
      val producer = new KafkaProducer[K, V](config)
      sys.addShutdownHook {
        producer.close()
      }
      producer
    }
    new KafkaSink(createProducerFunc)
  }
}

object KafkaSinkDemo {

  val sparkConf = new SparkConf()
  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Seconds(10))

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "127.0.0.1:6667,localhost:6667",
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "group.id" -> "group_demo",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Set("test")

  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
  )

  val brokers = "127.0.0.1:6667,localhost:6667"

  // 以广播变量的形式，将KafkaProducer广播到每一个executor
  val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
    val kafkaProducerConfig = {
      val p = new Properties()
      p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
      p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
      p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
      p
    }
    println("kafka producer init done!")
    ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
  }

  stream.foreachRDD { rdd =>
    if (!rdd.isEmpty) {
      rdd.foreach(record => {
        kafkaProducer.value.send("output", record.key, record.value) // 发送到新的topic
        // do something else
      })
    }
  }

  ssc.start()
  ssc.awaitTermination()
}