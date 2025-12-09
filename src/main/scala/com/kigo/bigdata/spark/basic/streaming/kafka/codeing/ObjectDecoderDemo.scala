package com.kigo.bigdata.spark.basic.streaming.kafka.codeing

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringDeserializer, StringSerializer}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.util.Try

// -----------------------------
// 1. 可序列化的数据类
// -----------------------------
case class Person(name: String, age: Int) extends Serializable

// -----------------------------
// 2. 自定义 Serializer
// -----------------------------
class PersonSerializer extends Serializer[Person] {
  override def serialize(topic: String, data: Person): Array[Byte] = {
    if (data == null) return null
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(data)
    oos.close()
    bos.toByteArray
  }

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {}
  override def close(): Unit = {}
}

// -----------------------------
// 3. 自定义 Deserializer
// -----------------------------
class PersonDeserializer extends Deserializer[Person] {
  override def deserialize(topic: String, data: Array[Byte]): Person = {
    if (data == null) return null.asInstanceOf[Person]
    val bis = new ByteArrayInputStream(data)
    val ois = new ObjectInputStream(bis)
    val obj = ois.readObject().asInstanceOf[Person]
    ois.close()
    obj
  }

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {}
  override def close(): Unit = {}
}

// -----------------------------
// 4. 发送端：使用 Kafka 新 Producer API
// -----------------------------
object SenderMain {

  def getProducerConfig(brokerAddr: String): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", brokerAddr)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[PersonSerializer].getName)
    props.put("acks", "1")
    props.put("retries", "0")
    props.put("batch.size", "16384")
    props.put("linger.ms", "1")
    props
  }

  def sendMessages(topic: String, messages: List[Person], brokerAddr: String): Unit = {
    val props = getProducerConfig(brokerAddr)
    val producer = new KafkaProducer[String, Person](props)

    try {
      messages.foreach { person =>
        val record = new ProducerRecord[String, Person](topic, "Iteblog", person)
        producer.send(record)
      }
      producer.flush()
    } finally {
      producer.close()
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: SenderMain <topic>")
      System.exit(1)
    }
    val topic = args(0)
    val brokerAddr = "localhost:9092" // 请根据实际环境修改

    val data = List(
      Person("wyp", 23),
      Person("spark", 34),
      Person("kafka", 23),
      Person("iteblog", 23)
    )

    sendMessages(topic, data, brokerAddr)
    println("All messages sent successfully.")
  }
}

// -----------------------------
// 5. 接收端：Spark Streaming + Kafka Direct Stream
// -----------------------------
object ReceiverMain {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: ReceiverMain <topic> <group.id>")
      System.exit(1)
    }
    val topic = args(0)
    val groupId = args(1)
    val brokerList = "localhost:9092" // 请根据实际环境修改

    val sparkConf = new SparkConf()
      .setAppName("ReceiverMain")
    // .setMaster("local[2]") // 本地测试时取消注释

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokerList,
      "key.deserializer" -> classOf[StringDeserializer].getName,
      "value.deserializer" -> classOf[PersonDeserializer].getName,
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val stream = KafkaUtils.createDirectStream[String, Person](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, Person](Array(topic), kafkaParams)
    )

    stream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        rdd.foreach(record => {
          val person = record.value()
          if (person != null) {
            println(s"Received: name=${person.name}, age=${person.age}")
          }
        })
      } else {
        println("Empty RDD!!")
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}