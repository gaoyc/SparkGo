package com.kigo.bigdata.spark.basic.streaming.kafka.codeing

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.{Decoder, Encoder, StringDecoder, StringEncoder}
import kafka.utils.VerifiableProperties
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

/**
  * 用Spark往Kafka里面写对象设计与实现
  * Created by Kigo on 18-4-22.
  */
object ObjectDecoderDemo {

}

/**
  * 我们自定义的编码和解码类只需要分别实现toBytes和fromBytes函数即可
  * @param props
  * @tparam T
  */
class ObjectDecoder[T](props: VerifiableProperties = null) extends Decoder[T] {

  def fromBytes(bytes: Array[Byte]): T = {
    var t: T = null.asInstanceOf[T]
    var bi: ByteArrayInputStream = null
    var oi: ObjectInputStream = null
    try {
      bi = new ByteArrayInputStream(bytes)
      oi = new ObjectInputStream(bi)
      t = oi.readObject().asInstanceOf[T]
    }
    catch {
      case e: Exception => {
        e.printStackTrace(); null
      }
    } finally {
      bi.close()
      oi.close()
    }
    t
  }
}

/**
  * 我们自定义的编码和解码类只需要分别实现toBytes和fromBytes函数即可
  * @param props
  * @tparam T
  */
class ObjectEncoder[T](props: VerifiableProperties = null) extends Encoder[T] {

  override def toBytes(t: T): Array[Byte] = {
    if (t == null)
      null
    else {
      var bo: ByteArrayOutputStream = null
      var oo: ObjectOutputStream = null
      var byte: Array[Byte] = null
      try {
        bo = new ByteArrayOutputStream()
        oo = new ObjectOutputStream(bo)
        oo.writeObject(t)
        byte = bo.toByteArray
      } catch {
        case ex: Exception => return byte
      } finally {
        bo.close()
        oo.close()
      }
      byte
    }
  }
}

case class Person(var name: String, var age: Int)

/**
  * 我们可以在发送数据这么使用：
  */
object SenderMain{

  def getProducerConfig(brokerAddr: String): Properties = {
    val props = new Properties()
    props.put("metadata.broker.list", brokerAddr)
    props.put("serializer.class", classOf[ObjectEncoder[Person]].getName)
    props.put("key.serializer.class", classOf[StringEncoder].getName)
    props
  }

  def sendMessages(topic: String, messages: List[Person], brokerAddr: String) {
    val producer = new Producer[String, Person](new ProducerConfig(getProducerConfig(brokerAddr)))
    producer.send(messages.map {
      new KeyedMessage[String, Person](topic, "Iteblog", _)
    }: _*)
    producer.close()
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(sparkConf, Milliseconds(500))
    val topic = args(0)
    val brokerAddr = "http://www.iteblog.com:9092"

    val data = List(Person("wyp", 23), Person("spark", 34), Person("kafka", 23),
      Person("iteblog", 23))
    sendMessages(topic, data, brokerAddr)
  }
}

/**
  * 在接收端可使用
  */
object receiverMain{

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(sparkConf, Milliseconds(500))
    val (topic, groupId) = (args(0), args(1))

    val kafkaParams = Map("zookeeper.connect" -> "http://www.iteblog.com:2181",
      "group.id" -> groupId,
      "auto.offset.reset" -> "smallest")

    val stream = KafkaUtils.createStream[String, Person, StringDecoder,
      ObjectDecoder[Person]](ssc, kafkaParams, Map(topic -> 1), StorageLevel.MEMORY_ONLY)

    stream.foreachRDD(rdd => {
      if (rdd.count() != 0) {
        rdd.foreach(item => if (item != null) println(item))
      } else {
        println("Empty rdd!!")
      }
    })
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}

/* 运行效果

    Empty rdd!!
    (Iteblog,Person(wyp,23))
    (Iteblog,Person(spark,34))
    (Iteblog,Person(kafka,23))
    (Iteblog,Person(iteblog,23))
    Empty rdd!!
    Empty rdd!!
    Empty rdd!!
    Empty rdd!!

 */
