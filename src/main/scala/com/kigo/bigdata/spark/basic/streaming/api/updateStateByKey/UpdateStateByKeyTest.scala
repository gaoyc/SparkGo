package com.kigo.bigdata.spark.basic.streaming.api.updateStateByKey

//import kafka.producer.KeyedMessage
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}

import java.util.Properties
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.util.Random

/**
  * Created by kigo on 18-4-21.
  * spark streaming - kafka updateStateByKey
  * 统计所有时间所有用户的消费情况(使用updateStateByKey来实现)
  */
object UpdateStateByKeyTest {
  def functionToCreateContext(): StreamingContext = {
    // 创建streamingContext
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(60))

    // 将数据进行保存（这里作为演示，生产中保存在hdfs）
    ssc.checkpoint("checkPoint")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "group.id" -> "user_payment",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Set("user_payment")

    // 使用新版 KafkaUtils.createDirectStream 方法
    val user_payment = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    ).map(record => {
      record.value.trim
    })

    // 对一分钟的数据进行计算
    val paymentSum = user_payment.map(line => {
      val tp = line.split(",")
      val user = tp(0)
      val payment = tp(1).toDouble
      (user, payment)
    }).reduceByKey(_ + _)

    // 输出每分钟的计算结果
    paymentSum.print()

    // 将以前的数据和最新一分钟的数据进行求和
    val addFunction = (currValues: Seq[Double], prevState: Option[Double]) => {
      val currentSum = currValues.sum
      val previousSum = prevState.getOrElse(0.0)
      Some(currentSum + previousSum)
    }

    val totalPayment = paymentSum.updateStateByKey[Double](addFunction)

    // 输出总计的结果
    totalPayment.print()
    ssc
  }

  // 如果"checkPoint"中存在以前的记录，则重启streamingContext,读取以前保存的数据，否则创建新的StreamingContext
  val context = StreamingContext.getOrCreate("checkPoint", functionToCreateContext _)

  context.start()
  context.awaitTermination()
}

object KafkaProducer extends App {

  // 所有用户
  private val users = Array(
    "Kigo", "Gao",
    "Li", "Youtong"
  )

  private val random = new Random

  // 消费的金额（0-9）
  def payMount(): Double = {
    random.nextInt(10)
  }

  // 随机获得用户名称
  def getUserName(): String = {
    users(random.nextInt(users.length))
  }

  // Kafka参数
  val topic = "user_payment"
  val brokers = "127.0.0.1:9092,localhost:9092"
  val props = new Properties()
  props.put("metadata.broker.list", brokers)
  props.put("serializer.class", "kafka.serializer.StringEncoder")

//  val kafkaConfig = new ProducerConfig(props)
  val producer = {
    new KafkaProducer[String, String](props)
  }

  while (true) {
    // 创建消息
    val event = getUserName + "," + payMount()
    // 往kafka发送数据
    producer.send(new ProducerRecord[String, String](topic, event.toString))
    println("Message sent: " + event)

    // 每隔200ms发送一条数据
    Thread.sleep(200)
  }
}