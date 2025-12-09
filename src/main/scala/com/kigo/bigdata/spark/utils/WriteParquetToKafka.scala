package com.kigo.bigdata.spark.utils

import com.kigo.bigdata.spark.basic.streaming.kafka.write.KafkaSink
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import java.time.Instant
import java.util.Properties

object WriteParquetToKafka extends Logging{

  /**
   * 提交样例：
   * ./bin/spark-submit \
   * --class org.teligen.store.refining.extraction.WriteParquetToKafka \
   * --master yarn \
   * --deploy-mode cluster \
   * your_jar_file.jar \
   * hdfs:///Bigdata18/test/00000.parquet \
   * kafka-broker1:9092,kafka-broker2:9092 \
   * your_kafka_topic
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    println(s"args len: ${args.length} args: ${args.mkString(" ")}")
    if (args.length < 3 || args(0).trim.isEmpty || args(1).trim.isEmpty || args(2).trim.isEmpty) {
      printUsageAndExit()
    }

    // 假设的参数解析逻辑
    val parquetPath = args(0)  // Parquet 文件路径
    val kafkaBootstrapServers = args(1)  // Kafka 集群地址
    val kafkaTopic = args(2)  // Kafka Topic 名称

    val spark = SparkSession.builder()
      .appName("WriteParquetToKafka")
//      .master("local[*]")
      .getOrCreate()

    // 设置 Parquet 读取的兼容性配置， for spark v3
//    spark.conf.set("spark.sql.parquet.binaryAsString", "true")
//    spark.conf.set("spark.sql.parquet.writeLegacyFormat", "true")
//    spark.conf.set("spark.sql.parquet.compression.codec", "uncompressed") // 可选，确保解压方式兼容

    // 设置 Parquet 读取的兼容性配置， for spark v2
    spark.conf.set("spark.sql.parquet.binaryAsString", "true")
    spark.conf.set("spark.sql.parquet.writeLegacyFormat", "true")
    spark.conf.set("parquet.enable.summary-metadata", "false")
    spark.conf.set("spark.sql.parquet.filterPushdown", "true")
    spark.conf.set("spark.sql.parquet.compression.codec", "uncompressed")

    val split = "|++|"

    try {
      // Step 1: 读取 Parquet 文件
      val df = spark.read
        .option("ignoreCorruptFiles", "true")
        .parquet(parquetPath)
      df.show(2) // 可选：查看数据示例

      // Step 2: 将 DataFrame 转换为 RDD 并处理为 Kafka 消息格式
      val rdd = df.rdd.map(row => {
        // 提取字段
        val text = row.getAs[Object]("text").toString
        val score = row.getAs[Object]("score").toString
        val index = row.getAs[Object]("__index__").toString
        val source = row.getAs[Object]("source").toString

        // 构造 Kafka value 字段（例如 JSON 或自定义格式）
        val value = s"$text$split$score$split$index$split$source"

        // 构造 Kafka key（可选，可以是 text 或 source）
//        val key = text
        val timestamp = Instant.now.getEpochSecond.toInt.toString //直接返回以秒为单位的时间戳（10位）
        val key = timestamp
        (key, value)
      })

      // Step: 创建一个累加器来记录发送到 Kafka 的消息总数
      val messageCountAccumulator = spark.sparkContext.accumulator(0L, "kafkaMessageCount")

      // Step: 启动一个定时任务，定期打印累加器的值
      val statsPrintInterval = 10000 // 每10秒打印一次
      val statsPrintThread = new Thread {
        override def run(): Unit = {
          while (true) {
            Thread.sleep(statsPrintInterval)
            println(s"[INFO] Total messages sent to Kafka so far: ${messageCountAccumulator.value}")
            logInfo(s"[INFO] Total messages sent to Kafka so far: ${messageCountAccumulator.value}")
          }
        }
      }
      statsPrintThread.setDaemon(true) // 设置为守护线程，避免阻塞程序退出
      statsPrintThread.start()

      // Step 3: Kafka 生产者配置
      val producerProps = new Properties()
      producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers)
      producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
      producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
      //默认default all/-1, 0-不确认， 1-一个确认即返回
      producerProps.put(ProducerConfig.ACKS_CONFIG, "1") // 可选：设置确认机制
      producerProps.put(ProducerConfig.RETRIES_CONFIG, "3") // 可选：设置重试次数
      //生产者发送数据缓存大小，默认131072 (128KB)，设置4MB
      producerProps.put(ProducerConfig.SEND_BUFFER_CONFIG, "4194304")
      // 单分区每批记录发送批量大小, 设置4M
      producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "4194304")

//      val kafkaSink = spark.sparkContext.broadcast(KafkaSink[String, String](producerProps))

      // Step 4: 将数据写入 Kafka
      rdd.foreachPartition(partition => {
        val producer = new KafkaProducer[String, String](producerProps)
        var messagesSentInPartition = 0

        partition.foreach { case (key, value) =>
          val record = new ProducerRecord[String, String](kafkaTopic, key, value)
//          val record = new ProducerRecord[String, String](kafkaTopic, value)
          try {
//            kafkaSink.value.send(kafkaTopic, key, value)
            producer.send(record)
            messagesSentInPartition += 1
            messageCountAccumulator.add(1) // 每发送一条消息就更新累加器
          } catch {
            case e: Exception =>
              // 处理 Kafka 发送异常
              println(s"Failed to send message to Kafka: $key -> $value", e)
              logError(s"Failed to send message to Kafka: $key -> $value", e)
              e.printStackTrace()
          }
        }
        producer.close()
      })

      // 等待所有分区处理完成后再停止定时打印线程
      statsPrintThread.interrupt()

      // 最终打印总消息数
      println(s"Successfully wrote data from Parquet to Kafka topic: $kafkaTopic Total messages sent to Kafka: ${messageCountAccumulator.value}" )
      logInfo(s"Successfully wrote data from Parquet to Kafka topic: $kafkaTopic Total messages sent to Kafka: ${messageCountAccumulator.value}" )

    } finally {
      spark.stop()
    }
  }

  /**
   * 输出使用帮助信息并退出程序
   */
  def printUsageAndExit(): Unit = {
    println("Usage: WriteParquetToKafka <parquetPath> <kafkaBootstrapServers> <kafkaTopic>")
    println("  parquetPath: Path to the Parquet file (e.g., hdfs:///path/to/parquet)")
    println("  kafkaBootstrapServers: Comma-separated Kafka bootstrap servers (e.g., kafka1:9092,kafka2:9092)")
    println("  kafkaTopic: Target Kafka topic to write data to (e.g., your_kafka_topic)")
    System.exit(1)
  }
}