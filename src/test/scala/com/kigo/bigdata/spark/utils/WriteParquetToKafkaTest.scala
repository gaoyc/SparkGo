package com.kigo.bigdata.spark.utils

import org.apache.spark.sql.SparkSession

//import org.scalatest.flatspec.AnyFlatSpec
//import org.scalatest.matchers.should.Matchers

//class WriteParquetToKafkaTest extends AnyFlatSpec with Matchers {
object WriteParquetToKafkaTest {

  def main(args: Array[String]): Unit = {

//    val spark = SparkSession.builder()
//      .appName("WriteParquetToKafka")
//      .master("local[*]")
//      //      .config("spark.some.config.option", "some-value")
//      //      .enableHiveSupport()
//      .getOrCreate()
//
//    // For implicit conversions like converting RDDs to DataFrames
//    import spark.implicits._
//
//    val parquetPath = "E:/gyc/task-工作任务/3-方案测试类/1-testData-测试数据/fineweb-edu-dataset/00000.parquet" // Parquet 文件路径
//    // Step 1: 读取 Parquet 文件
//    val df = spark.read.parquet(parquetPath)
//    df.show(2) // 可选：查看数据示例


    test()


  }

  def test(): Unit = {
    val parquetPath = "E:/gyc/task-工作任务/3-方案测试类/1-testData-测试数据/fineweb-edu-dataset/" // Parquet 文件路径
    val kafkaBootstrapServers = "host167:9092,host168:9092,host169:9092"  // Kafka 集群地址
    val kafkaTopic = "fineweb_edu_dataset.V001"  // Kafka Topic 名称
    val args:Array[String] = Array[String](parquetPath, kafkaBootstrapServers, kafkaTopic)
    WriteParquetToKafka.main(args)
  }
}