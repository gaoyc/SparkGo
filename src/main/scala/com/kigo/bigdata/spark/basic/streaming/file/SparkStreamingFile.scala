package com.kigo.bigdata.spark.basic.streaming.file

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * 监控文件夹，实现单词统计，结果保存到HDFS
  * Created by Kigo on 18-4-20.
  */
object SparkStreamingFile {

  def main(args: Array[String]): Unit = {
    val classes: Array[Class[_]] = Array[Class[_]](classOf[LongWritable], classOf[Text])
    val conf = new SparkConf().setAppName("sparkstreamingFromfile")//.setMaster("local[2]")
    conf.set("spark.streaming.fileStream.minRememberDuration", "2592000s")
    conf.set("spark.serialize", classOf[KryoSerializer].getName())
    conf.registerKryoClasses(classes)

    //设置批次间隔时间
    val streamingContext = new StreamingContext(conf, Durations.seconds(30))
    //val inputPath = "/tmp/streaming/in"
    val inputPath = args(0)
    //val outputPath = "/tmp/streaming/out/"
    val outputPath=args(1)
    val hadoopConf = new Configuration()

    //创建文件流
    val fileStream: InputDStream[(LongWritable, Text)] =
      streamingContext.fileStream[LongWritable,Text,TextInputFormat](inputPath, //扫描路径
        //匿名函数,文件过滤规则
        (path: Path) => {println(path.getName);path.getName.endsWith(".csv")},
        false, hadoopConf)

    //遍历每一行，用“,”分割
    val flatMap: DStream[String] = fileStream.flatMap(_._2.toString.split(","))

    //将每个单词标记 为1
    val mapToPair: DStream[(String, Int)] = flatMap.map((_,1))
    //将相同单词标记 累加
    val reducerByKey: DStream[(String, Int)] = mapToPair.reduceByKey(_ + _)
    reducerByKey.foreachRDD((a,b)=> println(s"count time:${b},${a.collect().toList}"))

    //结果输出到HDFS
    //reducerByKey.saveAsTextFiles(outputPath, "suffix")
    reducerByKey.saveAsTextFiles(outputPath)

    //是否触发job取决于设置的Duration时间间隔
    streamingContext.start()
    //等待程序结束
    streamingContext.awaitTermination()
  }
}