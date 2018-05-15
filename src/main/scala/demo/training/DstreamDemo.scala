package demo.training

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by Administrator on 15-8-15.
 */
object DstreamDemo {

  def main(args: Array[String]) {
    //setStreamingLogLevels()
    var srcDir = "file:///home/kigo/test/in/"  // "hdfs://localhost:9000/test/in/"
    var outRoot = "file:///tmp/dstreamOut"      // "hdfs://localhost:9000/test/out/spark-test-out/dstreamOut"
    if (args.length >  0) {
      srcDir = args(1)
      println("srcDir="+srcDir)
    }
    if (args.length >  1) {
      outRoot = args(2)
      println("outRoot="+outRoot)
    }

    println(s"######################srcDir=$srcDir,  outRoot=$outRoot")

    val conf = new SparkConf().setAppName("Dstrem-HdfsTextFileDemo")
    val ssc = new StreamingContext(conf, Seconds(5))
    val dsRdd = ssc.textFileStream(srcDir)
    dsRdd.foreachRDD(lines => {
      val words = lines.flatMap(_.split(" "))
      val wordKvs = words.map(x => (x, 1))
      val wordCounts = wordKvs.reduceByKey(_ + _)
      wordCounts.foreach(println)
      val file = outRoot +"/" + System.currentTimeMillis()
      wordCounts.saveAsTextFile(file)
    })
    ssc.start()
    ssc.awaitTermination()
  }


  /** Set reasonable logging levels for streaming if the user has not configured log4j. */
  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      println("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}
