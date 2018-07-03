package com.kigo.framework.log4j2

import java.io.File

import org.apache.commons.logging.LogFactory
import org.apache.spark.{Logging, SparkContext}
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
  * Created by Kigo on 18-7-3.
  */
class Log4j2Test extends Logging {
  val LOG = LogFactory.getLog("org.teligen.store.util.KafkaManager")
  val logger = LoggerFactory.getLogger(this.getClass());

  @Test
  def testLog4j2(): Unit ={

    logInfo(s"with logginig: info++++++++++++")
    logDebug(s"with logginig: debug++++++++++++")
    logWarning(s"with logginig: warn++++++++++++")
    logError(s"with logginig: error++++++++++++")

    LOG.info(s"LogFactory.getLog info++++++++++++")
    LOG.debug(s"LogFactory.getLog debug++++++++++++")
    LOG.warn(s"LogFactory.getLog warn++++++++++++")
    LOG.error(s"LogFactory.getLog error++++++++++++")

    logger.info(s"info============")
    logger.debug(s"debug============")
    logger.warn(s"warn============")
    logger.error(s"error============")
  }


  @Test
  def testWordCount(): Unit = {
    val readMe = "file:///home/kigo/software/spark/spark-1.6.3-bin-hadoop2.6/README.md"
    //  val conf = new SparkConf()
    //  val sc = new SparkContext(conf)
    val sc = new SparkContext("local", "test", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass).toSeq )
    val line = sc.textFile(readMe)
    val words = line.flatMap(_.split(" ")).map( word =>{
      //println(word)
      (word, 1)
    }).cache()

    words.reduceByKey(_+_).collect().foreach(t=>{
      logger.warn(t.toString())
    })


  }

}
