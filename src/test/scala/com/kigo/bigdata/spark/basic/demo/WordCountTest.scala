package com.kigo.bigdata.spark.basic.demo

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Before, BeforeClass, Test}

/**
  * Created by kigo on 18-4-18.
  */
class WordCountTest {

  val outRoot = "/tmp/out"

  /**
    * 每个测试方法开始前执行
    */
  @Before
  def testBefore(): Unit ={

  }

  @Test
  def testSaveAsTextFile(): Unit = {
    val readMe = "file:///home/kigo/software/spark/spark-1.6.3-bin-hadoop2.6/README.md"
    //  val conf = new SparkConf()
    //  val sc = new SparkContext(conf)
    val sc = new SparkContext("local", "test", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass).toSeq )
    val line = sc.textFile(readMe)
    val words = line.flatMap(_.split(" ")).map( word =>{
      //println(word)
      (word, 1)
    }).cache()

    val start = System.currentTimeMillis()
    val topics = words.keys.distinct().collect().filter(_.length > 0)
    topics.foreach(topic=>{
      val topicRdd = words.filter(_._1.equalsIgnoreCase(topic)).map(_._2)
      val outPath = outRoot + "/" + topic
      val out = new File(outPath)
      if(out.exists()){
        out.delete()
      }
      println(s"============start to save outPath=[$outPath]")
      topicRdd.saveAsTextFile(outPath)
      println(s"============finish to  save outPath=[$outPath]")
    })
    val end = System.currentTimeMillis()
    println(s"====elapse ${end - start} ms")

  }

  @Test
  def test: Unit ={

    println("".length)

  }

  def main(args: Array[String]) {

    val inputFile = if(args.length > 0) args(0) else "file:///home/kigo/software/spark/spark-1.6.3-bin-hadoop2.6/README.md"
    println(s"@@@@@inputFile=$inputFile")
    val conf = new SparkConf()
    //val sc = new SparkContext("local", "test", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass).toSeq )
    val sc = new SparkContext(conf)
    val line = sc.textFile(inputFile)
    val words = line.flatMap(_.split(" ")).map( word =>{
      println(word)
      (word, 1)
    }).reduceByKey(_+_)
    words.collect().foreach(println)

    if(args.length > 0){
      println(s"@@@@@ save ret to dir=${args(1)}")
      words.saveAsTextFile(args(1))
    }
    sc.stop()
  }

}
