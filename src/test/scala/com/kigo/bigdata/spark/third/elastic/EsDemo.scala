package com.kigo.bigdata.spark.third.elastic

import java.util.concurrent.TimeUnit

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark
import org.junit._

//@Test
class EsDemo {

  //val sc = new SparkContext("local", "test", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass).toSeq )
  val master = "local[*]"
  val conf = new SparkConf().setAppName("esDemo").setMaster(master)
  conf.set("es.nodes", "localhost")
  conf.set("es.port", "9200")
  conf.set("es.index.auto.create", "true")
  val sc = new SparkContext(conf)

  //@Test
  def testSaveToEs(): Unit ={

    //将Map对象写入ElasticSearch
    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("OTP" -> "Otopeni", "SFO" -> "San Fran")
    sc.makeRDD(Seq(numbers, airports)).saveToEs("idx_test_eshadoop/docs")
    println("success to saveToEs!!")

    //将Json字符串写入ElasticSearch
    val json1 = """{"id" : 1, "blog" : "www.iteblog.com", "weixin" : "iteblog_hadoop"}"""
    val json2 = """{"id" : 2, "blog" : "books.iteblog.com", "weixin" : "iteblog_hadoop"}"""
    sc.makeRDD(Seq(json1, json2)).saveJsonToEs("idx_test_eshadoop/json")

    //动态设置插入的type
    val game = Map("media_type"->"game","title" -> "FF VI","year" -> "1994")
    val book = Map("media_type" -> "book","title" -> "Harry Potter","year" -> "2010")
    val cd = Map("media_type" -> "music","title" -> "Surfing With The Alien")
    //type是通过{media_type}通配符设置的，这个在写入的时候可以获取到，然后将不同类型的数据写入到不同的type中。
    sc.makeRDD(Seq(game, book, cd)).saveToEs("iteblog/{media_type}")

    /**
      *自定义id
      */

    val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
    val muc = Map("iata" -> "MUC", "name" -> "Munich")
    val sfo = Map("iata" -> "SFO", "name" -> "San Fran")
    val airportsRDD = sc.makeRDD(Seq((1, otp), (2, muc), (3, sfo)))
    airportsRDD.saveToEsWithMeta("iteblog/2015")
    //上面的Seq((1, otp), (2, muc), (3, sfo))语句指定为各个对象指定了id值，分别为1、2、3。然后你可以通过/iteblog/2015/1 URL搜索到otp对象的值。我们还可以如下方式指定id：

    //Json方式自定义ID - elasticsearch-hadoop-5.6.0版本下测试，无法解析获取ID字段值
    val json3 = """{"id" : 1, "blog" : "www.iteblog.com", "weixin" : "iteblog_hadoop"}"""
    val json4 = """{"id" : 2, "blog" : "books.iteblog.com", "weixin" : "iteblog_hadoop"}"""
    val rdd = sc.makeRDD(Seq(json3, json4))
    EsSpark.saveToEs(rdd, "iteblog/docs", Map("es.mapping.id" -> "id"))
    //上面通过es.mapping.id参数将对象中的id字段映射为每条记录的id。


  }

  def testSaveToEsMutiData(): Unit ={
    for(i <- 1 to 10){
      val byteBuffer = scala.collection.mutable.ArrayBuffer[String]()
      for(j <- 1 to 300000){
        val doc: String = s"""{ "id" : ${i}_$j, "content" : "content_${i}_$j" }"""
        byteBuffer +=(doc)
      }

      val rdd = sc.makeRDD(byteBuffer)
      println("==========================\n"+byteBuffer.take(10).toSeq)
      EsSpark.saveToEs(rdd, s"index_tmp_test/$i", Map(ConfigurationOptions.ES_MAPPING_ID -> "id"))
      println(s"=======success to save rdd to es, bath=$i")
      TimeUnit.SECONDS.sleep(5)
    }



  }

  @Test
  def testSaveToEsBySpecifyId(): Unit ={
    //Json方式自定义ID方式
      // elasticsearch-hadoop-5.6.0版本下测试保存出错，无法获取ID，待解决
    val json3 = """{ id : 1, "blog" : "www.iteblog.com", "weixin" : "iteblog_hadoop"}"""
    val json4 = """{ id : 2, "blog" : "books.iteblog.com", "weixin" : "iteblog_hadoop"}"""
    val rdd = sc.makeRDD(Seq(json3, json4))
    EsSpark.saveToEs(rdd, "iteblog_test/docs", Map("es.mapping.id" -> "id"))
    //上面通过es.mapping.id参数将对象中的id字段映射为每条记录的id。
  }




/*  def main(args: Array[String]) {

//    testSaveToEs
//    testSaveToEsMutiData
    testSaveToEsBySpecifyId
  }*/
}
