package com.kigo.bigdata.spark.inputoutput

/**
  * Created by Kigo Gao on 2021/6/10.
  */

import org.apache.hadoop.io.NullWritable

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {

  //文件内容：默认同时输出key和value。这里指定不输出key。
  override def generateActualKey(key: Any, value: Any): Any =
    NullWritable.get() //key设置为空,文件内容中不输出，


  //文件名：根据key和value自定义输出文件名。
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
    //key.asInstanceOf[String]
    key.toString+"/"+name

  // 叶子文件名，默认part-000*
  override def generateLeafFileName(name: String): String = super.generateLeafFileName(name)
}

object MultipleTextOutputFormatTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SplitTest")
//    val sc = new SparkContext(conf)
    val sc = new SparkContext("local", "test", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass).toSeq )
    sc.parallelize(List(("w", "www"), ("b", "blog"), ("c", "com"), ("w", "bt")))
      .map(value => (value._1, (value._2 + "Test").getBytes()))
      .partitionBy(new HashPartitioner(3))
      .saveAsHadoopFile("/D:/tmp/out", classOf[String], classOf[String],
        classOf[RDDMultipleTextOutputFormat])
    sc.stop()
  }
}