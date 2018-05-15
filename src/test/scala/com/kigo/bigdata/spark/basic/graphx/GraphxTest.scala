package com.kigo.bigdata.spark.basic.graphx

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.GraphLoader

/**
  * Created by kigo on 11/1/17.
  */
object GraphxTest {

  val conf = new SparkConf()
  val sc = new SparkContext("local[*]", "test", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass).toSeq )

  //关联操作

  // 该joinVertices运算符连接输入RDD的顶点，并返回一个新的视图，新图的顶点属性通过用户自定义的map功能作用在被连接的顶点上，没有匹配的RDD保留原始值
  val listFile = "test/graphx/datasource/web-Google.txt"
  val graph = GraphLoader.edgeListFile(sc, listFile)
  graph.vertices.take(10)
  val rawGraph = graph.mapVertices((id, atrr) => 0) //转换操作,修改顶点和边属性.这里修改每个顶点属性，索引不变
  val outDegrees = rawGraph.outDegrees  //输出端数，出度，引出次数
  val tmp = rawGraph.joinVertices(outDegrees)((_, _, optDet) => optDet)
  tmp.vertices.take(10)

}
