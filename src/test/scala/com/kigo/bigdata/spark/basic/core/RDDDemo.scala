package com.kigo.bigdata.spark.basic.core

import org.apache.spark.SparkContext
import org.junit.Test

import scala.util.Random

/**
  * Created by Kigo Gao on 2020/3/7.
  */
class RDDDemo {

  val sc = new SparkContext("local[*]", "RDDDemo")

  /**
    * 测试RDD交集操作
    */
  @Test
  def testTntersecRdd(): Unit ={

    val t1 = (1 to 6).map(i => (Random.nextInt(10), s"t-$i"))
    val t2 = (1 to 7).map(i => (Random.nextInt(10), s"t-$i"))

    val rdd1 = sc.parallelize(t1)
    val rdd2 = sc.parallelize(t2)

    //交集
    // RDD1.intersection(RDD2) 返回两个RDD的交集，并且去重
    // intersection 需要混洗数据，比较浪费性能
    val intersecRdd = rdd1.map(_._1).intersection(rdd2.map(_._1))
    val intersec = intersecRdd.collect()

    println("t1="+t1)
    println("t2="+t2)

    println("交集="+ intersec.mkString(","))

  }

  /**
    * RDD1.subtract(RDD2),返回在RDD1中出现，但是不在RDD2中出现的元素，不去重
    */
  @Test
  def TestSubtractRDD(): Unit ={
//    RDD1.subtract(RDD2),返回在RDD1中出现，但是不在RDD2中出现的元素，不去重
    val rdd1 = sc.parallelize(List("aa","bb","cc","dd"))
    val rdd2 = sc.parallelize(List("aa","dd","ff"))
    val subtractRdd = rdd1.subtract(rdd2)
    println("subtractRdd="+ subtractRdd.collect().mkString(","))

  }

  /**
    * RDD1.cartesian(RDD2) 返回RDD1和RDD2的笛卡儿积，这个开销非常大
    */
  @Test
  def TestCartesianRDD(): Unit ={
//    RDD1.subtract(RDD2),返回在RDD1中出现，但是不在RDD2中出现的元素，不去重
    val rdd1 = sc.parallelize(List("1","2","3"))
    val rdd2 = sc.parallelize(List("a","b","c"))
    val caresionRdd = rdd1.cartesian(rdd2)
    println("cartesian="+ caresionRdd.collect().mkString(","))

  }


}
