package com.kigo.bigdata.spark.basic.core

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.SparkContext
import org.junit.Test

/**
  * Created by Kigo on 18-6-19.
  */
class SparkContextTest {

  val sc = new SparkContext("local", "test", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass).toSeq )

  @Test
  def testAccumulator(): Unit ={
    val rdd = sc.parallelize(0 to 10, 2)
    val acc = sc.accumulator[(Long)]((0l))

    val sum = rdd.map(i=>{
      acc.add(1)
      i*2
    }).sum()

    println(s"sum=$sum, acc=${acc.value}")

  }


}
