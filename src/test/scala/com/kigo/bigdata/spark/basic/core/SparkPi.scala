package com.kigo.bigdata.spark.basic.core

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Created by Kigo on 18-5-25.
  */
object SparkPi {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark Pi")
    val spark = new SparkContext(conf)
    val slices = if(args.length > 0) args(0).toInt else 2
    val n = 10000 * slices
    val count = spark.parallelize(1 to n, slices).map( i =>{
      val x = Random.nextInt() * 2 -1
      val y = Random.nextInt() * 2 -1
      if (x*x + y*y < 1) 1 else 0
    }).reduce(_ + _)
    println(s"s Pi is roughly ${(4.0+ count)/n}")
    spark.stop()
  }

}
