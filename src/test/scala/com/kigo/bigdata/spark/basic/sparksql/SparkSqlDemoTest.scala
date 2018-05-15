package com.kigo.bigdata.spark.basic.sparksql

import org.apache.spark.SparkContext
import org.junit.Test

/**
  * Created by kigo on 18-4-18.
  */
class SparkSqlDemoTest {

  val SPARK_HOME =
    if(System.getenv("SPARK_HOME") != null)
      System.getenv("SPARK_HOME")
    else "/home/kigo/software/spark/spark-1.6.3-bin-hadoop2.6"

  val sc = new SparkContext("local", "testSparkSqlHiveDemo",
    System.getenv(SPARK_HOME), SparkContext.jarOfClass(this.getClass).toSeq )


  @Test
  def testDemoMutiDataSoure(): Unit ={

    MutiDataSourceQueryDemo.demoMutiDataSoure(sc)

  }

}
