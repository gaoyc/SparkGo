package com.kigo.bigdata.spark.basic.sparksql.hive

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.junit.Test

/**
  * Created by kigo on 18-4-18.
  */
class HiveDemoTest {

  val sc = new SparkContext("local", "testSparkSqlHiveDemo",
    System.getenv(HiveDemo.SPARK_HOME), SparkContext.jarOfClass(this.getClass).toSeq )

  println(s"=======SPARK_HOME=${HiveDemo.SPARK_HOME}")

  @Test
  def testDemo(): Unit ={
    HiveDemo.demoOfficerOperation(sc)
  }

  @Test
  def testDataFrameOperation(): Unit ={

    val hiveContext = new HiveContext(sc)
    //hiveContext.sql("FROM src where ")
    // 获取所有表名
    val tables = hiveContext.tableNames()
    tables.foreach(t=>println("table="+t))

    val dataFrame = hiveContext.table(tables(0))
    // 获取全部的列名或数据类型
    val dtypes = dataFrame.dtypes
    dtypes.foreach(t=>{
      println(s"table=${tables(0)}\tcol=${t._1}\ttype=${t._2}")
    })

  }

  @Test
  def testTmp(): Unit ={


    //  val conf = new SparkConf()
    //  val sc = new SparkContext(conf)
    // IDE LOCAL MODE
    // 使用hiveContext建表命令: CREATE [EXTERNAL] TABLE [IF NOT EXISTS] tableName
    val sqlCrateTable = "CREATE TABLE IF NOT EXISTS src (key INT, value STRING)"
    //如在spark-sql shell中，已内置hiveContext, 可直接使用
    val hiveContext = new HiveContext(sc)
/*    hiveContext.sql(sqlCrateTable)
    //加载数据
    // file://... 标识本地数据
    // hdfs://... 标识HDFS存储系统数据
    val sqlLoadData = s"LOAD DATA LOCAL INPATH 'file:///${HiveDemo.SPARK_HOME}/examples/src/main/resources/kv1.txt' into TABLE src"
    hiveContext.sql(sqlLoadData)*/

    hiveContext.sql("FROM src SELECT key, value").collect().foreach(println)


  }

}
