package com.kigo.bigdata.spark.basic.sparksql.hive

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kigo on 18-4-18.
  */
object HiveDemo {

  val SPARK_HOME =
    if(System.getenv("SPARK_HOME") != null)
      System.getenv("SPARK_HOME")
    else "/home/kigo/software/spark/spark-1.6.3-bin-hadoop2.6"


  /**
    * 运行支持Hive要点
    *  (1) 配置hive-site.xml
    *      将Hive配置信息hive-site.xml添加到$SPARK_HOME/conf
    *  (2) 当运行一个Yarn集群时,datanucleus jars和hive-site.xml必须在Driver和全部的Executor启动.
    *  (3) 可通过spark-submit命令通过--jars参数和--file参数加载.
    *  (4) 如无hive-site.xml配置文件, 仍可以创建一个HiveContext, 并会在当前目录下自动创建metastore_db和warehourse.(如在IDE下执行,则在当前工程根目录下)
    * @param sc
    */
  def createHiveContext(sc: SparkContext): Unit ={

    //如在spark-sql shell中，已内置hiveContext, 可直接使用.
    // 如没有配置hive-site.xml, 仍可创建一个HiveCOntext,会在当前目录下(IDE运行环境则在当前工程根目录)的自动地创建metastore_db和warehourse.
    val hiveContext = new HiveContext(sc)

    // 获取所有表名
    val tables = hiveContext.tableNames()

    // 获取指定表的DataFrame
    val df = hiveContext.table(tables(0))

    // 获取全部的列名和数据类型
    df.dtypes

  }

  /**
    * 官方演示例子. 建表, loadData方式加载数据, 查询
    * @param sc
    */
  def demoOfficerOperation(sc: SparkContext): Unit ={
    println(s"=======SPARK_HOME=$SPARK_HOME=")

    //  val conf = new SparkConf()
    //  val sc = new SparkContext(conf)
    // IDE LOCAL MODE
    // val sc = new SparkContext("local", "testSparkSqlHiveDemo", System.getenv(SPARK_HOME), SparkContext.jarOfClass(this.getClass).toSeq )

    // 使用hiveContext建表命令, 命令格式: CREATE [EXTERNAL] TABLE [IF NOT EXISTS] tableName
    val sqlCrateTable = "CREATE TABLE IF NOT EXISTS src (key INT, value STRING)"

    //如在spark-sql shell中，已内置hiveContext, 可直接使用.
    // 如没有配置hive-site.xml, 仍可创建一个HiveCOntext,会在当前目录下(IDE运行环境则在当前工程根目录)的自动地创建metastore_db和warehourse.
    val hiveContext = new HiveContext(sc)

    /** 建表 **/
    // 如下即使没有配置hive-site.xml,运行一次会,结果依然会持久化保存在当前目录下的Hive结构数据文件
    hiveContext.sql(sqlCrateTable)

    /** 加载数据 **/
      // file://... 标识本地数据
      // hdfs://... 标识HDFS存储系统数据
    val sqlLoadData = s"LOAD DATA LOCAL INPATH 'file:///${SPARK_HOME}/examples/src/main/resources/kv1.txt' into TABLE src"
    hiveContext.sql(sqlLoadData)

    /** 查询数据 **/
    hiveContext.sql("FROM src SELECT key, value").collect().foreach(println)


  }

  /**
    * 演示HiveContext的创建.常用API接口操作等
    * @param sc
    */
  def demoSqlContext(sc: SparkContext): Unit ={

    //如在spark-sql shell中，已内置hiveContext, 可直接使用.
    // 如没有配置hive-site.xml, 仍可创建一个HiveCOntext,会在当前目录下(IDE运行环境则在当前工程根目录)的自动地创建metastore_db和warehourse.
    val hiveContext = new HiveContext(sc)

    // 获取所有表名
    val tables = hiveContext.tableNames()

    // 获取指定表的DataFrame
    val df = hiveContext.table(tables(0))

    // 获取全部的列名和数据类型
    df.dtypes

  }


  /**
    * DataFrame操作示例
    */
  def demoDataFrameOp(hiveContext: HiveContext): Unit ={

    /** DataFrame Action操作 **/

  }

}
