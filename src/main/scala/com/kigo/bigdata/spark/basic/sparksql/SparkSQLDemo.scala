package com.kigo.bigdata.spark.basic.sparksql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kigo on 10/24/17.
  */
object SparkSQLDemo extends  App{


  /**
    * RDD转换为DataFrame
    *   以反射机制推断RDD模式
    */
  def testTransformRDDToDFByCase: Unit = {
    //以反射机制推断RDD模式
    // 必须创建case类，只有case类才能隐式转换为DataFrame
    // 必须生成DataFrame, 进行注册临时表操作
    // 必须在内存中register成临时表，才能提供查询使用
    val conf = new SparkConf()
    val sc = new SparkContext("local[*]", "test", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass).toSeq )
    val sqlContext=new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    //使用Case定义schema，属性不能超过22个
    //case class Person(name: String, age: Int)

    // 使用本地文件系统标识 "file://..."    使用hdfs文件系统标识 "hdfs://..."
    val people = sc.textFile("file:///home/kigo/software/spark/spark-1.5.1-bin-hadoop2.6/examples/src/main/resources/people.txt")
      .map(_.split(",")).map(p=>Person(p(0), p(1).trim.toInt)).toDF //将数据写入Person模式类，隐式转换为DataFrame

    //DataFrame注册成为临时表
    people.registerTempTable("peopleTable")

    //使用Sql进行SQL表达式
    val result = sqlContext.sql("SELECT name, age FROM peopleTable WHERE age >= 13")

    result.map(row => "Name[get by index]="+row(0)).collect().foreach(println) //按字段索引获取
    result.map(row => "Name[get by fieldName]="+row.getAs[String]("name")).collect().foreach(println)  //按字段名称获取结果

  }

  /**
    * RDD转换为DataFrame
    *   以编程方式定义RDD
    */
  def testTransformRDDToDFByPrograme: Unit = {
    //当匹配模式(java的JavaBean， Python的kwargs字典)不能提前被定n用户分别设计属性义的场景.例如将不同用户设计不同属性
    // step1. 从原始RDD创建一个RowS的RDD
    // step2. 创建一个StructType类型的Schema， 匹配第一步创建的RDD的RowS的结构
    // 必须在内存中register成临时表，才能提供查询使用
    val conf = new SparkConf()
    val sc = new SparkContext("local[*]", "test", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass).toSeq )
    val sqlContext=new org.apache.spark.sql.SQLContext(sc)

    // 使用本地文件系统标识 "file://..."    使用hdfs文件系统标识 "hdfs://..."
    val people = sc.textFile("file:///home/kigo/software/spark/spark-1.5.1-bin-hadoop2.6/examples/src/main/resources/people.txt")

    val schemaStr = "name age"
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{StringType, StructField, StructType}
    //生成一个基于schemaStr结构的schema
    val schema = StructType(schemaStr.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    val rowRdd = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))
    val peopleDF = sqlContext.createDataFrame(rowRdd, schema)

    //DataFrame注册成为临时表
    peopleDF.registerTempTable("peopleTable")

    //使用Sql进行SQL表达式
    val result = sqlContext.sql("SELECT name, age FROM peopleTable WHERE age >= 13")

    result.map(row => "Name[get by index]="+row(0)).collect().foreach(println) //按字段索引获取
    result.map(row => "Name[get by fieldName]="+row.getAs[String]("name")).collect().foreach(println)  //按字段名称获取结果

  }

  def testSchemaMerging: Unit ={
    val sc = new SparkContext("local[*]", "test", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass).toSeq )
    val sqlContext=new org.apache.spark.sql.SQLContext(sc)
    //隐式转换一个RDD为dataFrame
    import sqlContext.implicits._
    //创建一个DataFrame，储存数据到一个分区目录
    val df1 = sc.makeRDD(1 to 5).map(i => (i, i*2)).toDF("single", "double")
    df1.write.parquet("data/test_table/key=1")

    //创建一个新DataFrame，储存数据到一个新分区目录
    val df2 = sc.makeRDD(6 to 10).map(i => (i, i*3)).toDF("single", "triple")
    df2.write.parquet("data/test_table/key=2")

    //读取分区表
    val df3 = sqlContext.read.option("mergeSchema", "true").parquet("data/test_table")
    df3.printSchema()

    // The final schema consists of all 3 columns in the Parquet files together
    // with the partitioning column appeared in the partition directory paths.
    // root
    // |-- single: int (nullable = true)
    // |-- double: int (nullable = true)
    // |-- triple: int (nullable = true)
    // |-- key : int (nullable = true)

  }

  //testTransformRDDToDFByCase
  testTransformRDDToDFByPrograme

}

case class Person(name: String, age: Int)

object JDBCConnectDemo{

  /**
    * JDBC支持的参数列表
    * url: 待连接的JDBC URL
    * dbtable: 表名
    * driver: JDBC驱动雷鸣. 该类需可以被worker加载
    * partitionColumn, lowerBound, upperBound, numPartitions:
    *   这些选项描述了多个workers并行读取数据时如何分区表．任何一个选项被指定，所有选项都必须被指定．partitionColumn必须是一个问题表的数字列，lowerBound和upperBound是描述分区步骤，而不是过滤表中的行．所以表中所有的行将分区并返回．
    *
    * @param sqlContext
    */
  def connectJdbc(sqlContext: SQLContext): Unit ={

    val jdbcDF = sqlContext.load("jdbc", Map(
      "jdbc" -> "jdbc:postgresql:dbserver",
      "dbtable" -> "schema.tableName"
    ))

  }

}

/**
  * 多数据源整合查询示例
  */
object MutiDataSourceQueryDemo{

  val SPARK_HOME =
    if(System.getenv("SPARK_HOME") != null)
      System.getenv("SPARK_HOME")
    else "/home/kigo/software/spark/spark-1.6.3-bin-hadoop2.6"


  /**
    * 两个dataFrame数据源的保存，加载，整合(unionAll)查询示例. 使用HiveContext将数据源保存到表
    * @param sc
    */
  def demoMutiDataSoure(sc: SparkContext): Unit ={

    val sqlContext = new SQLContext(sc)

    //将一个RDD隐式转换为一个DataFrame
    import sqlContext.implicits._
    //创建一个RDD示例，包含ａ，ｂ两个字段
    val df1 =sc.parallelize(Array(("id1", "info1"), ("id2", "info2"))).map(l=>Log(l._1, l._2)).toDF()
    // 查看Schema结构
    df1.printSchema()

    /** Schema结构结果:
    root
    |-- id: string (nullable = true)
    |-- info: string (nullable = true)
    **/

    //将df1保存成Parquet文件. 如果存在覆盖重写模式
    val path1 = "file:///tmp/out/df1.parquet"
    df1.write.mode(SaveMode.Overwrite).parquet(path1)

    val df2 =sc.parallelize(Array(("id3", "info3"), ("id4", "info4"),("id5", "info5"))).map(l=>Log(l._1, l._2)).toDF()
    //将df2保存成Parquet文件. 如果存在覆盖重写模式
    val path2 = "file:///tmp/out/df2.parquet"
    df2.write.mode(SaveMode.Overwrite).parquet(path2)

    //加载数据源１
    val data1 = sqlContext.load(path1)
    //加载数据源２
    val data2 = sqlContext.load(path2)

    //进行数据源整合
    val dataUnion = data1.unionAll(data2)
    // 注册临时表
    dataUnion.registerTempTable("logs")
    // 执行查询
    sqlContext.sql("select * from logs").collect().foreach(println)

    /** == 测试将DataFrame保存到表 **/
    // 使用SQLContext创建的表必须是临时表，如要保存成非临时表. 必须使用HiveContext, 否则异常：
       // Tables created with SQLContext must be TEMPORARY. Use a HiveContext instead.
    val hiveContext = new HiveContext(sc)
    val hiveDataUion = hiveContext.load(path1).unionAll(hiveContext.load(path2))
    hiveDataUion.saveAsTable("tb_logs", SaveMode.Overwrite)

    // 打印表名
    hiveContext.tableNames().foreach(println)
    assert(hiveContext.tableNames().contains("tb_logs"), "已成功保存到表tb_logs")

  }

  case class Log(id: String, info: String)


  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local", "testSparkSqlHiveDemo",
      System.getenv(SPARK_HOME), SparkContext.jarOfClass(this.getClass).toSeq )

    demoMutiDataSoure(sc)


  }






}




