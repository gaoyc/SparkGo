package com.kigo.bigdata.spark.basic.sparksql.hive

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test
;

/**
 * Created by Kigo on 20-3-25.
 */
class HiveTeligenTest {

    //HiveContext: default warehouse location is /user/hive/warehouse
    val keyWareHourse = "hive.metastore.warehouse.dir" // spark1.x版本warehouse key
//    val keyWareHourse = "spark.sql.warehouse.dir"　// spark2.x版本warehouse key
    val warehousre = "file:///mnt/workIng/testdata/warehousre"

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("HiveOrcDemoTest")
      .set(keyWareHourse, warehousre) // 需要部署好Hive，配置warehousre才会生效
      .set("spark.sql.shuffle.partitions", "8") //spark sql设置并行度．spark.default.parallelism这个参数是在处理RDD时才会起作用的，对Spark sql来说是无效的。

    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)


    @Test
    def createTable(): Unit ={

        val sqlFile = this.getClass.getClassLoader.getResource("sql/hive-sql/tb_ltesms.sql").getPath


    }

    @Test
    def createTableLtesms(): Unit ={
//        val sql = scala.io.Source.fromFile("src/test/resources/sql/hive-sql/tb_ltesms.sql").getLines().mkString(" ")
//        println(s"sql=\n$sql")
        val dropsql = "DROP TABLE IF EXISTS TB_LTESMS_EXT"
        sqlContext.sql(dropsql)
        val createSql = "CREATE EXTERNAL TABLE IF NOT EXISTS TB_LTESMS_EXT (BEGINTIME         bigint, USERNUM         string, HOMEAREA         string, RELATENUM         string, RELATEHOMEAC         string, IMSI         string, IMEI         string, CURAREA         string, NEID         string, LAI         int, CI         string, LONGITUDE         float, LATITUDE         float, CALLTYPE         string, SENDTIME         bigint, MESSAGE         string, SID         string, MSGTAG         string, IDFLAG         int, RAWRELATENUM         string, TMSI         string, SPCODE         int, GUTI         string, CGI         string, TAGS         string, EXTRA_LONGITUDE         float, EXTRA_LATITUDE         float, GEOHASH         string, UUID         string, NEIDIPV6         STRING, AREACODE         INT ) partitioned by (cp bigint, ld bigint) stored as orc location 'file:///mnt/workIng/testdata/warehousre/tb_ltesms_ext'"
        sqlContext.sql(createSql)

        val addParSql = "alter table tb_ltesms_ext add partition (cp='2020030800',ld='2020030800') location 'cp=2020030800/ld=2020030800'"
        sqlContext.sql(addParSql)

    }

  @Test
  def testAddPartitins(): Unit ={
    //        val sql = scala.io.Source.fromFile("src/test/resources/sql/hive-sql/tb_ltesms.sql").getLines().mkString(" ")
    //        println(s"sql=\n$sql")
    val addPartitionSqlPath = this.getClass.getClassLoader.getResource("sql/hive-sql/add-partitions-ltesms.sql").getPath
    val sqls = scala.io.Source.fromFile(addPartitionSqlPath).getLines().toSet[String]
    val elapse = sqls.drop(1).map(_.replace(";",""))

    println("add partitions sql:"+ elapse.size)
//    elapse.map(sqlContext.sql(_))

    sqlContext.sql("show partitions tb_ltesms_ext").show()
  }

    @Test
    def testQueryLtesms(): Unit ={
//        sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
//        sqlContext.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

      sqlContext.sql("show tables").show
      sqlContext.sql("desc tb_ltesms_ext").show
      sqlContext.sql("show partitions tb_ltesms_ext").show
//      sqlContext.sql("select * from TB_LTESMS_TEST limit 10").show
      val sql="select CP, BEGINTIME, USERNUM, RAWRELATENUM, IMSI, LAI, CI, CGI, TAGS, LONGITUDE, LATITUDE, GEOHASH, MESSAGE FROM TB_LTESMS_EXT WHERE CP >='2020030800'  AND CP <'2020031900'"
      val ret = sqlContext.sql(sql)
      ret.take(10).foreach(println)
    }

  @Test
  def testSaveLtesms(): Unit ={
    val outFormat = "orc" // orc parquet
//    val out =  s"/mnt/workIng/testdata/out/ware-out/tb_ltesms_part_${outFormat}_out_test"
    val out =  s"/mnt/workIng/testdata/out/tb_ltesms_sample_${outFormat}_out_test"
    val sql="select CP, BEGINTIME, USERNUM, RAWRELATENUM, IMSI, LAI, CI, CGI, TAGS, LONGITUDE, LATITUDE, GEOHASH, MESSAGE FROM TB_LTESMS_EXT WHERE CP >='2020030800'  AND CP <'2020031000'"
    val ret = sqlContext.sql(sql)
    ret.printSchema()
    ret.sample(false, 0.01) //抽样1%
      .write
      .format(outFormat)
      .mode(SaveMode.ErrorIfExists) //设置模式，默认ErrorIfExists
      .partitionBy("CP") //指定分区
      .save(out)

  }


  @Test
  def testLoadOrcFile(): Unit ={
    //        val out =  "/mnt/workIng/testdata/out/ware-out/tb_ltesms_part_orc_out_test"
//    val path =  "/mnt/workIng/testdata/out/tb_ltesms_part_orc_out_test" //部分字段的结果文件
    val path = "/mnt/workIng/testdata/warehousre/tb_ltesms_ext" //原始orc文件
    val sql="select CP, BEGINTIME, USERNUM, RAWRELATENUM, IMSI, LAI, CI, CGI, TAGS, LONGITUDE, LATITUDE, GEOHASH, MESSAGE FROM TB_LTESMS_EXT WHERE CP >='2020030800'  AND CP <'2020031900'"

//    sqlContext.read.format("orc").load(path)
    // 专题原Hive使用ZLIB压缩，数据文件后缀不是.orc，因此直接编程方式加载ORC文件，除分区字段外，会丢失其它字段名信息，仅使用col0, col1, ...
    // 如果是直接spark保存的orc格式，数据文件为.orc，则会保留字段信息．
    // 如保存时使用df.format(..)partitionBy(..)指定分区保存的orc/parquet文件目录, 直接读取orc文件的方式，可以分区发现，查询时也支持分区
    val df = sqlContext.read.orc(path)
    df.printSchema()
    // df.select("*").where("CP >='2020030800'  AND CP <'2020031900'")

    df.registerTempTable("tb_ltesms_ext_tmp")
    sqlContext.sql("select count(1) from tb_ltesms_ext_tmp WHERE CP ='2020030800'").show()


  }

  @Test
  def testLoadFile(): Unit ={
    val outFormat = "orc" // orc parquet
    val tableFile = s"tb_ltesms_sample_${outFormat}_out_test"  // tb_ltesms_part_${outFormat}_out_test
    val path =  s"/mnt/workIng/testdata/out/$tableFile" //部分字段的结果文件
    val df = sqlContext.read.format(outFormat).load(path)
    df.printSchema()
    df.registerTempTable("tb_ltesms_ext_tmp")

    // 专题原Hive使用ZLIB压缩，数据文件后缀不是.orc，因此直接编程方式加载ORC文件，除分区字段外，会丢失其它字段名信息，仅使用col0, col1, ...
    // 如果是直接spark保存的orc格式，数据文件为.orc，则会保留字段信息．
    // 如保存时使用df.format(..)partitionBy(..)指定分区保存的orc/parquet文件目录, 直接读取orc文件的方式，可以分区发现，查询时也支持分区

//    val sql = s"select * from tb_ltesms_ext_tmp WHERE CP ='2020030900'"
    val sql = s"select count(1) from tb_ltesms_ext_tmp WHERE CP >='2020030822' AND CP <'2020030902' "
    sqlContext.sql(sql).show(1000, false)
    print("finish!")
  }



    def testGenerateAddPartitionDDL(): Unit ={

    }


}
