package com.kigo.bigdata.spark.basic.sparksql.hive

import org.apache.hadoop.hive.ql.io.orc.{OrcInputFormat, OrcStruct}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

/**
  * Created by Kigo on 20-3-24.
  */
class HiveOrcUtilTest {


  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HiveOrcDemoTest")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)


  @Test
  def orcExportByHadoopfile(): Unit = {
    val orcInput = "/mnt/workIng/testdata/warehousre"
    //    val orcRdd = sc.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat](orcInput)
    val orcRdd = sc.hadoopFile(orcInput, classOf[OrcInputFormat], classOf[NullWritable], classOf[OrcStruct])

    //不过滤数据，将各字段按逗号分隔
    //    val allRowRdd = orcRdd.map(t => { (0 to t._2.getNumFields -1).map(t._2.getFieldValue(_).toString).mkString(",") })

    val targetRowRdd =  HiveOrcUtil.readOrcfile(orcInput, sc, _ => true, HiveOrcUtil.tranformOrcRecordToLine(_))

    targetRowRdd.take(10).foreach(println(_))


  }




}
