package com.kigo.bigdata.spark.basic.sparksql.hive

import org.apache.hadoop.hive.ql.io.orc.{OrcInputFormat, OrcStruct}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext

import scala.reflect.ClassTag

/**
  * Created by Kigo on 20-3-24.
  */
object HiveOrcUtil {


  def orcExportByDataframe(orcInput: String, sc: SparkContext): Unit = {
    //    val sqlContext = new SQLContext(sc)
    val sqlContext = new HiveContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.

    val df = sqlContext.read.format("orc").load(orcInput)
    df.printSchema()
    df.show(10)
  }

  /**
    * 读取ORC文件，并可指定过滤功能、数据转换功能
    * @param orcInput
    * @param sc
    * @param filterFn 过滤函数
    * @param tranformRecord 转换函数
    * @tparam V
    * @return
    */
  def readOrcfile[V: ClassTag](orcInput: String, sc: SparkContext, filterFn: (OrcStruct) => Boolean, tranformRecord: (OrcStruct) => V): RDD[V] = {
    val orcRdd = sc.hadoopFile(orcInput, classOf[OrcInputFormat], classOf[NullWritable], classOf[OrcStruct])
    orcRdd.filter( t => {filterFn(t._2)} ).map(t => tranformRecord(t._2))
  }


  def tranformOrcRecordToLine(orcStruct: OrcStruct, fieldSplit: String = ","): String = {
    val fitLine = (0 to orcStruct.getNumFields - 1).map(orcStruct.getFieldValue(_)).mkString(fieldSplit)
    fitLine
  }


}
