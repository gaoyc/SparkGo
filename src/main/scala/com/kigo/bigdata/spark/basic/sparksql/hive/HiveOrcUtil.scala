package com.kigo.bigdata.spark.basic.sparksql.hive

import org.apache.hadoop.hive.ql.io.orc.{OrcInputFormat, OrcStruct}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext

import scala.reflect.ClassTag

/**
  * 读取ORC文件工具类。
  * Created by Kigo on 20-3-24.
  */
object HiveOrcUtil {

  def orcExportByDataframe(orcInput: String, sc: SparkContext): Unit = {
    val sqlContext = new HiveContext(sc)
    val df = sqlContext.read.format("orc").load(orcInput)
    df.printSchema()
    df.show(10)
  }

  /**
    * 读取ORC文件，并可指定过滤功能、数据转换功能
    * @deprecated 旧版本spark基于 OrcStruct 的低级读取逻辑，不建议使用，后续删除。新方法：readOrcAsLines
    * @param orcInput
    * @param sc
    * @param filterFn 过滤函数
    * @param tranformRecord 转换函数
    * @tparam V
    * @return
    */
  def readOrcfile[V: ClassTag](
                                orcInput: String,
                                sc: SparkContext,
                                filterFn: (OrcStruct) => Boolean,
                                tranformRecord: (OrcStruct) => V
                              ): RDD[V] = {
    val orcRdd = sc.hadoopFile(
      orcInput,
      classOf[OrcInputFormat],
      classOf[NullWritable],
      classOf[OrcStruct]
    )
    orcRdd
      .filter(t => filterFn(t._2))
      .map(t => tranformRecord(t._2))
  }

  /**
    * @deprecated 旧版本spark基于 OrcStruct 的低级读取逻辑，不建议使用，后续删除。
    * @param orcStruct
    * @param fieldSplit
    * @return
    */
  def tranformOrcRecordToLine(orcStruct: OrcStruct, fieldSplit: String = ","): String = {
//    (0 until orcStruct.getNumFields)
//      .map(i => orcStruct.getFieldValue(i))
//      .mkString(fieldSplit)

  val values = OrcStructHelper.getFieldValues(orcStruct)
  values.map(v => if (v == null) "null" else v.toString).mkString(fieldSplit)
  }

  /**
    * spark2+ 读取ORC文件新方法
    * @param orcInput
    * @param spark
    * @param fieldSplit
    * @return
    */
  def readOrcAsLines(orcInput: String, spark: SparkSession, fieldSplit: String = ","): RDD[String] = {
    val df = spark.read.orc(orcInput)
    // 如得到 RDD[String]（而不是 Dataset[String]），不要用 df.map(...)，而是用 df.rdd.map(...)
    df.rdd.map(row => row.mkString(fieldSplit))

    // 方案二：如果确实要用 Dataset[String]，则导入 implicits
    // import spark.implicits._
    // df.map(_.mkString(fieldSplit)).rdd
  }

}



import java.lang.reflect.Field
object OrcStructHelper {
  private val fieldsField: Field = {
    val f = classOf[OrcStruct].getDeclaredField("fields")
    f.setAccessible(true)
    f
  }

  def getFieldValues(struct: OrcStruct): Array[Object] = {
    fieldsField.get(struct).asInstanceOf[Array[Object]]
  }
}