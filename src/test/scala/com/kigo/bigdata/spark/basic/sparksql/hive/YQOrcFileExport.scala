/*
package com.kigo.bigdata.spark.basic.sparksql.hive

import java.nio.charset.Charset

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.hive.ql.io.orc.{OrcInputFormat, OrcStruct}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.teligen.store.util.DateTimeUtil
import org.teligen.store.util.etl.ExportLoadConf


/**
  * Created by kigo on 2020/3/5 0005.
  */
object YQOrcFileExport  {

  val LOG: Log = LogFactory.getLog(this.getClass)
  val evenTypes: Set[String] = Set("6", "7", "35", "36", "37", "38", "41", "42", "67", "70", "74") // 北京V4机卡数据过滤-事件类型筛选集合
  var exportSampleSize = -1
  var outRoot: String = ""

  def main(args: Array[String]) {

    lazy val taskName = "Export-task"

    val confFiles = List("exportHtable.conf") //"loader_localCommon.conf",
    val exportProps = ExportLoadConf.loadPropetiy(Charset.
        forName(ExportLoadConf.defaulCharSet), confFiles: _*)

    val dt = DateTimeUtil.sdformat.get().format(System.currentTimeMillis())

    val orcInput = exportProps.getProperty("app.task.export.inputpath", "/tmp/exporttask/input").trim
    outRoot = exportProps.getProperty("app.task.export.out", s"/tmp/exporttask/out").trim +"/" + dt

    exportSampleSize =  exportProps.getProperty("yiqing.hbase.export.sampleExport", "-1").toInt

    LOG.info(s"=========orcInput: $orcInput output $outRoot")
    val sparkConf = new SparkConf().setAppName(taskName)
    val sc = new SparkContext(sparkConf)
    val start = System.currentTimeMillis()

//    orcExportByDataframe(orcInput, sc)

    orcExportByHadoopfile(orcInput, sc)

  }


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
  def readOrcfile[V](orcInput: String, sc: SparkContext, filterFn: (OrcStruct) => Boolean, tranformRecord: (OrcStruct) => V): RDD[V] = {
    val orcRdd = sc.hadoopFile(orcInput, classOf[OrcInputFormat], classOf[NullWritable], classOf[OrcStruct])
    orcRdd.filter( t => {filterFn(t._2)} ).map(t => tranformRecord(t._2))
  }


  def orcExportByHadoopfile(orcInput: String, sc: SparkContext): Unit = {
    //    val orcRdd = sc.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat](orcInput)
    val orcRdd = sc.hadoopFile(orcInput, classOf[OrcInputFormat], classOf[NullWritable], classOf[OrcStruct])

    //不过滤数据，将各字段按逗号分隔
//    val allRowRdd = orcRdd.map(t => { (0 to t._2.getNumFields -1).map(t._2.getFieldValue(_).toString).mkString(",") })

    val targetRowRdd =  readOrcfile(orcInput, sc, filterDwLteevt, tranformOrcRecord)

    if(exportSampleSize > 0){
      // withReplacement false抽取后不放回,
      targetRowRdd.sample(false, 1, scala.util.Random.nextLong()).saveAsTextFile(outRoot)
    }else{
      targetRowRdd.saveAsTextFile(outRoot)
    }

  }

  def tranformOrcRecord(orcStruct: OrcStruct): String = {
    val fitLine = (0 to orcStruct.getNumFields - 1).map(orcStruct.getFieldValue(_)).mkString(",")
    fitLine
  }

  def filterDwLteevt(orcFields: OrcStruct): Boolean = {
    // 增加疫情项目北京抽数需求2：  relatenum=ae  state=at event=ab
    // V4版机卡过滤条件，　两者为or, 符合的数据就命中：
    //  1.relatenum (F5, ae)为空
    //  2.relatenum=00  且 state (F20)=0  且 event(F2) not in(6,7,35,36,37,38,41,42,67,70,74)
    val event = orcFields.getFieldValue(1).toString
    val relateNum = orcFields.getFieldValue(4).toString
    val state = orcFields.getFieldValue(19).toString

    LOG.trace(s"event $event relateNum $relateNum state $state")

    val bFit: Boolean = {
      relateNum match {
        case null | "" => true // 1.relatenum (F5, ae)为空
        case "00" => {
          // 2.relatenum=00  且 state (F20)=0  且 event(F2) not in(6,7,35,36,37,38,41,42,67,70,74)
          state match {
            case "0" => {
              if (!evenTypes.contains(event)) true else false
            }
            case _ => false
          }
        }
        case _ => false
      }
    }
    bFit
  }
}
*/
