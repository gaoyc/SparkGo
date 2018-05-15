package com.kigo.bigdata.spark.third.hbase.read

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableInputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * Created by kigo on 18-4-16.
  */
object ReadHbaseWithFilter extends Logging{

  val CF_FOR_FAMILY_USER = Bytes.toBytes("U");
  val CF_FOR_FAMILY_DEVICE = Bytes.toBytes("D")
  val QF_FOR_MODEL = Bytes.toBytes("model")
  val HBASE_CLUSTER = "hbase://xxx/"
  val TABLE_NAME = "xxx";
  val HBASE_TABLE = HBASE_CLUSTER + TABLE_NAME

  /**
    * spark的版本为1.6，hbase的版本为0.98
    * @param sc
    * @return
    */
  def readHbaseByScanWithFilter(sc:SparkContext) = {
    //20161229的数据,rowkey的设计为9999-yyyyMMdd
    val filter_of_1229 = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("79838770"))
    //得到qf为w:00-23的数据
    val filter_of_qf = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("w"))

    val all_filters = new java.util.ArrayList[Filter]()
    all_filters.add(filter_of_1229)
    all_filters.add(filter_of_qf)

    //hbase多个过滤器
    val filterList = new FilterList(all_filters)

    val scan = new Scan().addFamily(CF_FOR_FAMILY_USER)
    scan.setFilter(filterList)
    scan.setCaching(1000)
    scan.setCacheBlocks(false)

    val conf = HBaseConfiguration.create()
    conf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.INPUT_TABLE, HBASE_TABLE)
    conf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.SCAN,
      org.apache.hadoop.hbase.util.Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray()))
    sc.newAPIHadoopRDD(conf, classOf[org.apache.hadoop.hbase.mapreduce.TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result])


    //后面是针对hbase查询结果的具体业务逻辑
    // .map()
    //...
  }

    def main(args: Array[String]): Unit = {
      val Array(output_path) = args
      val sparkConf = new SparkConf().setAppName("demo")
      sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      val sc = new SparkContext(sparkConf)
      readHbaseByScanWithFilter(sc).saveAsTextFile(output_path)
      sc.stop()
    }

}
