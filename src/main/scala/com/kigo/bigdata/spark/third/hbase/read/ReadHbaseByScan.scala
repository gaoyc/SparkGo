package com.kigo.bigdata.spark.third.hbase.read

import java.io.{ByteArrayOutputStream, DataOutputStream, IOException}

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kigo on 18-4-16.
  */
class ReadHbaseByScan {


  /**
    * Hadoop2.7.2
      Hbase1.2.0
      Spark2.1.0
      Scala2.11.8
      实现了使用spark查询hbase特定的数据，然后统计出数量最后输出，当然上面只是一个简单的例子，重要的是能把hbase数据转换成RDD，只要转成RDD我们后面就能进行非常多的过滤操作。
    注意上面的hbase版本比较新，如果是比较旧的hbase，如果自定义下面的方法将scan对象给转成字符串，代码如下：
    */
  def testReadByNewAPIHadoopRDD(): Unit ={

    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()

    val startRowkey="row1"
    val endRowkey="row1"
    //开始rowkey和结束一样代表精确查询某条数据

    //组装scan语句
    val scan=new Scan(Bytes.toBytes(startRowkey),Bytes.toBytes(endRowkey))
    scan.setCacheBlocks(false)
    scan.addFamily(Bytes.toBytes("ks"));
    scan.addColumn(Bytes.toBytes("ks"), Bytes.toBytes("data"))

    //将scan类转化成string类型
    //val scan_str= TableMapReduceUtil.convertScanToString(scan)
    //意上面的hbase版本比较新，如果是比较旧的hbase，如果自定义下面的方法convertScanToString将scan对象给转成字符串
    val scan_str= convertScanToString(scan)
    conf.set(TableInputFormat.SCAN,scan_str)
    //最后，还有一点，上面的代码是直接自己new了一个scan对象进行组装，当然我们还可以不自己new对象，全部使用TableInputFormat下面的相关的常量，并赋值，最后执行的时候TableInputFormat会自动帮我们组装scan对象这一点通过看TableInputFormat的源码就能明白。 TableInputFormat的静态变量都可以conf.set的时候进行赋值，最后任务运行的时候会自动转换成scan，有兴趣的朋友可以自己尝试。

    //使用new hadoop api，读取数据，并转成rdd
    val rdd = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result])

    //打印扫描的数据总量
    println("count:"+rdd.count)

    //打印每条记录
    rdd.foreach{case (_,result) =>{
      //获取行键
      val key = Bytes.toString(result.getRow)
      //通过列族和列名获取列
      val name = Bytes.toString(result.getValue("cf".getBytes,"name".getBytes))
      val age = Bytes.toInt(result.getValue("cf".getBytes,"age".getBytes))
      println("Row key:"+key+" Name:"+name+" Age:"+age)
    }}

  }

  /**
    * 注意上面的hbase版本比较新，如果是比较旧的hbase，如果自定义下面的方法将scan对象给转成字符串，代码如下：
    * @param scan
    * @return
    */
/*  def convertScanToString(scan: Scan): String = {
    val out: ByteArrayOutputStream = new ByteArrayOutputStream
    val dos: DataOutputStream = new DataOutputStream(out)
    scan.write(dos)
    org.apache.hadoop.hbase.util.Base64.encodeBytes(out.toByteArray)
  }*/

  @throws[IOException]
  def convertScanToString(scan: Scan): String = {
    val proto: ClientProtos.Scan = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }


}
