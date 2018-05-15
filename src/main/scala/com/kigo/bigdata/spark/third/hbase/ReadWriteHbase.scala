package com.kigo.bigdata.spark.third.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.spark._
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.io._
/**
  * Created by kigo on 18-4-16.
  */
class ReadWriteHbase {


  /**
    * 使用saveAsHadoopDataset写入数据
    * @param args
    */
  def testWriteHbaseBySaveAsHadoopDataset(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum","slave1,slave2,slave3")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")

    val tablename = "account"

    //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[org.apache.hadoop.hbase.mapred.TableOutputFormat])
    jobConf.set(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.OUTPUT_TABLE, tablename)

    val indataRDD = sc.makeRDD(Array("1,jack,15","2,Lily,16","3,mike,16"))


    val rdd = indataRDD.map(_.split(',')).map{arr=>{
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */
      val put = new Put(Bytes.toBytes(arr(0).toInt))
      put.add(Bytes.toBytes("cf"),Bytes.toBytes("name"),Bytes.toBytes(arr(1)))
      put.add(Bytes.toBytes("cf"),Bytes.toBytes("age"),Bytes.toBytes(arr(2).toInt))
      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, put)
    }}

    rdd.saveAsHadoopDataset(jobConf)

    sc.stop()
  }

  /**
    * 使用saveAsNewAPIHadoopDataset写入数据
    */
  def testWriteHbaseBysaveAsNewAPIHadoopDataset(): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val tablename = "account"

    sc.hadoopConfiguration.set("hbase.zookeeper.quorum","slave1,slave2,slave3")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
    sc.hadoopConfiguration.set(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.OUTPUT_TABLE, tablename)

    val job = new Job(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[org.apache.hadoop.hbase.mapreduce.TableOutputFormat[ImmutableBytesWritable]])

    val indataRDD = sc.makeRDD(Array("1,jack,15","2,Lily,16","3,mike,16"))
    val rdd = indataRDD.map(_.split(',')).map{arr=>{
      val put = new Put(Bytes.toBytes(arr(0)))
      put.add(Bytes.toBytes("cf"),Bytes.toBytes("name"),Bytes.toBytes(arr(1)))
      put.add(Bytes.toBytes("cf"),Bytes.toBytes("age"),Bytes.toBytes(arr(2).toInt))
      (new ImmutableBytesWritable, put)
    }}

    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())
  }

  /**
    * 从hbase读取数据转化成RDD
      本例基于官方提供的例子
    */
  def readFromHbase(): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val tablename = "account"
    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum","slave1,slave2,slave3")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.INPUT_TABLE, tablename)

    // 如果表不存在则创建表
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(tablename)) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tablename))
      admin.createTable(tableDesc)
    }

    //读取数据并转化成rdd。无任何scan或者filer
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[org.apache.hadoop.hbase.mapreduce.TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val count = hBaseRDD.count()
    println(count)
    hBaseRDD.foreach{case (_,result) =>{
      //获取行键
      val key = Bytes.toString(result.getRow)
      //通过列族和列名获取列
      val name = Bytes.toString(result.getValue("cf".getBytes,"name".getBytes))
      val age = Bytes.toInt(result.getValue("cf".getBytes,"age".getBytes))
      println("Row key:"+key+" Name:"+name+" Age:"+age)
    }}

    sc.stop()
    admin.close()
  }

}
