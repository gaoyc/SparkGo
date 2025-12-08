package com.kigo.work.hz.olap.hive

import org.apache.spark.SparkContext

/**
  * Created by Kigo on 20-3-29.
  */
object DataFilter {

  val dirtyFilterName = "olap-dirty.filter"
  def main(args: Array[String]): Unit = {

    val inpath = if(args.length > 0) args(0) else
      "/mnt/workIng/testdata/out/ware-out/ltesms-all-0308/part-00000"

    val outRoot = if(args.length > 1) args(1) else "/tmp/out-"+System.currentTimeMillis()
    val dirtyFitlers = getDirtyFilter()
    println(s"==========inpath=$inpath, outRoot=$outRoot, dirtyFitlers=$dirtyFitlers")
    val sc = new SparkContext()
    val files = sc.textFile(inpath)
    val validLines = files.flatMap(line=>{
      val fields  =line.split("${sp}")
      //CP, BEGINTIME, USERNUM, RAWRELATENUM, IMSI, LAI, CI, CGI, TAGS, LONGITUDE, LATITUDE, GEOHASH, MESSAGE
      if(fields.length > 10){
        val msg = fields(11)
        if (isDirty(msg, dirtyFitlers)) None else Some(msg)
      } else None
    })

    validLines.saveAsTextFile(outRoot)

  }

  /**
    * 同一行内空格分割的脏词必须全部匹配, 不同行匹配关系为OR
    * @param content
    * @param dirtys
    */
  def isDirty(content:String, dirtys: Set[Set[String]]): Boolean ={
    dirtys.exists(andFilters => !andFilters.exists(!content.contains(_)))
  }

  /**
    * 同一行内空格分割的脏词必须全部匹配, 不同行匹配关系为OR
    * @return
    */
  def getDirtyFilter(): Set[Set[String]] ={
    val path = this.getClass.getClassLoader.getResource(dirtyFilterName).getPath
    val lines = scala.io.Source.fromFile(path).getLines().toSet[String]
    lines.map(_.split("\\s+").toSet)
  }

}
