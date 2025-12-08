package com.kigo.util

import java.io.File

/**
  * Created by Kigo on 20-3-29.
  */
object FileUtil {

  def getFile(file:File, filter: (File) => Boolean): Array[File] ={
    val files = file.listFiles()//.filter(! _.isDirectory)
      .filter(t => filter(t))
    files ++ file.listFiles().filter(_.isDirectory).flatMap(getFile(_, filter))
  }

  def main(args: Array[String]): Unit = {

    val addParDdlWithCpld = "alter table TMPL-TB add partition (cp='TMPL-CP',ld='TMPL-LD') location 'cp=TMPL-CP/ld=TMPL-LD';"
    val addParDdlWithCp = "alter table TMPL-TB add partition (cp='TMPL-CP') location 'cp=TMPL-CP';"
    val root = "/mnt/workIng/testdata/warehousre/tb_ltesms_ext"
    val allPartitions= getFile(new File(root), file =>{
      file.isDirectory && file.getName.startsWith("ld=")
    }).distinct

    val tableName  = "tb_ltesms_ext"

    val cplds = allPartitions.map(t=>(t.getParentFile.getName.replace("cp=",""), t.getName.replace("ld=","")))
    val addParDdlsCpld = cplds.map(t=>
      addParDdlWithCpld.replace("TMPL-TB", tableName)
        .replace("TMPL-CP", t._1).replace("TMPL-LD",t._2))
    val addParDdlsCp = cplds.map(_._1).distinct.map(t=>
      addParDdlWithCp.replace("TMPL-TB", tableName)
        .replace("TMPL-CP", t))
    println("===============add partitions ddls================")
    addParDdlsCpld.foreach(println)
    println("===============end add partitions ddls================")
//    addParDdlsCp.foreach(println)
  }

}
