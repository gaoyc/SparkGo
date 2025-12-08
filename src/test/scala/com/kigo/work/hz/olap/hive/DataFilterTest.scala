package com.kigo.work.hz.olap.hive

import org.junit.Test

/**
  * Created by Kigo on 20-3-30.
  */
class DataFilterTest {

  @Test
  def testGetDirtyFilter(): Unit ={
    val dirtyWords = DataFilter.getDirtyFilter()
    dirtyWords.foreach(println)
  }


  @Test
  def testIsDirty(): Unit ={

    val source = scala.io.Source.fromFile("/home/kigo/head.log").getLines().toList
    val size = source.size
    val dirtyWords = DataFilter.getDirtyFilter()
    val filters = source.filter(!DataFilter.isDirty(_, dirtyWords))

    filters.foreach(println)
    println(s"source size = ${size} filters=${filters.size}")

  }

}
