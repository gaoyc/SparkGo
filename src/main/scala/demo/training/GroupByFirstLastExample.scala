package demo.training

import org.apache.spark.{SparkConf, SparkContext}

object GroupByFirstLastExample {
  def main(args: Array[String]): Unit = {
    // 初始化 SparkContext
    val conf = new SparkConf().setAppName("GroupByFirstLast").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 创建原始 RDD 数据
    val data = List(
//      ("a", 1), ("b", 2), ("a", 3), ("b", 4),
//      ("a", 5), ("c", 6), ("c", 7), ("d", 10)

      (1668287557,1668287557),
      (1668287567,1668287567),
      (1668287577,1668287577),
      (1668287601,1668287601),
      (1668387537,1668387537),
      (1668387547,1668387547),
      (1668387557,1668387557),
      (1668387567,1668387567),
      (1668387577,1668387577),
      (1668387601,1668387601)
    )
    val rdd = sc.parallelize(data)

    // 按 Key 分组
//    val groupedRDD = rdd.groupBy(_._1)
    val groupedRDD = rdd.groupBy(_._1/86400)

    // 对每个分组的值进行排序，然后取出首尾两个值
    val resultRDD = groupedRDD.flatMap { case (key, values) =>
      val valueList = values.toList
      val sortedList = valueList.sortBy(_._2) // 按 value 升序排序
      if (sortedList.isEmpty) {
        Nil // 如果为空，返回空列表
      } else {
        val first = sortedList.head._2
        val last = sortedList.last._2
        List((key, first), (key, last)) // 生成两个记录
      }
    }

    // 收集并打印结果
    resultRDD.collect().foreach(println)

    // 停止 SparkContext
    sc.stop()
  }
}

