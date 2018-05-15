package demo.training

import org.apache.spark.{SparkConf, SparkContext}

object WordCountDemo {
  def main(args: Array[String]) {

    val inputFile = if(args.length > 0) args(0) else "file:///home/kigo/software/spark/spark-1.6.3-bin-hadoop2.6/README.md"
    println(s"@@@@@inputFile=$inputFile")
    val conf = new SparkConf()
    //val sc = new SparkContext("local", "test", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass).toSeq )
    val sc = new SparkContext(conf)
    val line = sc.textFile(inputFile)
    val words = line.flatMap(_.split(" ")).map( word =>{
      println(word)
      (word, 1)
    }).reduceByKey(_+_)
    words.collect().foreach(println)

    if(args.length > 0){
      println(s"@@@@@ save ret to dir=${args(1)}")
      words.saveAsTextFile(args(1))
    }
    sc.stop()
  }
}
