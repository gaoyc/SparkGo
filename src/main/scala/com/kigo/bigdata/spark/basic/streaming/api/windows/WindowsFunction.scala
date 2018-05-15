package com.kigo.bigdata.spark.basic.streaming.api.windows

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by kigo on 18-4-21.
  * 使用streaming window函数统计用户不同时间段平均消费金额等指标. 给出的例子计算的是每5秒,每30秒,每1分钟的用户消费金额，消费次数，平均消费。
  */
object WindowsFunction {

  //利用用户消费金额总和计算结果以及用户消费次数统计计算结果计算平均消费金额
  def avgFunction(sum:DStream[(String,Double)],count:DStream[(String,Int)]): DStream[(String,Double)] = {
    val payment = sum.join(count).map(r => {
      val user = r._1
      val sum = r._2._1
      val count = r._2._2
      (user,sum/count)
    })
    payment
  }

  def main (args: Array[String]) {

    def functionToCreateContext(): StreamingContext = {
      val conf = new SparkConf().setAppName("test").setMaster("local[*]")
      val ssc = new StreamingContext(conf, Seconds(5))

      val zkQuorum = "localhost:2181,127.0.0.1:2181"
      val consumerGroupName = "user_payment"
      val kafkaTopic = "user_payment"
      val kafkaThreadNum = 1

      val topicMap = kafkaTopic.split(",").map((_, kafkaThreadNum.toInt)).toMap

      //Kafka数据格式: userName, payment
      // 例如: kigo, 8
      //用户->消费金额
      val user_payment = KafkaUtils.createStream(ssc, zkQuorum, consumerGroupName, topicMap).map(x=>{
        x._2.trim
      })

      //计算每5s每个用户的消费总和
      val paymentSum = user_payment.map(line =>{
        val tp = line.split(",")
        val user = tp(0)
        val payment  = tp(1)
        (user,payment.toDouble)
      }).reduceByKey(_+_)

      //输出结果
      paymentSum.print()

      //计算每5s每个用户的消费次数
      val paymentCount = user_payment.map(line =>{
        val user = line.split(",")(0)
        (user,1)
      }).reduceByKey(_+_)

      //      paymentCount.print()

      //计算每5s每个用户平均的消费金额
      val paymentAvg = avgFunction(paymentSum,paymentCount)
      //      paymentAvg.print()

      //窗口操作，在其中计算不同时间段的结果，入库的话根据使用场景选择吧
      def windowsFunction()  {
        //每5秒计算最后30秒每个用户消费金额
        val windowSum_30 = paymentSum.reduceByKeyAndWindow((a: Double, b: Double) => (a + b),_-_, Seconds(30), Seconds(5))
        //        windowSum_30.print()

        //每5秒计算最后30秒每个用户消费次数
        val windowCount_30 = paymentCount.reduceByKeyAndWindow((a: Int, b: Int) => (a + b),_-_, Seconds(30), Seconds(5))
        //        windowCount_30.print()

        //每5秒计算最后30秒每个用户平均消费
        val windowAvg_30 = avgFunction(windowSum_30,windowCount_30)
        //        windowAvg_30.print()

        //每5秒计算最后60秒每个用户消费金额
        val windowSum_60 = windowSum_30.reduceByKeyAndWindow((a:Double,b:Double)=>(a+b),_-_,Seconds(10),Seconds(5))
        //       windowSum_60.print()

        //每5秒计算最后60秒每个用户消费次数
        val windowCount_60 = windowCount_30.reduceByKeyAndWindow((a:Int,b:Int) => (a+b),_-_,Seconds(10),Seconds(5))
        //        windowCount_60.print()

        //每5秒计算最后60秒每个用户平均消费
        val windowAvg_60 = avgFunction(windowSum_60,windowCount_60)
        //        windowAvg_60.print
      }

      windowsFunction()

      ssc
    }

    val context = StreamingContext.getOrCreate("checkPoint", functionToCreateContext _)

    context.start()
    context.awaitTermination()
  }
}


/* 运行结构示例
//-----------消费总额－－－－－－－
(zhangsan,146.0)
(lisi,79.0)
(wangwu,99.0)
(zhaoliu,115.0)

//-----------消费次数－－－－－－－
(zhangsan,32)
(lisi,18)
(wangwu,30)
(zhaoliu,24)

//-----------平均消费－－－－－－－
(zhangsan,4.5625)
(lisi,4.388888888888889)
(wangwu,3.3)
(zhaoliu,4.791666666666667)
*/
