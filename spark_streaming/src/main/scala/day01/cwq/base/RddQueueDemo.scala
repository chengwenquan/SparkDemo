package day01.cwq.base

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * Rdd队列
  */
object RddQueueDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("rddqueue").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(4))

    val queue: mutable.Queue[RDD[Int]] = mutable.Queue[RDD[Int]]()
    // oneAtATime true:每个RDD分别处理,
    //            false:4秒内创建的RDD一起处理

    val isd= ssc.queueStream(queue,true)

    val result: DStream[Int] = isd.reduce(_+_)
    result.print

    ssc.start()
    println("################")
    //向队列中写数据
    for (elem <- 1 to 5) {
      println("*********************")
      queue += ssc.sparkContext.parallelize(1 to 10)
      Thread.sleep(1000)
    }
    ssc.awaitTermination()

  }

}
