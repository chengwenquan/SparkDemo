package day01.cwq.base

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * socketTextStream:socket数据源
  * hadoop201: nc -lk 9999   指令的作用是再服务器开启一个服务端然后再开启一个客户端向9999端口发送信息
  */
object WorldCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("worldcount").setMaster("local[2]")
    //1.创建StreamingContext
    val ssc = new StreamingContext(conf,Seconds(5))
    //2.创建核心数据集
    val rid: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop201",9999)
    //3.对DStream进行操作
    val ds: DStream[(String, Int)] = rid.flatMap(e => e.split(" ")).map((_,1)).reduceByKey(_+_)
    //4.最终数据的处理
    ds.print(100)
    //5.启动Streaming Context
    ssc.start()
    //6.阻止当前线程退出
    ssc.awaitTermination()
  }
}
