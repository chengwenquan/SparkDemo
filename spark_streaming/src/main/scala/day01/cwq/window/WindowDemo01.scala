package day01.cwq.window

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowDemo01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("window").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(3))
    ssc.checkpoint("./wd")
    val ids: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop200",9999)
    ids.flatMap(e =>e.split(" ")).map((_,1)).reduceByKeyAndWindow(_+_,Seconds(9)).print
    //ids.flatMap(e =>e.split(" ")).map((_,1)).reduceByKeyAndWindow((a:Int,b:Int)=>{a+b},Seconds(9),Seconds(6)).print
    //(a:Int,b:Int)=>a+b //批处理后聚合
    //(a:Int,b:Int)=>a-b //批处理后减旧值
//    ids.flatMap(e =>e.split(" ")).map((_,1))
//      .reduceByKeyAndWindow((a:Int,b:Int)=>a+b,(a:Int,b:Int)=>a-b,Seconds(9),Seconds(6),filterFunc = _._2>0 ).print()


    ssc.start()
    ssc.awaitTermination()

  }

}
