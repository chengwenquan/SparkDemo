package day01.cwq.status

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 无状态
  */
object UnStatusDemo {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("unstatus").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(3))
    val rid: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop201",9999)
    val ds: DStream[(String, Int)] = rid.transform(rdd => {rdd.flatMap(e => e.split(" ")).map((_,1)).reduceByKey(_+_)})
    ds.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
