package day01.cwq.status

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
  * 有状态
  */
object StatusDemo {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("unstatus").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(3))
    ssc.checkpoint("./ck01")
    val rid: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop201",9999)
    val ds: DStream[(String, Int)] = rid.flatMap(e=>e.split(" ")).map((_,1))
    val ds1: DStream[(String, Int)] = ds.updateStateByKey[Int]((seq:Seq[Int], opt:Option[Int])=>Some(seq.sum + opt.getOrElse(0)))

    ds1.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
