package day01.cwq.kafkasource

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Streaming Context从kafka中读取数据
  */
object HighApiKafka {

  def getStreamContext() ={
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("kafka")
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.checkpoint("./ck2")  //设置检查点目录

    val group ="bigdata"
    val brokers ="hadoop200:9092,hadoop201:9092,hadoop202:9092"
    val map: Map[String, String] = Map[String, String](
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers
    )
    val set = Set("test")
    val ids: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,map,set)
    ids.print()
    ssc
  }


  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./ck2",getStreamContext)
    ssc.start()
    ssc.awaitTermination()
  }
}
