package day01.cwq.kafkasource

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Streaming Context从kafka中读取数据
  */
object KafkaDemo {
  def main(args: Array[String]): Unit = {
    //1.创建StreamingContext
    val conf: SparkConf = new SparkConf().setAppName("kafka").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))
    /**
      * @tparam K type of Kafka message key
      * @tparam V type of Kafka message value
      * @tparam KD type of Kafka message key decoder
      * @tparam VD type of Kafka message value decoder
      * ssc: StreamingContext,
      * kafkaParams: Map[String, String],
      * topics: Set[String]
      */
    //对接kafka创建DStream
    val group ="bigdata"
    val brokers ="hadoop200:9092,hadoop201:9092,hadoop202:9092"
    val map: Map[String, String] = Map[String, String](
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers
    )
    val set = Set("test")

    //泛型1、2: kev,vlaue的类型   泛型3、4: keyvalue的解码器
    val ids = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,map,set)
    ids.print

    //StreamingContext启动
    ssc.start()
    //阻止当前线程退出
    ssc.awaitTermination()

  }
}
