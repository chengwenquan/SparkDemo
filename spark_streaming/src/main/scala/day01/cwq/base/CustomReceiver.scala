package day01.cwq.base

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 自定义Receiver
  */
object CustomReceiver {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("custom").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))

    val ids: ReceiverInputDStream[String] = ssc.receiverStream(MyReceiver("hadoop201",9999))

    val ds: DStream[(String, Int)] = ids.flatMap(e=>e.split(" ")).map((_,1)).reduceByKey(_+_)
    ds.print(100)

    ssc.start()
    ssc.awaitTermination()

  }
}

/**
  * String：将来DStream中存的数据类型
  * StorageLevel.MEMORY_ONLY:运行级别
  */
case class MyReceiver(host:String,port:Int)extends Receiver[String](StorageLevel.MEMORY_ONLY){

  //接收器启动的时候调用的方法
  //启动一个子线程，循环不断的从数据源接收数据，因为如果时循环，那么就会阻塞主线程
  override def onStart(): Unit = {
    new Thread(){
      override def run(): Unit = receiveData
    }.start()
  }

  //接收器停止时的回调方法
  override def onStop(): Unit = {}

  //从数据源接收数据
  def receiveData(): Unit ={
    try{
      //创建一个socket
      val socket = new Socket(host, port)
      //从socket中读取数据
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, "utf-8"))
      //读取一行数据
      var line = reader.readLine()
      while (line != null) {
        //将数据存放到store中
        store(line)
        //读取下一行
        line = reader.readLine()
      }
      reader.close()//读取结束，关闭流
      socket.close()//关闭socket
    }catch {
      case e :Exception => e.printStackTrace()
    }finally {
      restart("重新连接") //因为要一直读取
    }

  }
}


