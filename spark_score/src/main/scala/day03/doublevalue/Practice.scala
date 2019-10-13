package day03.doublevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Practice {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("group001").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("D:\\大数据\\上课视频\\20Spark\\02_资料\\agent.log")

    //将每行的数据进行切割 （省id，广告id）,点击次数
    val p1: RDD[((String, String), Int)] = lines.map(x => {
      val strs: Array[String] = x.split(" ")
      ((strs(1), strs(4)), 1)
    })
    //将（省id，广告id）,点击次数进行聚合   （省id，广告id）,总点击次数
    val p2: RDD[((String, String), Int)] = p1.reduceByKey(_+_)
    //将省id和广告id进行拆分省id，(广告id,总点击次数)
   // val p3: RDD[(String, (String, Int))] = p2.map(x => (x._1._1,(x._1._2,x._2)))
    val p3: RDD[(String, (String, Int))] = p2.map({
     case ((a,b),c) => (a,(b,c))
   })
    //将不同省份的数据分组
    val p4: RDD[(String, Iterable[(String, Int)])] = p3.groupByKey()
    //取每个省份的top3
    val p5: RDD[(String, List[(String, Int)])] = p4.map(x => {
      val list: List[(String, Int)] = x._2.toList
      (x._1, list.sortBy(_._2)(Ordering.Int.reverse).take(3))
    }).sortByKey()
    //输出
    p5.collect().foreach(println)
    sc.stop()
  }
}

/*
  1.	数据结构：时间戳，省份，城市，用户，广告，字段使用空格分割。
 	1516609143867 6 7 64 16
  1516609143869 9 4 75 18
  1516609143869 1 7 87 12

  2.	需求: 统计出每一个省份广告被点击次数的 TOP3
  RDD[(provice,List[(ads,count),(ads,count)])]
  RDD[(provice,(ads,count)),(provice,(ads,count))]
  RDD[((provice,ads),l)]

 */