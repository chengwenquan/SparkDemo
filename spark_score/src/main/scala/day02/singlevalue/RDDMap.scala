package day02.singlevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDMap {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Demo_WordCount").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val list = List(10,20,30,40,50,60)
    val unit: RDD[Int] = sc.parallelize(list)
    println("partitions："+unit.getNumPartitions)
    unit.map(_ * 2).collect.foreach(println)
    sc.stop()
  }
}
