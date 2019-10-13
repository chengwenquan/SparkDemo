package day02.singlevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 作用: 缩减分区数到指定的数量，用于大数据集过滤后，提高小数据集的执行效率
  *   coalesce 默认是只能减少分区, 而且这个减少不会shuffle
  *   减少
  *
  *   repartition 可以增也可以减, 一定会shuffle
  *   增加
  */
object RDDCoalease {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("Coalease").setMaster("local[3]")
    var sc = new SparkContext(conf)
    var rdd1: RDD[Int] = sc.parallelize(Array(30, 50, 70, 60, 30, 70, 10, 70, 60, 30), 4)
    println(rdd1.getNumPartitions)
    val rdd2 =rdd1.coalesce(6)
    println(rdd2.getNumPartitions)
    val rdd3 = rdd1.repartition(6)
    println(rdd3.getNumPartitions)

    sc.stop()

  }

}
