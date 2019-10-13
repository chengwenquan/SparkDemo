package day02.singlevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDSample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(30, 50, 70, 60, 10, 20, 1, 3, 4, 5))
    // 参数1: 是否放回  第二个参数: 抽样的比例
    val rdd2: RDD[Int] = rdd1.sample(true, 0.5)
    rdd2.collect.foreach(println)
    sc.stop()
  }
}
