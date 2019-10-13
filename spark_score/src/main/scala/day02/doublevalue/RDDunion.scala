package day02.doublevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDunion {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(1, 3, 4, 5))
    val rdd2= sc.parallelize(Array(30, 50, 70))
    val value: RDD[Int] = rdd1.union(rdd2)
    value.foreach(println)
    sc.stop()
  }
}
