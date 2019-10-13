package day02.singlevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 排序
  */
object RDDSort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(30, 50, 70, 60, 10, 20, 1, 3, 4, 5))
    val rdd2 = rdd1.sortBy(x => x, ascending = false)  //ascending = false：倒序  true：升序
    rdd2.collect.foreach(println)
    sc.stop()
  }
}
