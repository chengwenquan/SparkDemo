package day03.doublevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDGroupByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("group001").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val list: RDD[(String, Int)] = sc.parallelize(List(("a",1),("b",2),("a",2),("b",3)))
    list.groupByKey().collect().foreach(println)
    sc.stop()

  }
}
