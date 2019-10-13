package review

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WriteFileDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("writefile").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[(Int, String)] = sc.parallelize(Array((1,"zhangsan"),(2,"lisi")))
    rdd1.saveAsTextFile("D:\\tmpFile\\t2.txt")
    sc.stop()
  }

}
