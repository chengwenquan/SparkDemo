package day02.singlevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 作用: 将每一个分区的元素合并成一个数组，形成新的 RDD 类型是RDD[Array[T]]
  */
object RDDGlom {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDDGlom").setMaster("local[4]")
    val sc= new SparkContext(conf)
    val rdd: RDD[Int] = sc.parallelize(Array(1,3,6,4,8,2,9,7,5,0))
    val array = rdd.glom()
    array.collect().foreach(e => println(e.mkString(",")))
    sc.stop()
  }
}
