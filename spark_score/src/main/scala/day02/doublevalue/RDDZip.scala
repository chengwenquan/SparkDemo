package day02.doublevalue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RDDZip {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(1, 3, 5,4))
    val rdd2= sc.parallelize(Array(30, 50, 70, 60))

    val rdd4: RDD[(Int, Int)] = rdd1.zip(rdd2)
    rdd4.foreach(println)
    println("--------------------")
    val rdd3: RDD[(Int, Int)] = rdd1.zipPartitions(rdd2)((a,b)=>a.zip(b))
    rdd3.glom().foreach(e => println(e.mkString(",")))
    sc.stop()
  }
}

object RDDZipDemo{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val array3: RDD[Int] = sc.parallelize(Array(1,3,5,7))
    val array4: RDD[Int] = sc.parallelize(Array(2,4,6,8))
    val zip1 = array3.zip(array4)
    val unit= zip1.zipWithIndex()
  }
}
