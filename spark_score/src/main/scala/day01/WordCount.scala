package day01

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf对象, 并设置 App名字
    val conf: SparkConf = new SparkConf().setAppName("Demo_WordCount").setMaster("local[2]")
    // 2. 创建SparkContext对象
    val sc = new SparkContext(conf)
    //sc.setLogLevel("error")
    // 3. 使用sc创建RDD并执行相应的transformation和action
    val results: Array[(String, Int)] = sc.textFile("D:\\tmpFile\\a.txt")
      .flatMap(_.split("\\W+"))
      .map((_, 1))
      .reduceByKey(_ + _)
      .collect
    results.foreach(println)
    // 4. 关闭连接
    sc.stop()
  }
}
