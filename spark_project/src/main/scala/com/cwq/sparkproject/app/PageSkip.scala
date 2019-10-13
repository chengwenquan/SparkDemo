package com.cwq.sparkproject.app

import com.cwq.sparkproject.bean.UserVisitAction
import org.apache.spark.rdd.RDD

/**
  * 计算页面跳转转化率
  */
object PageSkip {
  def main(args: Array[String]): Unit = {
    var page = "1,2,3,4,5,6,7"
    val splits: Array[String] = page.split(",")
    val head: Array[String] = splits.take(splits.length-1)
    val tail: Array[String] = splits.tail

    val tuples: Array[(String, Any)] = head.zip(tail)
    tuples.foreach(println)
  }
  //"1,2,3,4,5,6,7"
  def pageSkipFun(rdd: RDD[UserVisitAction],page:String) ={
    val splits: Array[String] = page.split(",")
    val head: Array[String] = splits.take(splits.length-1)
    val tail: Array[String] = splits.tail

    val tuples: Array[(String, Any)] = head.zip(tail)
    tuples.foreach(println)

  }
}



















