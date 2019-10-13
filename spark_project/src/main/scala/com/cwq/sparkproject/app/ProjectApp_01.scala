package com.cwq.sparkproject.app

import com.cwq.sparkproject.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 需求一：Top10 热门品类     根据点击、下单、支付的量来统计热门品类.
  *
  * 数据：2019-07-17_31_3e51a9d3-b861-44a0-b6be-5f3c14f2d6a4_7_2019-07-17 00:07:52_null_-1_-1_5,20,8_48,40,95_null_null_8
  *   2019-07-17 ：时间
  *   31：用户id
  *   3e51a9d3-b861-44a0-b6be-5f3c14f2d6a4  :session Id
  *   7:页面id
  *   2019-07-17 00:07:52：事件发生时间点
  *   null：搜索关键词，没有就是null
  *   -1:点击品类id
  *   -1:点击商品id
  *   5,20,8:下单品类ids
  *   48,40,95:下单商品ids
  *   null:支付品类ids
  *   null:支付商品ids
  *   8：城市id
  */
object ProjectApp_01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("p_01").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //从文件中读取数据
    val rdd1: RDD[String] = sc.textFile("D:\\大数据\\上课视频\\20Spark\\02_资料\\user_visit_action.txt")

    //将读出的数据映射成对象(在两个分区中)
    val rdd2: RDD[UserVisitAction] = rdd1.map(x => {
      val splits: Array[String] = x.split("_")
      UserVisitAction(
        splits(0), splits(1),
        splits(2), splits(3),
        splits(4), splits(5),
        splits(6), splits(7),
        splits(8), splits(9),
        splits(10), splits(11),
        splits(12)
      )})

    //需求一：Top10 热门品类     根据点击、下单、支付的量来统计热门品类.
    val top10: List[CategoryCountInfo] = CategoryTop10.categoryTop10Fun(sc,rdd2)
    top10.foreach(println)
    println("--------------------------------------")
    //需求 2: Top10热门品类中每个品类的 Top10 活跃 Session 统计
    //对于排名前 10 的品类，分别获取每个品类点击次数排名前 10 的 sessionId
    //(仍然在两个分区中)
    //方法一：
    println("-------方法一：----------")
    val categoryTop10_1: RDD[(String, List[(String, Int)])] = CategorySessionTopApp.statisticsFun01(top10,rdd2)
    categoryTop10_1.collect().foreach(println)
    println("-------方法二：----------")
    //方法二：
    val categoryTop10_2 = CategorySessionTopApp.statisticsFun02(top10,rdd2)
    categoryTop10_2.foreach{case (cid,arr) => {
      println(cid + ":" + arr.mkString(","))
    }}
    println("-------方法三：----------")
    //方法三：
    val categoryTop10_3= CategorySessionTopApp.statisticsFun03(top10,rdd2)
    categoryTop10_3.collect().foreach(println)

  }
}








