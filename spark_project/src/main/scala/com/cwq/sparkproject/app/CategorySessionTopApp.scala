package com.cwq.sparkproject.app

import com.cwq.sparkproject.bean.{CategoryCountInfo, CategorySession, UserVisitAction}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * 需求 2: Top10热门品类中每个品类的 Top10 活跃 Session 统计
  * 对于排名前 10 的品类，分别获取每个品类点击次数排名前 10 的 sessionId
  */
object CategorySessionTopApp {

  /*
    思路；
      1.先根据top10的品类id对每个分区中的数据进行过滤
      2.对品类和sessionid组成的key计数
   */
  def statisticsFun01(top10: List[CategoryCountInfo],rdd2: RDD[UserVisitAction]) ={
    //top10的所有品类id
    val top10_list: List[String] = top10.map(_.categoryId)
    //过滤出在top10中所有的点击   (两个分区内分别计算)
    val rdd3: RDD[UserVisitAction] = rdd2.filter(e => top10_list.contains(e.click_category_id))
    //将数据格式化成 [(点击商品的品类Id,sessionId),1]   (两个分区内分别计算)
    val rdd4: RDD[((String, String), Int)] = rdd3.map(e =>((e.click_category_id,e.session_id),1))
    //统计商品品类下的sessionId，并格式化 (g, s), count) => (g, (s, count)
    // reduceByKey把两个分区内的数据汇总到一起，计算完成之后再把计算结果分到两个分区
    val rdd5: RDD[(String, (String, Int))] = rdd4.reduceByKey(_ + _).map({
      case ((g, s), count) => (g, (s, count))
    })
    //对商品品类进行分组
    // groupByKey把两个分区内的数据汇总到一起，计算完成之后再把计算结果分到两个分区
    val rdd6: RDD[(String, Iterable[(String, Int)])] = rdd5.groupByKey()
    //在两个分区内分别执行  缺点：如果迭代器的数据足够大, 当转变成 List 的时候, 会把这个迭代器的所有数据都加载到内存中, 所以有可能造成内存的溢出.
    rdd6.map({
      case (g, it) => {
        (g, it.toList.sortBy({ case (a, b) => b })(Ordering.Int.reverse).take(10))
      }
    })
  }

  def statisticsFun02(top10: List[CategoryCountInfo],rdd2: RDD[UserVisitAction]) ={
    //top10的所有品类id
    val top10_list: List[String] = top10.map(_.categoryId)
    //过滤出在top10中所有的点击   (两个分区内分别计算)
    val rdd3: RDD[UserVisitAction] = rdd2.filter(e => top10_list.contains(e.click_category_id))
    //将数据格式化成 [(点击商品的品类Id,sessionId),1]   (两个分区内分别计算)
    val rdd4: RDD[((String, String), Int)] = rdd3.map(e =>((e.click_category_id,e.session_id),1))
    //统计商品品类下的sessionId，并格式化 (g, s), count) => (g, (s, count)
    // reduceByKey把两个分区内的数据汇总到一起，计算完成之后再把计算结果分到两个分区
    val rdd5: RDD[(String, (String, Int))] = rdd4.reduceByKey(_ + _).map({
      case ((g, s), count) => (g, (s, count))
    })
    //对数据按商品品类进行分组,
    // groupByKey把两个分区内的数据汇总到一起，计算完成之后再把计算结果分到两个分区
    val rdd6: RDD[(String, Iterable[(String, Int)])] = rdd5.groupByKey()

    val tuples: List[(String, Array[(String, Int)])] = top10_list.map(x => { //对每组的数据进行计算
      val value: RDD[(String, Iterable[(String, Int)])] = rdd6.filter(_._1 == x)
      val value1: RDD[(String, Int)] = value.flatMap({ case (cid, it) => it })
      val tuples = value1.sortBy({ case (event, count) => count }, ascending = false).take(10)
      (x, tuples)
    })
    tuples
  }

  def statisticsFun03(top10: List[CategoryCountInfo],rdd2: RDD[UserVisitAction]) ={
    //top10的所有品类id
    val top10_list: List[String] = top10.map(_.categoryId)
    //过滤出在top10中所有的点击   (两个分区内分别计算)
    val rdd3: RDD[UserVisitAction] = rdd2.filter(e => top10_list.contains(e.click_category_id))
    //将数据格式化成 [(点击商品的品类Id,sessionId),1]   (两个分区内分别计算)
    val rdd4: RDD[((String, String), Long)] = rdd3.map(e =>((e.click_category_id,e.session_id),1L))
    //统计商品品类下的sessionId，并格式化 (g, s), count) => (g, (s, count)
    // reduceByKey把两个分区内的数据汇总到一起，计算完成之后再把计算结果分到top10_list.size个分区
    val rdd5= rdd4.reduceByKey(new MyPartitioner(top10_list),_ + _).map({
      case ((g, s), count) =>  CategorySession(g,s,count)
    })

    val resultRdd: RDD[CategorySession] = rdd5.mapPartitions(it => {
      var treeSet: mutable.TreeSet[CategorySession] = new mutable.TreeSet[CategorySession]()
      it.foreach(cs => {
        treeSet += cs
        if (treeSet.size > 10) {
          treeSet = treeSet.take(10)
        }
      })
      treeSet.toIterator
    })
    resultRdd
  }
}

class MyPartitioner(val top10:List[String]) extends Partitioner {
  //直接取对应的下标做为分区
  private val map: Map[String, Int] = top10.zipWithIndex.toMap

  override def numPartitions: Int = top10.size

  //key:(String, String), Int)
  override def getPartition(key: Any): Int = {
    key match {
      case (cid:String,sid) => map(cid)
    }
  }
}