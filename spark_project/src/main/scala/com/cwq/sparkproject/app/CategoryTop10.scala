package com.cwq.sparkproject.app

import com.cwq.sparkproject.acc.MyAcc01
import com.cwq.sparkproject.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


/**
  * 需求一：Top10 热门品类     根据点击、下单、支付的量来统计热门品类.
  */
object CategoryTop10 {

  def categoryTop10Fun(sc:SparkContext,rdd: RDD[UserVisitAction]) ={
    val acc = new MyAcc01
    sc.register(acc)  //注册累加器
    //将数据传递给累加器，在累加器中进行计算
    rdd.foreach(x => {
      acc.add(x)
    })

    //根据商品品类id分类
    val result_map= acc.value.groupBy(_._1._1)
    // CategoryCountInfo
    val result_list = result_map.map {
      case (cid, map) => {
        new CategoryCountInfo(
          cid,
          map.getOrElse((cid, "click"), 0L),
          map.getOrElse((cid, "order"), 0L),
          map.getOrElse((cid, "pay"), 0L)
        )
      }
    }.toList
    val result: List[CategoryCountInfo] = result_list.sortBy(info => (info.clickCount,info.orderCount,info.payCount))(Ordering.Tuple3(Ordering.Long.reverse,Ordering.Long.reverse,Ordering.Long.reverse)).take(10)
    result
  }

}
