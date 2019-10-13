package com.cwq.sparkproject.acc

import com.cwq.sparkproject.bean.UserVisitAction
import org.apache.spark.util.AccumulatorV2

/**
  * 需求一的累加器
  */
class MyAcc01 extends AccumulatorV2[UserVisitAction,Map[(String,String),Long]]{

  //存放所有的   (品类id,操作)，统计
  var map: Map[(String, String), Long] = Map[(String,String),Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, Map[(String, String), Long]] = {
    val acc = new MyAcc01
    acc.map ++= map
    acc
  }

  override def reset(): Unit = this.map = Map[(String,String),Long]()

  override def add(v: UserVisitAction): Unit = {
    if(v.click_category_id != "-1"){//说明是点击事件
      //查看该品类是否被统计过，如果统计过则加1，否则添加一条新的记录
      map += (v.click_category_id,"click") -> (map.getOrElse((v.click_category_id,"click"),0L) + 1)
    }
    if(v.order_category_ids != "null"){//说明是下单
      val orders: Array[String] = v.order_category_ids.split(",")
      orders.foreach(x => {
        map += (x,"order") -> (map.getOrElse((x,"order"),0L) + 1)
      })
    }
    if(v.pay_category_ids != "null"){//说明是支付
      val pays: Array[String] = v.pay_category_ids.split(",")
      pays.foreach(x => {
        map += (x,"pay") -> (map.getOrElse((x,"pay"),0L) + 1)
      })
    }
  }

  //合并各个分区中的数据
  override def merge(other: AccumulatorV2[UserVisitAction, Map[(String, String), Long]]): Unit = {
    val o: MyAcc01 = other.asInstanceOf[MyAcc01]
    o.map.foreach(x => {
      this.map += x._1 -> (this.map.getOrElse(x._1,0L) + x._2)
    })
  }

  override def value: Map[(String, String), Long] = this.map
}
