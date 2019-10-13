package day05

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 累加器
  */
object AccumulatorDemo01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(List(1,2,3,4))
    var a = 0
    println("------在executor中执行和driver端没关系------")
    rdd1.map(x => {
      a+=1
      println(a)
    }).collect()
    println("--------仍会输出a,因为driver端的变量和executor是没关系的--------")
    println(a)
    sc.stop()
  }
}

object AccumulatorDemo02 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(List(1,2,3,4))
    val acc: LongAccumulator = sc.longAccumulator  //设置driver的累加器
    rdd1.map(x => {
      acc.add(1)  //累加
    }).collect()
    println(acc.value)//输出累加器的值
    sc.stop()
  }
}

object AccumulatorDemo03 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(List(1,2,3,4))
    val acc = new MyAcc
    sc.register(acc)
    val rdd2 =rdd1.map(x => {
      acc.add(1)  //累加
    })
    rdd2.collect()
    println(acc.value)//输出累加器的值
    sc.stop()
  }
}



class MyAcc extends AccumulatorV2[Long, Long]{
  var sum = 0L
  //判断初始值
  override def isZero: Boolean = {
    println("isZero")
    sum == 0
  }

  //拷贝对象到executor
  override def copy(): AccumulatorV2[Long, Long] = {
    println("copy")
    val myAcc = new MyAcc
    myAcc.sum = this.sum
    myAcc
  }

  //重置
  override def reset(): Unit = {
    println("reset")
    sum = 0
  }

  //累加
  override def add(v: Long): Unit = {
    println("add")
    sum += v
  }

  //合并，将各个executor中的累加结果进行合并
  override def merge(other: AccumulatorV2[Long, Long]): Unit = {
    println("merge")
    val o: MyAcc = other.asInstanceOf[MyAcc]
    this.sum += o.sum
  }

  //获取累加结果
  override def value: Long = {
    println("value")
    this.sum
  }
}

