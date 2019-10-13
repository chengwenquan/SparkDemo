package day03.doublevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDAggregateByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("group001").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)

    //zeroValue:初始值     seqOp:分区内key计算   combOp:分区间key计算
    //  def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U, combOp: (U, U) => U)
    //找出每个key在各个分区中V的最大值，并将多个分区中的最大值进行累加
    val p1: RDD[(String, Int)] = rdd1.aggregateByKey(Int.MinValue)((max,v) => max.max(v),_+_)
    p1.collect().foreach(println)
    println("------------")
    //找出每个分区的最大值和最小值分别进行累加
    val p2: RDD[(String, (Int, Int))] = rdd1.aggregateByKey((Int.MinValue,Int.MaxValue))((a,v)=>(a._1.max(v),a._2.min(v)),(a,b)=>(a._1 + b._1 , a._2 + b._2))
    p2.collect().foreach(println)

    sc.stop()
  }
}
