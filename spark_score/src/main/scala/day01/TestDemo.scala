package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, RangePartitioner, SparkConf, SparkContext}

object TestDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Demo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(1 to 10)
    rdd1.map(x => x*x).collect().foreach(println)
    println("------")
    val unit: RDD[Int] = rdd1.mapPartitions(it => it.map(x => x * x) )
    unit.collect().foreach(println)
    println("------")
    rdd1.mapPartitionsWithIndex((index,context)=>context.map(x => (index,x))).collect().foreach(println)

    println("----------")
    val strlist: RDD[String] = sc.parallelize(List("a b c d","e f g"))
    strlist.flatMap(e => e.split(" ")).collect().foreach(println)

    println("----------")
    val rdd2: RDD[Int] = sc.parallelize(Array(1,3,6,4,8,2,9,7,5,0))
    val unit2: RDD[(Int, Iterable[Int])] = rdd2.groupBy(e=>e%2)
    unit2.collect().foreach(println)

    println("hello hi".contains("llo"))


    val array1: RDD[Int] = sc.parallelize(Array(1,3,5,7))
    val array2: RDD[Int] = sc.parallelize(Array(1,3))
    array1.subtract(array2).collect().foreach(println)
    array1.cartesian(array2).collect().foreach(println)

    println("***************//////")
    val array3: RDD[Int] = sc.parallelize(Array(1,3,5,7))
    val array4: RDD[Int] = sc.parallelize(Array(2,4,6,8))
    val zip1 = array3.zip(array4)
    zip1.collect().foreach(println)
    println("***************//////")
    val zip2 = array3.zipWithIndex()  // 元素和他的索引进行zip
    zip2.collect().foreach(println)
    sc.stop()
  }
}

object TestDemo1{
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Demo").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8),2)
    val rdd2 = rdd1.map((_, null))
    // partitionBy 只能用在键值对形式的RDD上, 所以需要先把需要的RDD转换成RDD[k-v]
    var rdd3 = rdd2.partitionBy(new HashPartitioner(2)).map(_._1)
    rdd3.glom().collect().foreach(e =>println(e.mkString(", ")))
  }
}

object TestDemo2{
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Demo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(("female",1),("male",5),("female",5),("male",2)))
    rdd1.reduceByKey(_ + _).collect().foreach(println)

    //val rdd2 = sc.parallelize(Array("hello", "world", "atguigu", "hello", "are", "go"))
    rdd1.groupByKey().collect().foreach(println)
  }
}

object TestDemo3{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array("hello", "hello", "world", "hello", "atguigu", "hello", "atguigu", "atguigu"))
    val rdd2 = rdd1.map((_, 1))
    val rdd3 = rdd2.combineByKey(
        v => v + 10,
        (last: Int, v:Int) => last + v,
        (v1: Int, v2:Int) => v1 + v2)

    rdd3.collect().foreach(println)
  }
}

object TestDemo4{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(5,2,6,7,3,1,4))
    val rdd1: Array[Int] = rdd.takeOrdered(2)(Ordering.Int.reverse)
    rdd1.foreach(println)
    sc.stop()
  }
}

object TestDemo5{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)
//    val rdd1 = sc.makeRDD(Array(100, 30, 10, 30, 1, 50, 1, 60, 1), 2)
//    rdd1.aggregate(0)(_ + _, _ + _)

    val rdd1 = sc.makeRDD(Array("a", "b", "c", "d"), 2)
    val str: String = rdd1.aggregate("x")(_ + _, _ + _)
    println(str)
    sc.stop()
  }
}

object TestDemo6{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(Array(100, 30, 10, 30, 1, 50, 1, 60, 1), 2)
    val unit: RDD[Array[Int]] = rdd1.glom()
    unit.collect().foreach(x => println(x.mkString(",")))

    println("------------")

    rdd1.foreachPartition(it =>{
      it.foreach(println)
    })
  }
}

object TestDemo7{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(1->"a",2->"b",3->"c",4->"d",5->"e",6->"f",7->"g"), 2)
    val unit: RDD[(Int, String)] = rdd1.partitionBy(new RangePartitioner(2,rdd1))
    unit.glom().collect().foreach(it => println(it.mkString(" , ")))

  }
}


object TestDemo8{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, (String, Int))] = sc.parallelize(List((1001,("A",10)),(1002,("B",10)),(1001,("A",20)),(1002,("B",20))),2)
    println("------rdd-------")
    rdd.mapPartitionsWithIndex((index,it)=>{
      it.map(x => {
        ("分区:"+index,x)
      })
    }).collect().foreach(println)
    println("------------------")
    val rdd1: RDD[(Int, Iterable[(String, Int)])] = rdd.groupByKey()  //分组之前是两个分区，分组之后仍是两个分区
    println("------rdd1-------")
    rdd1.mapPartitionsWithIndex((index,it)=>{
      it.map(x => {
        ("分区:"+index,x)
      })
    }).collect().foreach(println)
    println("------------------")

    val rdd2: RDD[(Int, Iterable[(String, Int)])] = rdd1.filter(_._1==1001) //在各个分区中过滤出数据，分区内没有数据的为空
    println("------rdd2-------")
    println("分区数："+rdd2.getNumPartitions)
    rdd2.mapPartitionsWithIndex((index,it)=>{
      it.map(x => {
        ("分区:"+index,x)
      })
    }).collect().foreach(println)
    println("------------------")


    val rdd3 = rdd2.flatMap { //分区中原有数据是哪些，操作之后数据也是存放在那个分区
      case (_, it) => it
    }

    println("------rdd3-------")
    println("分区数："+rdd3.getNumPartitions)
    rdd3.mapPartitionsWithIndex((index,it)=>{
      it.map(x => {
        ("分区:"+index,x)
      })
    }).collect().foreach(println)
    println("------------------")

    rdd3.foreach(println)

  }
}

object TestDemo9 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.parallelize(List(1,8,2,6,4,9,5,3,7),2)

    println("------rdd-------")
    println("分区数："+rdd.getNumPartitions)
    rdd.mapPartitionsWithIndex((index,it)=>{
      it.map(x => {
        ("分区:"+index,x)
      })
    }).collect().foreach(println)
    println("------------------")

    val rdd1 = rdd.sortBy(x => x)
    println("------rdd1-------")
    println("分区数："+rdd1.getNumPartitions)
    rdd1.mapPartitionsWithIndex((index,it)=>{
      it.map(x => {
        ("分区:"+index,x)
      })
    }).collect().foreach(println)
    println("------------------")



  }
}