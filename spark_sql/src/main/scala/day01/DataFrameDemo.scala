package day01

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object DataFrameBaseDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("dataFrame").master("local[2]").getOrCreate()
    val df =spark.read.json("D:/tmpFile/people.json")

   // SQL 语法风格(主要):临时视图只能在当前 Session 有效, 在新的 Session 中无效.
   //df.createOrReplaceTempView("user")
   // 可以创建全局视图. 访问全局视图需要全路径:如global_temp.xxx
   // df.createGlobalTempView("user")
   //spark.sql("select name,age from user").show()

    //DSL 语法风格(了解)
    //df.select("name","age").show(2)
    import spark.implicits._
    df.select($"name",$"age" + 2).show  //	设计到运算的时候, 每列都必须使用$
  }
}


object RDDDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("rdd").master("local[2]").getOrCreate()
    //获取rdd
    val sc: SparkContext = spark.sparkContext
    val rdd: RDD[String] = sc.textFile("D:/tmpFile/people.txt")

    val rdd1: RDD[(String, Int)] = rdd.map(x => {
      val splits: Array[String] = x.split(",")
      (splits(0), splits(1).toInt)
    })
    import spark.implicits._
    val df: DataFrame = rdd1.toDF("name","age")
    df.show()

    val rdd2: RDD[People] = rdd.map(x => {
      val splits: Array[String] = x.split(",")
      People(splits(0), splits(1).toInt)
    })
    val df1: DataFrame = rdd2.toDF()
    df1.filter(e => e.getAs[Int](1)>20).show()

    val rdd3: RDD[(String, Int)] = sc.parallelize(Array(("lisi", 10), ("zs", 20), ("zhiling", 40)))
    // 映射出来一个 RDD[Row], 因为 DataFrame其实就是 DataSet[Row]
    val rowRdd: RDD[Row] = rdd3.map(x => Row(x._1, x._2))
    // 创建 StructType 类型
    val structType = StructType(Array(StructField("name",StringType),StructField("age",IntegerType)))
    val df2: DataFrame = spark.createDataFrame(rowRdd,structType)
    df2.show()

  }
}

case class People(name:String,age:Int)

object DataFrameDemo{
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("rdd").master("local[2]").getOrCreate()
    import spark.implicits._
    val df: DataFrame = spark.read.json("D:/tmpFile/people.json")
//    val rdd: RDD[Row] = df.rdd
//    rdd.foreach(println)
      val value: Dataset[People] = df.map(e=> People(e.getAs[String](0),e.getAs[Int](1)))
    val ds = df.as[People]
    ds.show()

  }
}

object DataSetDemo{
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("rdd").master("local[2]").getOrCreate()
    import spark.implicits._
    //创建ds
    val ds = List(People("lisi", 20), People("zs", 21)).toDS
    ds.show()
    val ds1: Dataset[(String, Int)] = List(("zhangsan",18),("lisi",20)).toDS()
    ds1.show()
  //Rdd转ds
    val sc: SparkContext = spark.sparkContext
    val peopleRDD: RDD[String] = sc.textFile("examples/src/main/resources/people.txt")
    val rdd_people: RDD[People] = peopleRDD.map(line => {
      val para: Array[String] = line.split(",");
      People(para(0), para(1).toInt)
    })
    val ds2 = rdd_people.toDS()


    val ds3: Dataset[People] = Seq(People("lisi", 40), People("zs", 20)).toDS
    val rdd3: RDD[People] = ds.rdd
    rdd3.foreach(e=>println(e.age))



  }
}


