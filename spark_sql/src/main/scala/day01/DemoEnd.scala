package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object DemoEnd {
  def main(args: Array[String]): Unit = {

    //初始化SparkSession
    val spark: SparkSession = SparkSession.builder().master("local[2]").appName("demoend").getOrCreate()
    val rdd: RDD[(String, Int, String)] = spark.sparkContext.parallelize(Array(("lisi", 20, "male"), ("ww", 18, "female")))
    import spark.implicits._
    val dataFrame: DataFrame = rdd.toDF("name","age","sex")

    val ds: Dataset[Row] = dataFrame.select("name","age").filter($"age">18)
    println("------------------------")
    val ds1: Dataset[User] = ds.as[User]
    ds1.select("name").show()
    //ds1.foreach(e => println(e.name))
  }
}
case class User(val name:String,val age:Int)

object ReadJsonDemo{
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[2]").appName("json").getOrCreate()
    import spark.implicits._
//    val df: DataFrame = spark.read.json("D:\\tmpFile\\people.json")
//    df.show()

    val frame: DataFrame = spark.read.format("json").load("D:\\tmpFile\\people.json")
    frame.show()
  }
}

object ReadTxtDemo{
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[2]").appName("json").getOrCreate()
    import spark.implicits._
    val df: DataFrame = spark.read.text("D:\\tmpFile\\people.txt")
    val unit: Dataset[String] = df.map(e => {
      val strings: Array[String] = e.getString(0).split(",")
      strings(0)
    })
    unit.toDF("name").show()
  }
}

object ReadCsvDemo{
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[2]").appName("json").getOrCreate()
    import spark.implicits._
    val dataFrame: DataFrame = spark.read.csv("D:\\tmpFile\\people.csv")
    dataFrame.foreachPartition(
      it =>{
        it.foreach(e => println(e.getString(0) + "---" +e.getString(1)))
      }
    )
  }
}