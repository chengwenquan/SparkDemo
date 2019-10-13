package day02

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, DataFrameReader, Dataset, SparkSession}

object JDBCReadDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("jdbc").master("local[2]").getOrCreate()
    var url="jdbc:mysql://hadoop100:3306/sparkDB"
    var table ="student1"
//    val properties = new Properties()
//    properties.setProperty("user","root")
//    properties.setProperty("password","000000")
//    val df: DataFrame = spark.read.jdbc(url,table,properties)
//    df.show()

    val df1: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://hadoop100:3306/sparkDB")
      .option("user", "root")
      .option("password", "000000")
      .option("dbtable", table).load()
    df1.show()

  }
}
object JDBCWriteDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("jdbc").master("local[2]").getOrCreate()
    var url="jdbc:mysql://hadoop100:3306/sparkDB"

    import spark.implicits._
    val rdd: RDD[Student] = spark.sparkContext.parallelize(List(Student(1008,"tom2",8)))
    val ds: Dataset[Student] = rdd.toDS()
//    val properties = new Properties()
//    properties.setProperty("user","root")
//    properties.setProperty("password","000000")
//    ds.write.mode("append").jdbc(url,"student1",properties)
    ds.write.format("jdbc")
      .option("url", "jdbc:mysql://hadoop100:3306/sparkDB")
      .option("user", "root")
      .option("password", "000000")
      .option("dbtable", "student1").mode("append")
      .save()
  }
}

case class Student(id:Int,name:String,age:Int)

