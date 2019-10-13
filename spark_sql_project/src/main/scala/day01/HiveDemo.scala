package day01

import org.apache.spark.sql.SparkSession

object HiveDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("hive")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    import spark.sql
    sql("show databases").show
  }


}
