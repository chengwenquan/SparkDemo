package day01

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object CustomUDFDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("udf").master("local[2]").getOrCreate()
    val df: DataFrame = spark.read.json("D:/tmpFile/people.json")
    spark.udf.register("MyUpper",(s:String) => s.toUpperCase)
    df.createTempView("user")
    import spark.sql
    spark.sql("select MyUpper(name) name ,age from  user").show()
    //因为导入了spark.sql所以能把spark简化成如下代码
    sql("select MyUpper(name) name ,age from  user").show()
  }
}


object CustomUDFDemo01 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("udf").master("local[2]").getOrCreate()
    val df: DataFrame = spark.read.json("D:/tmpFile/people.json")
    spark.udf.register("MySum",new MySum)
    df.createTempView("user")
    import spark.sql
    sql("select MySum(age) from  user").show()
  }
}

/**
  * 自定义的udf函数，实现求和的功能5
  */
class MySum extends UserDefinedAggregateFunction{

  //函数的输入类型
  override def inputSchema: StructType = StructType(Array(StructField("age",IntegerType)))

  //缓存的类型（计算结果的类型）
  override def bufferSchema: StructType = StructType(Array(StructField("sum",IntegerType)))

  //最终返回值的类型
  override def dataType: DataType = IntegerType

  // 确定性: 相同输入是否应该返回相同的输出
  override def deterministic: Boolean = true

  //初始化：定义缓存区零值
  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0)=0

  //分区中聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = buffer(0) = buffer.getInt(0) + input.getInt(0)

  //分区间聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = buffer1(0)=buffer1.getInt(0)+buffer2.getInt(0)

  //最终返回值
  override def evaluate(buffer: Row): Any = buffer.getInt(0)
}