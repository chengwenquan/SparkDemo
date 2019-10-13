package day04

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
  * 读取文件
  */
object TextFileDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("D:\\tmpFile\\a.txt")
    val rdd2 = rdd1.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ +_)
    rdd2.collect().foreach(println)
    rdd2.saveAsTextFile("D:\\tmpFile\\b.txt")
  }
}

/**
  * 读取json文件
  */
object JSONFileDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.textFile("D:\\tmpFile\\people.json")
    val result: RDD[Option[Any]] = rdd1.map(JSON.parseFull);//逐个JSON字符串解析
    result.foreach(x => x match {
      case Some(map:Map[String,Any]) => {
        map.foreach({case (a,b) => println(a+":"+b)})
      }
    })
    sc.stop()
  }
}

object SequenceFileDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)
//    val rdd: RDD[(String, Int)] = sc.parallelize(List("zhang3"->18,"lisi"->19))
//    rdd.saveAsSequenceFile("D:\\tmpFile\\seq")

    val value: RDD[(String, Int)] = sc.sequenceFile[String,Int]("D:\\tmpFile\\seq")
    value.foreach(println)

    sc.stop()
  }
}

object ObjectFileDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)
//    val rdd: RDD[A] = sc.parallelize(List(A(1001,"zhang3"),A(1002,"li4")))
//    rdd.saveAsObjectFile("D:\\tmpFile\\obj")

    val rdd1= sc.objectFile[A]("D:\\tmpFile\\obj")
    rdd1.foreach(println)


  }
}

case class A(val id:Long,val name:String)

/**
  * 从数据库读数据
  */
object JdbcReadDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop100/sparkDB"
    val username = "root"
    val password = "000000"

    val dataSouce = new JdbcRDD(
      sc,
      () => {
        Class.forName(driver)
        DriverManager.getConnection(url, username, password)
      },
      "select id,name,age from student where ? <= id and id <= ? ",
      1001,
      1005,
      2,
      result => {
//        val id = result.getString("id")
//        val name = result.getString("name")
//        val age= result.getString("age")
        val id = result.getString(1)
        val name = result.getString(2)
        val age = result.getString(3)
        (id, name, age)
      }
    )
    dataSouce.foreach(println)
  }
}

/**
  * 向数据库写数据
  */
object JdbcWriteDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop100/sparkDB"
    val username = "root"
    val password = "000000"

    val rdd: RDD[(Int, String, Int)] = sc.parallelize(List((1003,"admin",20),(1004,"root",21),(1005,"jack",22)),2)
    rdd.foreachPartition(it => {
      //创建数据库连接
      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url,username,password)
      val ps: PreparedStatement = connection.prepareStatement("insert into student values (?,?,?)")
      it.foreach({case (id,name,age)=> {
        ps.setInt(1,id)
        ps.setString(2,name)
        ps.setInt(3,age)
        ps.addBatch()
      }})
      ps.executeBatch()
      ps.close()
      connection.close()
    })
    sc.stop()
  }
}


/**
  * 从Hbase中读取数据
  */
object HbaseReadDemo{
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val hbaseConf: Configuration = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum","hadoop200,hadoop201,hadoop202")
    hbaseConf.set(TableInputFormat.INPUT_TABLE,"student")

    val rdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(hbaseConf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])

    rdd.map({case (rowkey,result) => {
      Bytes.toString(rowkey.get()) //rowkey
      val cells: Array[Cell] = result.rawCells()
      for (elem <- cells) {
        val bytes: Array[Byte] = CellUtil.cloneQualifier(elem)
        println(Bytes.toString(bytes))
      }
    }})
  }
}