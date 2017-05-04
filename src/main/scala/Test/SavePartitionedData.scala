package Test

import java.util.Properties

import Test.SparkSQLUDF.attack
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}

/**
  * Created by suyu on 17-4-26.
  */
object SavePartitionedData {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <src_path> is a list of one or more Kafka brokers
                            |  <dest_path> is a list of one or more kafka topics to consume from
        """.stripMargin)
      System.exit(1)
    }


    val Array(windowsloginpath, outputpath) = args
    println(windowsloginpath)
    println(outputpath)

  /* val conf = new SparkConf().setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)*/

    val warehouseLocation = "hdfs://192.168.1.21:9000/user/hive/warehouse"
    val spark = SparkSession.builder().appName("Spark Hive Example").config("spark.master", "local").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = new SQLContext(sc)
    //"spark.master", "local"
    //specify spark datawarehouse
//    "spark.sql.warehouse.dir", warehouseLocation
  //  spark.conf.set("spark.master", "local")
    import spark.sql
   // val netstdType = sqlContext.read.csv("hdfs://192.168.1.21:8020/tmp/netstds_type").toDF("a","b","c","d","e")
    val netstdTest = sqlContext.read.parquet("hdfs://192.168.1.21:8020/apps/hive/warehouse/typetable")

    netstdTest.registerTempTable("net_type")
    sql("SELECT * FROM net_type where  category = \"A\" and subcategory = \"17\"").show()

    netstdTest.printSchema()
  //  netstdType.write.mode(SaveMode.Append).format("parquet").partitionBy("_c0").saveAsTable("typepartitioned")
    //read json file
  /*  val file =sqlContext.read.parquet(windowsloginpath)//.toDF()file.printSchema()
    // println(file.count())
    file.printSchema()

    val temptable = file.registerTempTable("windowslogin") //where loginresult = 537 or loginresult = 529

    val cols = sqlContext.sql("select year(collecttime) as year,month(collecttime) as month ,dayofmonth(collecttime) as day,hour(collecttime) as hour from windowslogin")

   // cols.rdd.foreach(println)


  // val fullDF = file.join(cols)
    //fullDF.printSchema()

    val options = Map("path" -> "/sheshou/test") //.options(options)
    file.write.format("parquet").mode(SaveMode.Append).saveAsTable("default.examples")
    // println(  cols.select("year").collectAsList())
    // println("************"+tmp.count())
   // cols.write.mode(SaveMode.Append).save(outputpath+"/"+cols.select("year").collect()+"/"+cols.select("month")+"/"+cols.select("day")+"/"+cols.select("hour")+"/")
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "andlinks")


    //val dfWriter = tmp.write.mode("append").option("driver", "com.mysql.jdbc.Driver")

    // dfWriter.jdbc("jdbc:mysql://192.168.1.22:3306/log_info", "attack_list", prop)
*/
  }
}
