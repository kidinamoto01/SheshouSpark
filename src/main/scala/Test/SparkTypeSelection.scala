package Test

import java.util.{Calendar, Properties}

import Test.SparkSQLUDF.attack
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by suyu on 17-4-25.
  */
object SparkTypeSelection {

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

    // val windowsloginpath = "hdfs://192.168.1.21:8020/sheshou/data/parquet/windowslogin/2017/4/16/14"

    val conf = new SparkConf().setAppName("Offline Doc Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.udf.register("attack", attack _)
    //read json file
    val file =sqlContext.read.parquet(windowsloginpath)//.toDF()file.printSchema()
   // println(file.count())
    file.printSchema()

    val netstdType = sqlContext.read.csv("hdfs://192.168.1.21:8020/tmp/netstds_type")
    netstdType.registerTempTable("netstdtype")
    netstdType.printSchema()

    val temptable = file.registerTempTable("netstds") //where loginresult = 537 or loginresult = 529

    val data = sqlContext.sql("select _c1 as category ,_c2 as attack_type, _c3 as subcategory from netstdtype")
//(select _c1 as category ,_c2 as attack_type, _c3 as subcategory from netstdtype)t
    val mergedata = sqlContext.sql("select * from netstds left join  netstdtype on netstds.category = netstdtype._c1 and  netstds.subcategory = netstdtype._c3 ")
    mergedata.registerTempTable("result_table")

    mergedata.printSchema()

    val tmp= sqlContext.sql("select \"0\" as id, time as attack_time,  dstIP as dst_ip,srcip as src_ip, _c4 as attack_type, \"0\" as src_country_code, srcLocation as src_country, srcLocation as src_city,\"0\" as dst_country_code,dstLocation as dst_country,dstLocation as dst_city,srclatitude as src_latitude, srclongitude as  src_longitude, dstLatitude as dst_latitude,dstLongitude as dst_longitude, time as  end_time, \"0\" as asset_id,toolName as assent_name,\"0\" as alert_level  from result_table ")

    tmp.printSchema()
    tmp.head(10).map(println)

    // println("************"+tmp.count())
    //tmp.write.mode(SaveMode.Append).save(outputpath+"/"+Year+"/"+Month+"/"+date+"/"+Hour+"/")
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "andlinks")


    //val dfWriter = tmp.write.mode("append").option("driver", "com.mysql.jdbc.Driver")

    // dfWriter.jdbc("jdbc:mysql://192.168.1.22:3306/log_info", "attack_list", prop)

  }
}
