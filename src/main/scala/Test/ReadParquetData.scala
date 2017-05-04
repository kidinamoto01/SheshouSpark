package Test

import java.sql.Date
import java.util.{Calendar, Properties}

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}

/**
  * Created by suyu on 17-4-13.
  */
object ReadParquetData {

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

    val conf = new SparkConf().setAppName("Offline Doc Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val hiveContext = new HiveContext(sc)
    //read json file
    val file =sqlContext.read.parquet(windowsloginpath)//.toDF()
   file.printSchema()
   // println(file.count())

    file.registerTempTable("windowslogin") //where loginresult = 537 or loginresult = 529

    //val result = sqlContext.sql("select * from (select  collecttime,destip,srcip,destequp,loginresult,destcountrycode,srccountrycode,srccountry,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,count(*) as sum from windowslogin group by loginresult, destcountrycode,srccountrycode, collecttime,destip,srcip,destequp,srccountry,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude)t where t.sum > 2 and (t.loginresult = 528 or t.loginresult = 529)")

   val tmp = sqlContext.sql("select  t.year,t.month,t.day,t.hour,t.id,t.attack_time,t.destip as dst_ip, t.srcip as src_ip, t.attack_type, t.srccountrycode as src_country_code, t.srccountry as src_country, t.srccity as src_city,t.destcountrycode as dst_country_code,t.destcountry as dst_country,t.destcity as dst_city , t.srclatitude as src_latitude, t.srclongitude as src_longitude ,t.destlatitude as dst_latitude ,t.destlongitude as dst_longitude ,t.end_time,t.asset_id,t.asset_name,t.alert_level from (select \"0\" as id,loginresult , collecttime as attack_time, destip,srcip,\"forcebreak\" as attack_type ,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collectequpip,collecttime as end_time, count(*) as sum ,0 as asset_id, \"0\" as asset_name,0 as  alert_level,  year,month,day,hour from windowslogin group by year,month,day,hour, loginresult,collecttime,destip,srcip,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collecttime,collectequpip)t where (t.sum > 2 and ( t.loginresult = 539 or t.loginresult = 529 or  t.loginresult = 528 ))")
    tmp.registerTempTable("attacklist")
    hiveContext.sql("insert into sheshou.attack_list partition(year,month,day,hour) select * from attacklist")
   tmp.printSchema()

   //println(file.count())


   // tmp.rdd.foreach(println)
    //println("************"+tmp.count())
    //tmp.write.mode(SaveMode.Append).option("compression","none").save(outputpath+"/"+Year+"/"+Month+"/"+date+"/"+Hour+"/")
    //MySQL connection property

    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "andlinks")


    //val dfWriter = tmp.write.mode("append").option("driver", "com.mysql.jdbc.Driver")

    //dfWriter.jdbc("jdbc:mysql://192.168.1.22:3306/log_info", "attack_list", prop)
  }
}
