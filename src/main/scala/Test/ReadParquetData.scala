package Test

import java.sql.Date
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}

/**
  * Created by suyu on 17-4-13.
  */
object ReadParquetData {

  def main(args: Array[String]) {

    val windowsloginpath = "hdfs://192.168.1.21:8020/sheshou/data/parquet/windowslogin/2017/4/16/14"
    val middlewarepath = "hdfs://192.168.1.21:8020/user/root/test/webmiddle/20170413/web.json"
    val hdfspath = "hdfs://192.168.1.21:8020/user/root/test/windowslogin/20170413/windowslogin"
    val conf = new SparkConf().setAppName("Offline Doc Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //read json file
    val file =sqlContext.read.parquet(windowsloginpath)//.toDF()
   // file.printSchema()
   // println(file.count())

    val temptable = file.registerTempTable("windowslogin") //where loginresult = 537 or loginresult = 529

    //val result = sqlContext.sql("select * from (select  collecttime,destip,srcip,destequp,loginresult,destcountrycode,srccountrycode,srccountry,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,count(*) as sum from windowslogin group by loginresult, destcountrycode,srccountrycode, collecttime,destip,srcip,destequp,srccountry,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude)t where t.sum > 2 and (t.loginresult = 528 or t.loginresult = 529)")

   val tmp = sqlContext.sql("select t.id,t.attack_time,t.destip, t.srcip, t.attack_type, t.srccountrycode, t.srccountry, t.srccity,t.destcountrycode,t.destcountry,t.destcity, t.srclatitude, t.srclongitude ,t.destlatitude,t.destlongitude,t.end_time,t.asset_id,t.asset_name,t.alert_level from (select \"0\" as id,loginresult , collecttime as attack_time, destip,srcip,\"forcebreak\" as attack_type ,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collectequpip,collecttime as end_time, count(*) as sum ,\"0\" as asset_id, \"name\" as asset_name,\"0\" as  alert_level from windowslogin group by loginresult,collecttime,destip,srcip,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collecttime,collectequpip)t where t.sum > 2")

    tmp.printSchema()
   //val result = sqlContext.sql("select * from (select id,collecttime,destip,srcip,\"break\",srccountrycode,,srccity,destcountrycode,srccountry,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collecttime,collectequpip,collectequpip,level,count(*) as sum from windowslogin group by id,collecttime,destip,srcip,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collecttime,collectequpip,level)t where t.sum >2")

    //val result = sqlContext.sql("select * from (select id,collecttime,destip,srcip,srccountrycode, srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude, destlatitude,destlongitude,collecttime,collectequpip,level,loginresult,count(*) as sum from windowslogin group by id,collecttime,destip,srcip,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collecttime,collectequpip,collectequpip,level,loginresult)t where t.sum > 2 and (t.loginresult = 528 or t.loginresult = 529)")

   //println(file.count())
   // println(tmp.count())

  tmp.rdd.foreach(println)

    val cal = Calendar.getInstance()
    val date =cal.get(Calendar.DATE )
    val Year =cal.get(Calendar.YEAR )
    val Month1 =cal.get(Calendar.MONTH )
    val Month = Month1+1
    val Hour = cal.get(Calendar.HOUR_OF_DAY)

    //tmp.write.mode(SaveMode.Overwrite).save("hdfs://192.168.1.21:8020/sheshou/data/parquet/realtime/"+"break"+"/"+Year+"/"+Month+"/"+date+"/"+Hour+"/")


  }
}
