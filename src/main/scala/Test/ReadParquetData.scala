package Test

import java.sql.Date
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}

/**
  * Created by suyu on 17-4-13.
  */
object ReadParquetData {

  case class AttackList(id:Int,attack_time:String,dst_ip :String, src_ip: String, attack_type: String, src_country_code :String,
                        src_country :String, src_city :String, dst_country_code:String, dst_country :String, dst_city :String,
                        src_latitude: Double, src_longitude :Double, dst_latitude :Double, dst_longitude :Double,
                        end_time :String, asset_id :Int, asset_name :String, alert_level :String)

  def main(args: Array[String]) {
    val logFile = "/usr/local/share/spark-2.1.0-bin-hadoop2.6/README.md" // Should be some file on your system
    val filepath = "/Users/b/Documents/andlinks/sheshou/log/0401log3(1).txt"
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

  val tmp = sqlContext.sql("select * from (select loginresult, collecttime,destip,srcip,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collectequpip, count(*) as sum from windowslogin group by loginresult,collecttime,destip,srcip,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collecttime,collectequpip,collectequpip )t where t.sum > 2 and (t.loginresult = 528 or t.loginresult = 529)")

    tmp.printSchema()
   //val result = sqlContext.sql("select * from (select id,collecttime,destip,srcip,\"break\",srccountrycode,,srccity,destcountrycode,srccountry,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collecttime,collectequpip,collectequpip,level,count(*) as sum from windowslogin group by id,collecttime,destip,srcip,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collecttime,collectequpip,level)t where t.sum >2")

    //val result = sqlContext.sql("select * from (select id,collecttime,destip,srcip,srccountrycode, srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude, destlatitude,destlongitude,collecttime,collectequpip,level,loginresult,count(*) as sum from windowslogin group by id,collecttime,destip,srcip,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collecttime,collectequpip,collectequpip,level,loginresult)t where t.sum > 2 and (t.loginresult = 528 or t.loginresult = 529)")

   println(file.count())
    println(tmp.count())
 //result.printSchema()
    tmp.foreach{x=>
      println(x.get(0))
    }

  tmp.rdd.foreach(println)
/*
   val finalRDD =   result.foreach{x=>
     import sqlContext.implicits._

      val tmp = AttackList(System.nanoTime().toInt,//ID
        x.getString(0), //攻击发起时间
        x.getString(1), //destip
        x.getString(2),//srcip
        "break", //attack_type
        x.getString(6), //src_country_code
        x.getString(7),//src_country
        "N/A",//src_city
       x.getString(5), //dst_country_code
        x.getString(8),//dst_country
        "N/A", //dst_city
        x.getString(10).toDouble,//src_latitude
        x.getString(11).toDouble,//src_longitude
        x.getString(12).toDouble,//dst_latitude
        x.getString(13).toDouble,//dst_longitude
        x.getString(0), //end_time
       0,//asset_id
        "N/A",//asset_name
       "N/A")//alert_level
       println(tmp)
    }*/


    val cal = Calendar.getInstance()
    val date =cal.get(Calendar.DATE )
    val Year =cal.get(Calendar.YEAR )
    val Month1 =cal.get(Calendar.MONTH )
    val Month = Month1+1
    val Hour = cal.get(Calendar.HOUR_OF_DAY)

    tmp.write.mode(SaveMode.Append).save("hdfs://192.168.1.21:8020/sheshou/data/parquet/realtime/"+"forcebreak"+"/"+Year+"/"+Month+"/"+date+"/"+Hour+"/")


  }
}
