package Test

import java.util.Calendar

import Test.SparkSQLUDF.attack
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by suyu on 17-4-21.
  */
object TransformNetstdsData {

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
    //read json file
    val file =sqlContext.read.parquet(windowsloginpath)//.toDF()file.printSchema()
    println(file.count())
    file.printSchema()
    val temptable = file.registerTempTable("netstds") //where loginresult = 537 or loginresult = 529

    //sqlContext.sql("""create temporary function attack as 'com.andlinks.hive.function.AttackTypeFilterUDF'""")
    sqlContext.udf.register("attack", attack _)
    //val tmp = sqlContext.sql("select t.id,t.attack_time,t.destip as dst_ip, t.srcip as src_ip, t.attack_type, t.srccountrycode as src_country_code, t.srccountry as src_country, t.srccity as src_city,t.destcountrycode as dst_country_code,t.destcountry as dst_country,t.destcity as dst_city , t.srclatitude as src_latitude, t.srclongitude as src_longitude ,t.destlatitude as dst_latitude ,t.destlongitude as dst_longitude ,t.end_time,t.asset_id,t.asset_name,t.alert_level from (select \"0\" as id,loginresult , collecttime as attack_time, destip,srcip,\"forcebreak\" as attack_type ,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collectequpip,collecttime as end_time, count(*) as sum ,\"0\" as asset_id, \"name\" as asset_name,\"0\" as  alert_level from windowslogin group by loginresult,collecttime,destip,srcip,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collecttime,collectequpip)t where (t.sum > 2 and ( t.loginresult = 539 or t.loginresult = 529 or  t.loginresult = 528 ))")
    val tmp= sqlContext.sql("select \"0\" as id, time as attack_time,  dstIP as dst_ip,srcip as src_ip, \"netstds\" as attack_type, \"0\" as src_country_code, srcLocation as src_country, srcLocation as src_city,\"0\" as dst_country_code,dstLocation as dst_country,dstLocation as dst_city,srclatitude as src_latitude, srclongitude as  src_longitude, dstLatitude as dst_latitude,dstLongitude as dst_longitude, time as  end_time, \"0\" as asset_id,toolName as assent_name,\"0\" as alert_level  from netstds ")

    tmp.printSchema()

    tmp.rdd.foreach(println)

    val cal = Calendar.getInstance()
    val date =cal.get(Calendar.DATE )
    val Year =cal.get(Calendar.YEAR )
    val Month1 =cal.get(Calendar.MONTH )
    val Month = Month1+1
    val Hour = cal.get(Calendar.HOUR_OF_DAY)

    //  tmp.rdd.foreach(println)
    // println("************"+tmp.count())
    // tmp.write.mode(SaveMode.Append).save(outputpath+"/"+Year+"/"+Month+"/"+date+"/"+Hour+"/")


  }

}
