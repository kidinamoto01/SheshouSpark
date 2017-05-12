package sheshou.streaming
import java.net.InetAddress
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util

import kafka.serializer.StringDecoder
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util.{Calendar, Properties}

import org.apache.spark.sql.hive.HiveContext
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql.EsSparkSQL

/**
  * Created by suyu on 17-4-13.
  * Function: save kafka data into HDFS
  */
object SaveKafkaData {

  def attack(requetspage : String): String = {
    var result = "Nothing"
    //命令执行漏洞
    val commands=List( "|id", "|cat /etc/passwd#","'|'ld", "\"|\"ld","';cat /etc/passwd;'","\";cat /etc/passwd;\""
      , "||cat /etc/passwd", "'&dir&'", "\"&dir&\"", "|ping -n 20 127.0.0.1||x", "`ping -c 20 127.0.0.1`", "&ping -n 20 127.0.0.1&","|ping -c 20 127.0.0.1||x")

    commands.map{
      x=>
        if ( requetspage.contains(x))
          result =  "Web层攻击-命令执行漏洞"
    }

    //文件包含漏洞
    val packagefile = List("\\..\\..\\..\\..\\..\\..\\..\\..","/../../../../../../","\\\\..\\\\..\\\\..\\\\..\\\\..\\\\..\\\\..\\\\..\\\\","/\\..\\..\\..\\..\\", "/????/????/????/????/????/????/????/????",
      "/..??..??..??..??..??..","/..?..?..?..?..?..?..?.","./././././././././././././././.","boot.ini","win.ini", "/windows/win.ini",
      "/etc/passwd", "/WEB-INF/web.xml", "//../....//....//WEB-INF/web.xml")

    packagefile.map{
      x=>
        if ( requetspage.contains(x))
          result = "Web层攻击-文件包含漏洞"
    }

    //路径扫描
    val pathfile = List(".mdb","cmd.exe","fuck.php", "fck_select.html", "cao.php", "inc.asp.bak","ydxzdate.asa"
      ,"1.asp.asa","a.asp;(1).jpg","360.aspx", "6qv4myup.aspx","DataShop).aspx","admin_.aspx", "CEO.jsp","admin/go.jsp"
      , "install/install.jsp","readme.txt")

    pathfile.map{
      x=>
        if ( requetspage.contains(x))
          result = "Web层攻击-路径扫描"
    }

    //SQL注入攻击
    val sqlFile = List("user()","\"", "\'", "--", "/*", "/*!", "';", "-1' or", "-1 or",
      "order by", "b/**/y", "/*!by*/","IF(SUBSTR(@@version", "and sleep(", "' and sleep(","waitfor delay '",
      "@@version", "having 1=1--", "' and", "' || '' || '","' exec master..xp_cmdshell 'vol'--")

    sqlFile.map{
      x=>
        if ( requetspage.contains(x))
          result = "Web层攻击-SQL注入攻击"
    }

    //struts2漏洞
    val strutsFile = List()
    strutsFile.map{
      x=>
        if ( requetspage.contains(x))
          result = "Web层攻击-struts2漏洞"
    }

    //模糊测试
    val vagueFile = List(")))))))))))")
    vagueFile.map{
      x=>
        if ( requetspage.contains(x))
          result = "Web层攻击-模糊测试"
    }

    //XSS跨站脚本攻击
    val xssFile = List("\">", "\"><script>", "</textArea><script>", ";//", "alert%281%29","alert(1)", "alert(", "confirm("
      , "confirm%281%29", "prompt(","prompt%281%29", "onMouseOver","document.location", "document%2elocation%3d1", "document.title="
      ,"document%2etitle%3d1","<a>","<script>alert(1)</script>", "<?xml version=\"1.0\" encoding=\"utf-8\"?>", "<xxx>"
      , "--><")

    xssFile.map{
      x=>
        if ( requetspage.contains(x))
          result = "Web层攻击-XSS跨站脚本攻击"
    }

    return result
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
        """.stripMargin)
      System.exit(1)
    }


    val Array(brokers, topics) = args
    println(brokers)
    println(topics)

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("SaveKafkaData").setMaster("local[*]")
    //sparkConf.set("spark.hadoop.parquet.enable.summary-metadata", "true")
    //spark.hadoop.parquet.enable.summary-metadata false
    sparkConf.set("es.internal.spark.sql.pushdown.strict", "true")
    sparkConf.set("es.index.auto.create", "false")
    sparkConf.set("es.nodes", "42.123.99.38")
    sparkConf.set("es.net.http.auth.user","sheshou")
    sparkConf.set("es.net.http.auth.pass","sheshou12345")
    val  sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))


    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
      "zookeeper.connect" -> "localhost:2181")

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams,  Set("webmiddle")).map(_._2)

    val messages2 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set("netstdsonline")).map(_._2)

    val messages3 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set("windowslogin")).map(_._2)

    //system info
    val messgesysinfo =  KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams,  Set("sysinfo")).map(_._2)

    //Get Date
    val cal = Calendar.getInstance()
    val date = cal.get(Calendar.DATE)
    val Year = cal.get(Calendar.YEAR)
    val Month1 = cal.get(Calendar.MONTH)
    val Month = Month1+1
    val Hour = cal.get(Calendar.HOUR_OF_DAY)

    // Get the lines
    messages.foreachRDD{ x =>

      val sqlContext = new  HiveContext(sc)
     // val text = sqlContext.read.json(x)
      val text = sqlContext.read.json(x)

      //get json schame
      text.toDF().printSchema()

      //save text into parquet file
      //make sure the RDD is not empty
      if(text.count()>0)
      {

        println("write")

        //text.write.format("parquet").mode(SaveMode.Append).parquet("hdfs://192.168.1.21:8020/sheshou/data/parquet/"+"webmiddle"+"/"+Year+"/"+Month+"/"+date+"/"+Hour+"/")
        sqlContext.udf.register("attack", attack _)
        //val tmp = sqlContext.sql("select t.id,t.attack_time,t.destip as dst_ip, t.srcip as src_ip, t.attack_type, t.srccountrycode as src_country_code, t.srccountry as src_country, t.srccity as src_city,t.destcountrycode as dst_country_code,t.destcountry as dst_country,t.destcity as dst_city , t.srclatitude as src_latitude, t.srclongitude as src_longitude ,t.destlatitude as dst_latitude ,t.destlongitude as dst_longitude ,t.end_time,t.asset_id,t.asset_name,t.alert_level from (select \"0\" as id,loginresult , collecttime as attack_time, destip,srcip,\"forcebreak\" as attack_type ,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collectequpip,collecttime as end_time, count(*) as sum ,\"0\" as asset_id, \"name\" as asset_name,\"0\" as  alert_level from windowslogin group by loginresult,collecttime,destip,srcip,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collecttime,collectequpip)t where (t.sum > 2 and ( t.loginresult = 539 or t.loginresult = 529 or  t.loginresult = 528 ))")
        val tmp= sqlContext.sql("select year,month,day,hour,collecttime as attack_time,  \"0\" as dst_ip,srcip as src_ip, attack(requestpage) as attack_type, srccountrycode as src_country_code, srccountry as src_country, srccity as src_city,\"0\" as dst_country_code,\"0\" as dst_country,\"0\" as dst_city,srclatitude as src_latitude, srclongitude as  src_longitude, \"0\" as dst_latitude,\"0\" as dst_longitude, collecttime as  end_time, id as asset_id,\"0\" as assent_name,warnlevel as alert_level  from webmiddle where attack(requestpage) != \"Nothing\" ")

        tmp.printSchema()
        tmp.registerTempTable("webmiddle")
        sqlContext.sql("insert into sheshou.attack_list partition(year,month,day,hour) select * from webmiddle")
      }

    }

    // Get the lines
    messages2.foreachRDD{ x =>

      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      // val text = sqlContext.read.json(x)
      val text = sqlContext.read.json(x)

      //get json schame
      text.toDF().printSchema()

      //save text into parquet file
      //make sure the RDD is not empty
      if(text.count()>0)
      {

        println("write2")
        //text.write.format("parquet").mode(SaveMode.Append).parquet("hdfs://192.168.1.21:8020/tmp/sheshou/parquet/")
        val temptable = text.registerTempTable("netstds")
       // val tmp = sqlContext.sql("select \"0\" as id, time as attack_time,  dstIP as dst_ip,srcip as src_ip, \"netstds\" as attack_type, \"0\" as src_country_code, srcLocation as src_country, srcLocation as src_city,\"0\" as dst_country_code,dstLocation as dst_country,dstLocation as dst_city,srclatitude as src_latitude, srclongitude as  src_longitude, dstLatitude as dst_latitude,dstLongitude as dst_longitude, time as  end_time, \"0\" as asset_id,toolName as assent_name,\"0\" as alert_level from netstds ")

        val tmp = sqlContext.sql("select attack_time, dst_ip,src_ip,attack_type,src_country_code,src_country,src_city,dst_country_code,dst_country, dst_city,src_latitude,  src_longitude,dst_latitude, dst_longitude,  end_time, asset_id, asset_name, alert_level from netstds ")
       // text.write.format("parquet").mode(SaveMode.Append).parquet("hdfs://192.168.1.21:8020/sheshou/data/parquet/"+"realtime/breaks"+"/"+Year+"/"+Month+"/"+date+"/"+Hour+"/")
        //MySQL connection property
        val prop = new Properties()
        prop.setProperty("user", "root")
        prop.setProperty("password", "andlinks")


        val dfWriter = tmp.write.mode("append").option("driver", "com.mysql.jdbc.Driver")

        dfWriter.jdbc("jdbc:mysql://192.168.1.22:3306/log_info", "attack_list", prop)
        println("mysql")

      }

    }


    // Get the lines
    //windowslogin
    messages3.foreachRDD{ x =>

      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      // val text = sqlContext.read.json(x)
      val text = sqlContext.read.json(x)

      //get json schame
      text.toDF().printSchema()

      //save text into parquet file
      //make sure the RDD is not empty
      if(text.count()>0)
      {

        println("write3")
        text.registerTempTable("windowlogin")
        //text.write.format("parquet").mode(SaveMode.Append).parquet("hdfs://192.168.1.21:8020/tmp/sheshou/parquet/")
        val result = sqlContext.sql("select t.id,t.attack_time,t.destip as dst_ip, t.srcip as src_ip, t.attack_type, t.srccountrycode as src_country_code, t.srccountry as src_country, t.srccity as src_city,t.destcountrycode as dst_country_code,t.destcountry as dst_country,t.destcity as dst_city , t.srclatitude as src_latitude, t.srclongitude as src_longitude ,t.destlatitude as dst_latitude ,t.destlongitude as dst_longitude ,t.end_time,t.asset_id,t.asset_name,t.alert_level from (select \"0\" as id,loginresult , collecttime as attack_time, destip,srcip,\"forcebreak\" as attack_type ,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collectequpip,collecttime as end_time, count(*) as sum ,\"0\" as asset_id, \"name\" as asset_name,\"0\" as  alert_level from windowslogin group by loginresult,collecttime,destip,srcip,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collecttime,collectequpip)t where t.sum > 2")

        result.write.format("parquet").mode(SaveMode.Append).parquet("hdfs://192.168.1.21:8020/sheshou/data/parquet/"+"attacklist"+"/"+Year+"/"+Month+"/"+date+"/"+Hour+"/")
      }

    }


    //messages from topic report sysinfo
    messgesysinfo.foreachRDD { x =>
      //read json data
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
     // val sql = new org.apache.spark.sql.SQLContext(sc)
      val text = sqlContext.read.json(x)
      text.printSchema()
      text.registerTempTable("sysinfo")

      //mysql connection
      Class.forName("com.mysql.jdbc.Driver")
      //insert into  mysql
      val prop = new Properties()
      //  val userName =  SheShouStreamingConf.getString(Constants.MYSQL_USER)
      //  val passWord =  SheShouStreamingConf.getString(Constants.MYSQL_PASSWORD)
      // val connectUrl = SheShouStreamingConf.getString(Constants.MYSQLCONNECT_URL2)
      val connectionString = "jdbc:mysql://192.168.1.22:3306/nssa_db?user=root&password=andlinks"
      //val connectionString=connectUrl+"user="+userName+"&password"+passWord


      val elasticIndex = "sheshou_info_test/first"
      if (text.count() > 0) {
        text.toDF().select("osname").show()
        val namesDF = text.toDF().select("osname")
        namesDF.collectAsList().get(1)
        namesDF.foreach{
          x=>
            val content = x.getString(0)
            val test = ssc.sparkContext.esRDD(elasticIndex,"?q=product_type:"+content.mkString)
        }
        //val test = sc.esRDD(elasticIndex,"?q=product_type:"+text.toDF().select("osname").head().mkString)
        //test.first()
      }



    //  val test = sc.esRDD(elasticIndex,"?q=product_type:"+text.toDF().head().getString(1))
//      if (text.count() > 0) {
//        text.foreach{
//          x=>
//
//            val tmp  = EsSparkSQL.esDF(sqlContext, "sheshou_info/first", "?q=product_type:MyBB(1.4.11)")
//
//            val esConfig:Map[String,String] = Map(+ x.getString(17)
//              "es.nodes" -> "42.123.99.38"
//              ,"es.port" -> "9200"
//              ,"es.net.http.auth.user"->"sheshou"
//              ,"es.net.http.auth.pass"->"sheshou12345"
//            )
//
//
//        }

        //
//        val osVerDF = sqlContext.sql("select osname,id from sysinfo")
//
//        osVerDF.foreach{
//          x=>
//
//            val tmpRDD = EsSparkSQL.esDF(sqlContext, "sheshou_info/first", "?q=product_type:MyBB(1.4.11)")
//
//            tmpRDD.printSchema()
//
//        }


      //}
      //get json schame

      //save text into parquet file
      //make sure the RDD is not empty
//      if (text.count() > 0) {
//        //insert into  attack_list@mysql
//        sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
//        text.registerTempTable("sysinfo")
//        text.printSchema()
//
//        val osVerDF = sqlContext.sql("select osname,id from sysinfo")
//
//        osVerDF.foreachPartition{
//
//             part=>
//               val conn = DriverManager.getConnection(connectionString)
//                 part.foreach{
//                   line =>
//                     val osname = line.getString(0)
//
//                      //println(test.first()._2.get("info_type").mkString)
//                      if (test.count() > 0) {
//
//                        test.foreach {
//                          x =>
//                            val dateFormatter = new SimpleDateFormat("YYYY-MM-d HH:MM:ss")
//                            val now = dateFormatter.format(Calendar.getInstance().getTime)
//                            //val insertSQL = "Insert into vulnerability_warnings" + " values( \"0\" ,\"" + now + "\" , \"" + x._2.get("title").mkString + "\" , " + "\"" + line.getString(1) + "\"" + " , \"" + x._2.get("vul_type").mkString + "\" , \"" + x._2.get("score_level").mkString + "\", \"补丁\",0)"
//                            val insertSQL = "Insert into vulnerability_warnings" + " values( \"0\" ,\"" + now + "\" , \"" + x.getString(0) + "\" , " + "\"" + line.getString(1) + "\"" + " , \"" + x.getString(0) + "\" , \"" + x.getString(1)+ "\", \"补丁\",0)"
//
//                            println(insertSQL)
//                            conn.createStatement.execute(insertSQL)
//                        }
//
//
//                      }
//            }
//        }
//      }
    }
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
