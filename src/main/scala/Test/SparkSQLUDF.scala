package Test

import java.util.{Calendar, Properties}

import org.apache.spark.sql.types.DataTypes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}

/**
  * Created by suyu on 17-4-21.
  */
object SparkSQLUDF {

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
     println(file.count())
    file.printSchema()
    val temptable = file.registerTempTable("webmiddle") //where loginresult = 537 or loginresult = 529

    //sqlContext.sql("""create temporary function attack as 'com.andlinks.hive.function.AttackTypeFilterUDF'""")
    sqlContext.udf.register("attack", attack _)
    //val tmp = sqlContext.sql("select t.id,t.attack_time,t.destip as dst_ip, t.srcip as src_ip, t.attack_type, t.srccountrycode as src_country_code, t.srccountry as src_country, t.srccity as src_city,t.destcountrycode as dst_country_code,t.destcountry as dst_country,t.destcity as dst_city , t.srclatitude as src_latitude, t.srclongitude as src_longitude ,t.destlatitude as dst_latitude ,t.destlongitude as dst_longitude ,t.end_time,t.asset_id,t.asset_name,t.alert_level from (select \"0\" as id,loginresult , collecttime as attack_time, destip,srcip,\"forcebreak\" as attack_type ,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collectequpip,collecttime as end_time, count(*) as sum ,\"0\" as asset_id, \"name\" as asset_name,\"0\" as  alert_level from windowslogin group by loginresult,collecttime,destip,srcip,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collecttime,collectequpip)t where (t.sum > 2 and ( t.loginresult = 539 or t.loginresult = 529 or  t.loginresult = 528 ))")
    val tmp= sqlContext.sql("select collecttime as attack_time,  \"0\" as dst_ip,srcip as src_ip, attack(requestpage) as attack_type, srccountrycode as src_country_code, srccountry as src_country, srccity as src_city,\"0\" as dst_country_code,\"0\" as dst_country,\"0\" as dst_city,srclatitude as src_latitude, srclongitude as  src_longitude, \"0\" as dst_latitude,\"0\" as dst_longitude, collecttime as  end_time, id as asset_id,\"0\" as assent_name,warnlevel as alert_level  from webmiddle where attack(requestpage) = \"Nothing\" ")

   tmp.printSchema()

   // tmp.rdd.foreach(println)
println(tmp.count())
    val cal = Calendar.getInstance()
    val date =cal.get(Calendar.DATE )
    val Year =cal.get(Calendar.YEAR )
    val Month1 =cal.get(Calendar.MONTH )
    val Month = Month1+1
    val Hour = cal.get(Calendar.HOUR_OF_DAY)

  //  tmp.rdd.foreach(println)
   // println("************"+tmp.count())
   //tmp.write.mode(SaveMode.Append).save(outputpath+"/"+Year+"/"+Month+"/"+date+"/"+Hour+"/")
   val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "andlinks")


    val dfWriter = tmp.write.mode("append").option("driver", "com.mysql.jdbc.Driver")

   // dfWriter.jdbc("jdbc:mysql://192.168.1.22:3306/log_info", "attack_list", prop)

  }
}
