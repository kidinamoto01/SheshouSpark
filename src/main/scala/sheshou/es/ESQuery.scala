package sheshou.es

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.elasticsearch.spark._
/**
  * Created by suyu on 17-5-10.
  */
object ESQuery {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Offline ES Application").setMaster("local[*]")
    conf.set("es.internal.spark.sql.pushdown.strict", "true")
    conf.set("es.index.auto.create", "false")
    conf.set("es.nodes", "42.123.99.38")
    conf.set("es.net.http.auth.user","sheshou")
    conf.set("es.net.http.auth.pass","sheshou12345")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    //mysql connection
    Class.forName("com.mysql.jdbc.Driver")
    //insert into  mysql
    val prop = new Properties()

    val connectionString = "jdbc:mysql://192.168.1.22:3306/nssa_db?user=root&password=andlinks"
   // val connectionString=connectUrl+"user="+userName+"&password"+passWord
    val conn = DriverManager.getConnection(connectionString)
           val elasticIndex = "sheshou_info_test/first"
           val url = "42.123.99.38:9200"

    val verName = "MyBB(1.4.11)"
    val test = sc.esRDD(elasticIndex,"?q=product_type:"+verName)
    test.collect()
    println(test.first()._2)
    //println(test.first()._2.get("info_type").mkString)
           if(test.count()>0){

             println(test.count())
             test.foreach{
               x=>
                 val dateFormatter = new SimpleDateFormat("YYYY-MM-d HH:MM:ss")
                 val now = dateFormatter.format(Calendar.getInstance().getTime)
                 val insertSQL = "Insert into vulnerability_warnings"+" values( \"0\" ,\""+now+"\" , \""+
                   x._2.get("title").mkString+"\" , "+"\"abc\""+" , \""+x._2.get("vul_type").mkString+"\" , \""+x._2.get("score_level").mkString+"\", \"补丁\",0)"

                 println(insertSQL)

                 //conn.createStatement.execute(insertSQL)
             }

  }


  }

}
