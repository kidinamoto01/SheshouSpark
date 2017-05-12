package sheshou.es

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.elasticsearch.spark.sql.EsSparkSQL

/**
  * Created by suyu on 17-5-11.
  */
object ESSpark {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Offline ES Application").setMaster("local[*]")
    conf.set("es.internal.spark.sql.pushdown.strict", "true")
    conf.set("es.index.auto.create", "false")
    conf.set("es.nodes", "42.123.99.38")
    conf.set("es.net.http.auth.user", "sheshou")
    conf.set("es.net.http.auth.pass", "sheshou12345")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    val test =  EsSparkSQL.esDF(sqlContext,"sheshou_info/first", "?q=product_type:Windows")

    test.collect()
    test.printSchema()
   // test.head(10).foreach(println)
    println(test.count())
    test.registerTempTable("sysinfo")
    val osVerDF =  sqlContext.sql("select product_type,vul_type from sysinfo")
    test.toDF().take(100).foreach{
      line=>
        println(line.getString(0))

    }
  }

  }
