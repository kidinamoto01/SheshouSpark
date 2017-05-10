package sheshou.streaming

import java.net.InetAddress
import java.util.Properties

import org.apache.spark.network.client.TransportClient
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._
import org.apache.spark.sql.hive.client
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.spark._

/**
  * Created by suyu on 17-5-4.
  */
object ESDemo {
  def main(args: Array[String]) {
  /*  if (args.length < 2) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <src_path> is a list of one or more Kafka brokers
                            |  <dest_path> is a list of one or more kafka topics to consume from
        """.stripMargin)
      System.exit(1)
    }*/

    val conf = new SparkConf().setAppName("Offline ES Application").setMaster("local[*]")
    conf.set("es.internal.spark.sql.pushdown.strict", "true")
    conf.set("es.index.auto.create", "false")
    conf.set("es.nodes", "42.123.99.38")
    conf.set("es.net.http.auth.user","sheshou")
    conf.set("es.net.http.auth.pass","sheshou12345")
   // conf.set("spark.debug.maxToStringFields",)

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    /*val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

    sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")
    val game = Map("media_type"->"game","title" -> "FF VI","year" -> "1994")
    val book = Map("media_type" -> "book","title" -> "Harry Potter","year" -> "2010")
    val cd = Map("media_type" -> "music","title" -> "Surfing With The Alien")*/

  //  sc.makeRDD(Seq(game, book, cd)).saveToEs("my-collection/{media_type}")

    //sc.esRDD("my-collection/book", "?q=something", Map[String, String]("es.read.field.include"->"title"))

    ////"es.scroll.limit" -> "100000",
   /*val df = sqlContext.read.format("org.elasticsearch.spark.sql").options(Map( "es.read.field.include" -> "author")).load("test/books")

    println(df.count())
    df.rdd.foreach(println)*/

    val elasticIndex = "sheshou_info_test/first"
    val url = "42.123.99.38:9200"
   val reader = sqlContext.read.
      format("org.elasticsearch.spark.sql").
   //  option("es.net.http.auth.user","elastic").
     // option("es.net.http.auth.pass","bbd@2017!ELASTICSEARCH5").
     option("es.net.http.auth.user","sheshou").
    option("es.net.http.auth.pass","sheshou12345").
      option("es.nodes",url).
      option("es.nodes.wan.only","true")

    println(s"Loading: ${url} ...")
    val data = reader.load(elasticIndex)

    val tmpResult =data.filter( data("product_type").like("Cisco"))

   data.printSchema()
  //  data.head(1).foreach(println)
    tmpResult.registerTempTable("esdata")

   /* val schemaRDD = sqlContext.sql("CREATE TEMPORARY view sqlvarcol " +
      "USING org.elasticsearch.spark.sql " +
      "OPTIONS (resource '" + elasticIndex + "')")

    val tmp = sqlContext.sql("select product_type from sqlvarcol where product_type == 'Cisco' ")
    */
    tmpResult.head(100).foreach(println)
    //sqlContext.sql("CREATE TEMPORARY TABLE myIndex    " + "USING org.elasticsearch.spark.sql " + "OPTIONS ( resource 'sheshou_info/first', nodes '42.123.99.38')")
   /*val Settings settings = Settings.builder().put("cluster.name", "elasticsearch").put("xpack.security.user", "sheshou:sheshou12345").put("client.transport.sniff", true).build()

*/


    val test = sc.esRDD(elasticIndex,"?q=product_type:MyBB(1.4.11)")
    test.collect()
    println(test.first()._2)
    //println(test.first()._2.get("info_type").mkString)


  }
}
