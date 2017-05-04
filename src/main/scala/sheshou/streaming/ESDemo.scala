package sheshou.streaming

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
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
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "192.168.1.21")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    /*val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

    sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")*/
    val game = Map("media_type"->"game","title" -> "FF VI","year" -> "1994")
    val book = Map("media_type" -> "book","title" -> "Harry Potter","year" -> "2010")
    val cd = Map("media_type" -> "music","title" -> "Surfing With The Alien")

  //  sc.makeRDD(Seq(game, book, cd)).saveToEs("my-collection/{media_type}")

    //sc.esRDD("my-collection/book", "?q=something", Map[String, String]("es.read.field.include"->"title"))

    ////"es.scroll.limit" -> "100000",
   val df = sqlContext.read.format("org.elasticsearch.spark.sql").options(Map( "es.read.field.include" -> "author")).load("test/books")

    println(df.count())
  }
}
