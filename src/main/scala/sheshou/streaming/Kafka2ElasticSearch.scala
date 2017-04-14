package sheshou.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark._

/**
  * Created by suyu on 17-4-14.
  */
object Kafka2ElasticSearch {

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |  <spliter_in> is the spliter for input
                            |  <m_length> is the designed length of the message
        """.stripMargin)
      System.exit(1)
    }


    val Array(brokers, topics,spliter_in,m_length) = args
    println(brokers)
    println(topics)
    println(spliter_in)

    println(m_length)
    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("Save2ES").setMaster("local[*]")
    sparkConf.set("es.nodes", "192.168.1.21")
    sparkConf.set("es.port", "9200")
    sparkConf.set("es.index.auto.create", "true")

    val  sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet).map(_._2)

    // Get the lines
    messages.foreachRDD{ x =>



      //get json schame
      //text.toDF().printSchema()

      //save text into elastic search
      x.saveToEs("spark/new")

    }


    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
