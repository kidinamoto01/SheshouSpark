package sheshou.streaming
import java.util

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util.Calendar
/**
  * Created by suyu on 17-4-13.
  * Function: save kafka data into HDFS
  */
object SaveKafkaData {

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
    val sparkConf = new SparkConf().setAppName("SaveKafkaData").setMaster("local[*]")
    //sparkConf.set("spark.hadoop.parquet.enable.summary-metadata", "true")
    //spark.hadoop.parquet.enable.summary-metadata false
    val  sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))


    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
      "zookeeper.connect" -> "localhost:2181",
      "auto.offset.reset" -> "largest")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet).map(_._2)


    // Get the lines
    messages.foreachRDD{ x =>

      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
     // val text = sqlContext.read.json(x)
      val text = sqlContext.read.json(x)

      //get json schame
      //text.toDF().printSchema()

      //save text into parquet file
      //make sure the RDD is not empty
      if(text.count()>0)
      {
        val cal = Calendar.getInstance()
        val date = cal.get(Calendar.DATE)
        val Year = cal.get(Calendar.YEAR)
        val Month1 = cal.get(Calendar.MONTH)
        val Month = Month1+1
        val Hour = cal.get(Calendar.HOUR_OF_DAY)
        text.write.mode(SaveMode.Append).parquet("hdfs://192.168.1.21:8020/sheshou/parquet/"+Year+"/"+Month1+"/"+date+"/"+Hour+"/")
      }

    }


    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
