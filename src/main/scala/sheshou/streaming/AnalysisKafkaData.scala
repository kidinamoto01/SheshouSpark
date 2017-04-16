package sheshou.streaming

import java.util.Calendar

import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by suyu on 17-4-13.
  */
object AnalysisKafkaData {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println(s"""se
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
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet).map(_._2)


    // Get the lines
    messages.foreachRDD{ x =>

      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      // val text = sqlContext.read.json(x)
      val text = sqlContext.read.json(x)


      //get json schame
      //text.toDF().printSchema()

      //register temp table
      val temptable = text.registerTempTable("windowslogin")

      if(text.count() > 0){


        val result = sqlContext.sql("select * from (select collectequp,collecttime,statuscode,count(*) as sum from windowslogin group by collectequp, collecttime,statuscode)t where t.sum >2")

        val cal = Calendar.getInstance()
        val date = cal.get(Calendar.DATE)
        val Year = cal.get(Calendar.YEAR)
        val Month1 = cal.get(Calendar.MONTH)
        val Month = Month1+1
        val Hour = cal.get(Calendar.HOUR_OF_DAY)

        result.write.mode(SaveMode.Append).save("hdfs://192.168.1.21:8020/tmp/sheshou/alters/"+topics+"/"+Year+"/"+Month+"/"+date+"/"+Hour+"/")
      }

      //text.write.format("parquet").mode(SaveMode.Append).insertInto("parquet_test")
    }


    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
