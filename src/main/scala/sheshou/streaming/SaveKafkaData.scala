package sheshou.streaming
import java.util

import kafka.serializer.StringDecoder
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util.{Calendar, Properties}
/**
  * Created by suyu on 17-4-13.
  * Function: save kafka data into HDFS
  */
object SaveKafkaData {

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
    val  sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))


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

    //Get Date
    val cal = Calendar.getInstance()
    val date = cal.get(Calendar.DATE)
    val Year = cal.get(Calendar.YEAR)
    val Month1 = cal.get(Calendar.MONTH)
    val Month = Month1+1
    val Hour = cal.get(Calendar.HOUR_OF_DAY)

    // Get the lines
    messages.foreachRDD{ x =>

      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
     // val text = sqlContext.read.json(x)
      val text = sqlContext.read.json(x)

      //get json schame
      text.toDF().printSchema()

      //save text into parquet file
      //make sure the RDD is not empty
      if(text.count()>0)
      {

        println("write")
        //text.write.format("parquet").mode(SaveMode.Append).parquet("hdfs://192.168.1.21:8020/tmp/sheshou/parquet/")

        text.write.format("parquet").mode(SaveMode.Append).parquet("hdfs://192.168.1.21:8020/sheshou/data/parquet/"+"webmiddle"+"/"+Year+"/"+Month+"/"+date+"/"+Hour+"/")
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
        //text.write.format("parquet").mode(SaveMode.Append).parquet("hdfs://192.168.1.21:8020/tmp/sheshou/parquet/")

        text.write.format("parquet").mode(SaveMode.Append).parquet("hdfs://192.168.1.21:8020/sheshou/data/parquet/"+"windowslogin"+"/"+Year+"/"+Month+"/"+date+"/"+Hour+"/")
      }

    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
