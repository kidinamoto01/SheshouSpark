package sheshou.streaming

import java.sql.Date
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

  case class AttackList(id:Int,attack_time:Date,dst_ip :String, src_ip: String, attack_type: String, src_country_code :String,
                        src_country :String, src_city :String, dst_country_code:String, dst_country :String, dst_city :String,
                        src_latitude: Double, src_longitude :Double, dst_latitude :Double, dst_longitude :Double,
                        end_time :Date, asset_id :Int, asset_name :String, alert_level :String)
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""se
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
    val sparkConf = new SparkConf().setAppName("AnalysisKafkaData").setMaster("local[*]")
    //sparkConf.set("spark.hadoop.parquet.enable.summary-metadata", "true")
    //spark.hadoop.parquet.enable.summary-metadata false
    val  sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(60))

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


       /* val result = sqlContext.sql("select * from " +
          "(select collectequp,collecttime,statuscode,count(*) as sum " +
          "from windowslogin group by collectequp, collecttime,statuscode)t " +
          "where t.sum >2")*/
        val result = sqlContext.sql("select * from (select loginresult, collecttime,destip,srcip,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collectequpip, count(*) as sum from windowslogin group by loginresult,collecttime,destip,srcip,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collecttime,collectequpip,collectequpip )t where t.sum > 2 and (t.loginresult = 528 or t.loginresult = 529)")

        result.rdd.foreach(println)
        /*result.foreach{
          x=>
            val tmp = AttackList(x.get(0),x.get(1),x.get(2),x.get(3))
        }*/
        val cal = Calendar.getInstance()
        val date = cal.get(Calendar.DATE)
        val Year = cal.get(Calendar.YEAR)
        val Month1 = cal.get(Calendar.MONTH)
        val Month = Month1+1
        val Hour = cal.get(Calendar.HOUR_OF_DAY)

        result.write.mode(SaveMode.Append).save("sheshou/data/parquet/realtime/"+"forcebreak"+"/"+Year+"/"+Month+"/"+date+"/"+Hour+"/")
      }

    }


    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
