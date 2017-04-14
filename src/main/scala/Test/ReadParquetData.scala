package Test

import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by suyu on 17-4-13.
  */
object ReadParquetData {

  def main(args: Array[String]) {
    val logFile = "/usr/local/share/spark-2.1.0-bin-hadoop2.6/README.md" // Should be some file on your system
    val filepath = "/Users/b/Documents/andlinks/sheshou/log/0401log3(1).txt"
    val middlewarepath = "hdfs://192.168.1.21:8020/user/root/test/webmiddle/20170413/web.json"
    val hdfspath = "hdfs://192.168.1.21:8020/user/root/test/windowslogin/20170413/windowslogin"
    val conf = new SparkConf().setAppName("Offline Doc Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //read json file
    val file =sqlContext.read.json(middlewarepath)//.toDF()
    file.printSchema()
   // println(file.count())

    val temptable = file.registerTempTable("windowslogin") //where loginresult = 537 or loginresult = 529

    //val result = sqlContext.sql("select destip,srcip, collecttime , count(*) as sum from windowslogin group by srcip,destip,collecttime")
    val result = sqlContext.sql("select collectequp,collecttime,statuscode,count(*) from windowslogin group by collectequp, collecttime,statuscode")

   //println(result.count())
   result.rdd.foreach(println)

    val cal = Calendar.getInstance()
    val date =cal.get(Calendar.DATE )
    val Year =cal.get(Calendar.YEAR )
    val Month1 =cal.get(Calendar.MONTH )
    val Month = Month1+1
    // println(file.first())
 /*   file.write
      .format("com.databricks.spark.csv")
      .mode("append")
      .option("delimiter","\t")
      .save("hdfs://192.168.1.21:8020/tmp/sheshou/hive/"+Year+Month+date)
*/

  }
}
