package Test

import java.util.Calendar

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by suyu on 17-4-12.
  */
object OfflineDocTrandform {
  def main(args: Array[String]) {
    val logFile = "/usr/local/share/spark-2.1.0-bin-hadoop2.6/README.md" // Should be some file on your system
    val filepath = "hdfs://192.168.1.21:8020/tmp/sheshou/parquet/2017/3/14/17"
    val conf = new SparkConf().setAppName("Offline Doc Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //read json file
    val file =sqlContext.read.parquet(filepath)//.toDF()
    file.printSchema()
    println(file.first().get(0).toString+" "+file.first().get(1))
   // println(file.count())
    val cal = Calendar.getInstance()
    val date =cal.get(Calendar.DATE )
    val Year =cal.get(Calendar.YEAR )
    val Month1 =cal.get(Calendar.MONTH )
    val Month = Month1+1
   // println(file.first())
    /*file.write
      .format("com.databricks.spark.csv")
      .mode("append")
      .option("delimiter","\t")
      .save("hdfs://192.168.1.21:8020/tmp/sheshou/hive/"+Year+Month+date)

*/
  }
}
