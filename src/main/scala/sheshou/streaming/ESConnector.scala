package sheshou.streaming

import java.net.InetAddress

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.transport.BoundTransportAddress
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.SearchHits
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient




/**
  * Created by suyu on 17-5-5.
  */
object ESConnector {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Offline ES Application").setMaster("local[*]")
    //conf.set("es.index.auto.create", "true")
   // conf.set("es.nodes", "42.123.99.38")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
//("node.client", true)

    val settings = Settings.builder.put("cluster.name", "elasticsearch").put("xpack.security.transport.ssl.enabled", false).
      put("xpack.security.user", "sheshou:sheshou12345").
      put("client.transport.sniff", true).build

    val ipAddr = Array[Byte](42, 123, 99, 38)
   //  val client = new PreBuiltXPackTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByAddress(ipAddr), 9300))


    import org.elasticsearch.client.transport.TransportClient
    import org.elasticsearch.common.transport.InetSocketTransportAddress
    import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient
    import java.net.InetAddress
    val client = new PreBuiltXPackTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("42.123.99.38"), 9300))
   val searchResp = client.prepareSearch("sheshou_info").setTypes("first").setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(QueryBuilders.multiMatchQuery( "_all")) // Query

       //.setPostFilter(QueryBuilders.rangeQuery("pubdate").from("2016-02-12").to("2016-02-15")) // Filter

      // .setFrom(0).setSize(60).setExplain(true)
    .get()


     val hits = searchResp.getHits()

   println(hits.getTotalHits())

    hits.getHits.foreach{
      x=>
        println(x.getScore)
    }

    /*  val nodeName= client.nodeName()
    println(nodeName)*/
   // val indexNam
     client.close()

   }



}
