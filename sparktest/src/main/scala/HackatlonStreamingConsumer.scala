import java.io.StringWriter
import java.util.{Calendar, Date}

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.http.client.methods.HttpPost
import org.apache.spark.rdd.RDD
import com.fasterxml.jackson._
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient

import scala.beans.BeanProperty


object HackatlonStreamingConsumer {
  case class Record(dataset: String, subscriber: String, TAC: String, tzpe: String, timestamp: String, unix: String,lat: String, val lon: String)
  case class Location(@BeanProperty val lat: String, @BeanProperty val lon: String)
  case class LonLatMessage(@BeanProperty location: Location, @BeanProperty timestamp: String, @BeanProperty count: Long)
  val mapper = new ObjectMapper();
  //def json_printer = (rdd: RDD[(Record, Int)], time: Time) => {
  //  rdd.take(1).foreach(printMe)
  //}

  def jsonMe(record: LonLatMessage) : String = {
    mapper.writeValueAsString(record)
  }

  def sendRequest(message: String) : Unit = {
    val post = new HttpPost("http://asdasd.hu:19200/gero-aggregate/sample")
    post.setHeader("Content-type", "application/json")
    post.setEntity(new StringEntity(message))
    println(post)
    (new DefaultHttpClient).execute(post)
  }

  def main(args: Array[String]) {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm")
    val conf = new SparkConf().setMaster("local[2]").setAppName("HackatlonStreaming")
    val ssc = new StreamingContext(conf, Seconds(60))
    val lines = ssc.socketTextStream("localhost", 9999)
    val records = lines
      .map(_.split(";"))
      .map(x => Record(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7)))
    val wordCounts = records.map(record => (record, 1)).reduceByKey(_ + _)
      .map(x => LonLatMessage(Location(x._1.lat, x._1.lon), format.format(new java.util.Date()), x._2))
      .map(x => jsonMe(x))
      .map(sendRequest)


    ssc.start()
    ssc.awaitTermination()
  }
}