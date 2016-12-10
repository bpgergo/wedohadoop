import java.io.{BufferedWriter, File, FileWriter, StringWriter}
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
import org.apache.spark.streaming.dstream.ForEachDStream

import scala.beans.BeanProperty


object HackatlonStreamingConsumer {
  case class Record(dataset:String,subscriber:String,TAC:String,typez:String,timestamp:String,unix:String,latitude:String,longitude:String,zip:String,sex:String,age:String,magan:String,uzleti:String,magenta_1:String,arpu:String,sim_4g: String)
  case class Location(@BeanProperty val lat: String, @BeanProperty val lon: String)
  case class LonLatMessage(@BeanProperty location: Location, @BeanProperty sex: String, @BeanProperty age: String, @BeanProperty timestamp: String, @BeanProperty count: Long)
  val mapper = new ObjectMapper();
  //def json_printer = (rdd: RDD[(Record, Int)], time: Time) => {
  //  rdd.take(1).foreach(printMe)
  //}



  def jsonMe(record: LonLatMessage) : String = {
    mapper.writeValueAsString(record)
  }

  def sendRequest(message: String) : Unit = {
    val post = new HttpPost("http://asdasd.hu:19200/supa-gergo/sample")
    post.setHeader("Content-type", "application/json")
    post.setEntity(new StringEntity(message))
    //println(post)
    (new DefaultHttpClient).execute(post)
  }




  def main(args: Array[String]) {
    val cal = Calendar.getInstance();
    cal.add(Calendar.HOUR, -1);
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm")
    val conf = new SparkConf().setMaster("local[200]").setAppName("HackatlonStreaming")
    val ssc = new StreamingContext(conf, Seconds(5))
    val lines = ssc.socketTextStream("localhost", 9998)
    val records = lines
      .map(_.split(";"))
      .map(x => Record(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7),x(8), x(9), x(10), x(11), x(12), x(13), x(14), x(15)))
    val wordCounts = records.map(record => (record, 1)).reduceByKey(_ + _)
      .map(x => LonLatMessage(Location(x._1.latitude, x._1.longitude), x._1.sex, x._1.age, format.format(cal.getTime()), x._2))
      .map(x => jsonMe(x))
      //.map(sendRequest)
    def foreachFunc = (rdd: RDD[String], time: Time) => {
      //rdd.foreach(println)


      rdd.foreach(sendRequest)

    }

    wordCounts.foreachRDD(foreachFunc)

    ssc.start()
    ssc.awaitTermination()
  }
}