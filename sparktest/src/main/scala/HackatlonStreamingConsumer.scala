import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._


object HackatlonStreamingConsumer {
  case class Record(dataset: String, subscriber: String, TAC: String, tzpe: String, timestamp: String, unix: String, latitude: String, longitude: String)

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("HackatlonStreaming")
    val ssc = new StreamingContext(conf, Seconds(3))
    val lines = ssc.socketTextStream("localhost", 9999)
    val records = lines
      .map(_.split(";"))
      .map(x => Record(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7)))
      .map(x => x.longitude + "|" + x.latitude) // data is separated by semicolon

    val wordCounts = records.map(record => (record, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}