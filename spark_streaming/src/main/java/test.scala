import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by wang on 2020/12/16.
 */
object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")

    val ssc = new StreamingContext(conf,Seconds(3))

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val value: DStream[(String, Int)] = words.map((_,1))
    val value1: DStream[(String, Int)] = value.reduceByKey(_+_)
    value1.print()

//    ssc.stop()

    ssc.start()
    ssc.awaitTermination()

  }
}
