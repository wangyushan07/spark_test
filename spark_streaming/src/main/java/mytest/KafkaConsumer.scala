package mytest

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by wang on 2020/12/23.
 */
object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    val kafkaPara: Map[String, Object] = Map[String,Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG->"localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG->"test",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("first"), kafkaPara)
    )
    kafkaDataDS.map(_.value()).print()
//    val adClickData = kafkaDataDS.map(
//      kafkaData=>{
//        val data = kafkaData.value()
//        val datas = data.split(" ")
//        AdClickData(datas(0),datas(1),datas(2),datas(3),datas(4))
//      }
//    )





    ssc.start()
    ssc.awaitTermination()
  }


  case class AdClickData(ts:String,area:String,city:String,user:String,ad:String)

}
