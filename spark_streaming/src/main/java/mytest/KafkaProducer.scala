package mytest

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * Created by wang on 2020/12/23.
 */
object KafkaProducer {
  def main(args: Array[String]): Unit = {
    val prop = new Properties()
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String,String](prop)
    while(true){
      createdata().foreach(
        data=>{
          val record = new ProducerRecord[String,String]("first",data)
          producer.send(record)
          println(data)
        }
      )
      Thread.sleep(2000)
    }


  }

  def createdata()={
    val list = ListBuffer[String]()
    val areaList = ListBuffer[String]("华北","华东","华南")
    val cityList: ListBuffer[String] = ListBuffer[String]("北京","上海","深圳")

    for(i <- 1 to new Random().nextInt(50)){
      val area = areaList(new Random().nextInt(3))
      val city = cityList(new Random().nextInt(2))
      val userid = new Random().nextInt(6)+1
      val adid = new Random().nextInt(6)+1

      list.append(s"${System.currentTimeMillis()} ${area} ${city} ${userid} ${adid}")
    }
    list
  }

}
