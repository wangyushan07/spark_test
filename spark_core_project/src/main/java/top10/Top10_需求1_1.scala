package top10

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by wang on 2020/12/14.
 */
object Top10_需求1_1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Top10")
    val sc = new SparkContext(conf)
    //读取原始数据
    val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")

    //统计品类点击数量（品类id，点击数量）
    val clickfilterrdd = rdd.filter(action=>{
      val strings: Array[String] = action.split("_")
      strings(6) != "-1"
    })
    val clickCount: RDD[(String, Int)] = clickfilterrdd.map(action => {
      val strings: Array[String] = action.split("_")
      (strings(6), 1)
    }).reduceByKey(_ + _)

    //统计品类下单数量（品类id，下单数量）
    val orderfilterrdd = rdd.filter(action=>{
      val strings: Array[String] = action.split("_")
      strings(8) != "null"
    })
    val orderCount: RDD[(String, Int)] = orderfilterrdd.flatMap(action=>{
      val strings: Array[String] = action.split("_")
      val ids: Array[String] = strings(8).split(",")
      ids.map((_,1))
    }).reduceByKey(_+_)

    //统计品类支付数量（品类id，支付数量）
    val payfilterrdd = rdd.filter(action=>{
      val strings: Array[String] = action.split("_")
      strings(10) != "null"
    })
    val payCount: RDD[(String, Int)] = payfilterrdd.flatMap(action=>{
      val strings: Array[String] = action.split("_")
      val ids: Array[String] = strings(10).split(",")
      ids.map((_,1))
    }).reduceByKey(_+_)

    //将品类进行排序，并且取前十名 点击数量排序、下单数量排序、支付数量排序 （元组排序。先比较第一个，再比较第二个，再比较第三个）
    //（品类id（点击数量，下单数量，支付数量））
    //cogroup = connect + group
    val valuerdd: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickCount.cogroup(orderCount,payCount)
    val mapvaluerdd = valuerdd.mapValues{
      case (clickIter,orderIter,payIter)=>{
        var clickCnt = 0
        var orderCnt = 0
        var payCnt = 0

        if(clickIter.iterator.hasNext){
          clickCnt = clickIter.iterator.next()
        }

        if(orderIter.iterator.hasNext){
          orderCnt = orderIter.iterator.next()
        }

        if(payIter.iterator.hasNext){
          payCnt = payIter.iterator.next()
        }
        (clickCnt,orderCnt,payCnt)
      }
    }

    val resultrdd = mapvaluerdd.sortBy(_._2,false).take(10)

    //打印到控制台
    resultrdd.foreach(println)

    sc.stop()
  }
}
