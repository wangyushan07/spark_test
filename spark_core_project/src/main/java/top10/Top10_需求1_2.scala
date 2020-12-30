package top10

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by wang on 2020/12/14.
 */
//先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数。
/*优化
1.rdd的重复使用
2.cogroup性能可能会较低

*/

object Top10_需求1_2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Top10")
    val sc = new SparkContext(conf)
    //读取原始数据
    val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    rdd.cache()

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


    val newClickCount = clickCount.map(value=>{
      (value._1,(value._2,0,0))
    })

    val newOrderCount = orderCount.map(value=>{
      (value._1,(0,value._2,0))
    })

    val newpayCount = payCount.map(value=>{
      (value._1,(0,0,value._2))
    })

    //union (4,(5961,0,0))
    val valuerdd=newClickCount.union(newOrderCount).union(newpayCount)
    valuerdd.collect().foreach(println)
    println("------------------------")
    val mapvaluerdd = valuerdd.reduceByKey((t1,t2)=>{
      (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
    })

    var resultrdd = mapvaluerdd.sortBy(_._2,false).take(10)

    //打印到控制台
    resultrdd.foreach(println)

    sc.stop()
  }
}
