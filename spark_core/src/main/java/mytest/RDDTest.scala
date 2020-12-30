package mytest

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by wang on 2020/12/11.
 */
object RDDTest {
  //  获取原始数据：时间戳 省份 城市 用户 广告
  //  1516609143867 6 7 64 16
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("LOGRDD")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("datas/agent.log")
    val maprdd = rdd.map(line => {
      val strings = line.split(" ")
      ((strings(1), strings(4)), 1)
    })
    val reduceRdd = maprdd.reduceByKey(_ + _)
    val flatMapRdd = reduceRdd.map(t => {
      (t._1._1, (t._1._2, t._2))
    })
    val groupByRdd = flatMapRdd.groupByKey()

    val mapValueRdd = groupByRdd.mapValues(v => {
      v.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
    })
    mapValueRdd.collect().foreach(println)


  }
}
