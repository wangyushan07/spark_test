package top10

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by wang on 2020/12/14.
 */
//在需求一的基础上，增加每个品类用户 session 的点击统计

object Top10_需求2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Top10")
    val sc = new SparkContext(conf)
    //读取原始数据
    val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    rdd.cache()
    val actionrdd: Array[String] = top10Category(rdd)
    //过滤原始数据,保留点击和前10品类ID
    val filterrdd = rdd.filter(action => {
      val datas = action.split("_")
      if (datas(6) != "-1") {
        actionrdd.contains(datas(6))
      } else {
        false
      }
    })

    //根据品类id和sessionid进行点击量统计
    val maprdd: RDD[((String, String), Int)] = filterrdd.map(
      action => {
        val datas = action.split("_")
        ((datas(6), datas(2)), 1)
      }
    ).reduceByKey(_ + _)

    //结构转换
    val newrdd = maprdd.map(data => {
      (data._1._1, (data._1._2, data._2))
    })

    //相同品类进行分组
    val value: RDD[(String, Iterable[(String, Int)])] = newrdd.groupByKey()
    //    //将分组后的数据进行点击量排序，取前十名
    val resrdd: RDD[(String, List[(String, Int)])] = value.mapValues(iter => {
      iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
    })
    resrdd.collect().foreach(println)
    sc.stop()
  }

  def top10Category(rdd: RDD[String]) = {

    //转换数据结构（品类id，（1,0,0））
    val clickCount = rdd.flatMap(action => {
      val strings: Array[String] = action.split("_")

      if (strings(6) != "-1") {
        List((strings(6), (1, 0, 0)))
      } else if (strings(8) != "null") {
        val orderids: Array[String] = strings(8).split(",")
        orderids.map(id => (id, (0, 1, 0)))
      } else if (strings(10) != "null") {
        val payids: Array[String] = strings(10).split(",")
        payids.map(id => (id, (0, 0, 1)))
      } else {
        Nil
      }
    })

    //只有一个reduceByKey，只有一个shuffer
    val mapvaluerdd = clickCount.reduceByKey((t1, t2) => {
      (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
    })

    val resultrdd = mapvaluerdd.sortBy(_._2, false).take(10)
    //打印到控制台
    resultrdd.map(_._1)
  }

}
