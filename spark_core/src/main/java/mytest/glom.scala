package mytest

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by wang on 2020/12/11.
 */
object glom {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("LOGRDD")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5), 2)
    val rdd1 = rdd.glom()
    val rdd2: RDD[Int] = rdd1.map(data => {
      data.reduce(_ + _)
    })

    rdd2.collect().foreach(println)
    sc.stop()
  }
}
