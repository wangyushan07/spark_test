package mytest

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by wang on 2020/12/11.
 */
object testPartionIndex {
  def main(args: Array[String]): Unit = {
    val partiontest = new SparkConf().setMaster("local[*]").setAppName("partiontest")
    val sc = new SparkContext(partiontest)
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd1 = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        iter.map((index, _))
      }
    )
    rdd1.collect().foreach(println)
  }
}
