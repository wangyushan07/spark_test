package mytest

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by wang on 2020/12/11.
 */
object sample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("LOGRDD")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 0))
    //0.4不是概率，而是基准值
    val unit = rdd.sample(false, 0.4)
    unit.collect().foreach(println)
  }
}
