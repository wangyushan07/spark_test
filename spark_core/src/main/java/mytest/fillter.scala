package mytest

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by wang on 2020/12/11.
 */
object fillter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("LOGRDD")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("datas/apache.log")
    rdd.filter(line => {
      val str = line.split(" ")(3)
      str.startsWith("17/05/2015")
    }).collect().foreach(println)
  }
}