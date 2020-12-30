package mytest

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by wang on 2020/12/11.
 */
object apachelog {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("LOGRDD")
    val sc = new SparkContext(conf)
    sc.textFile("datas/apache.log").map(line => {
      line.split(" ")(6)
    }).collect().foreach(println)
    sc.stop()
  }
}
