package mytest

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by wang on 2020/12/11.
 */
object testFlatMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("LOGRDD")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(List(List(1, 2), 3, List(4, 5)))
    val rdd1 = rdd.flatMap(

      data => {
        data match {
          case list: List[Int] => list
          case dat => List(dat)
        }
      }
    )
    rdd1.collect().foreach(println)


  }
}
