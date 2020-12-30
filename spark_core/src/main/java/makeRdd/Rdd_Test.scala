package makeRdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by wang on 2020/12/10.
 */
object Rdd_Test {
  def main(args: Array[String]): Unit = {
    val rDDTest = new SparkConf().setMaster("local[*]").setAppName("RDDTest")
    val sc = new SparkContext(rDDTest)
    val rdd = sc.makeRDD(List(1,2,3,4),2)
    rdd.saveAsTextFile("output")

  }
}
