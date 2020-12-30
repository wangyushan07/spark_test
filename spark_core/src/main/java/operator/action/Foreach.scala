package operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Foreach {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4))

        // foreach 其实是Driver端内存集合的循环遍历方法
        rdd.collect().foreach(println)
        println("******************")
        // foreach 其实是Executor端内存数据打印
        rdd.foreach(println)


        sc.stop()

    }
}
