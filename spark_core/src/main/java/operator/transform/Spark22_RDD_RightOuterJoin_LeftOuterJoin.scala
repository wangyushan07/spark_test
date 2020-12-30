package operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark22_RDD_RightOuterJoin_LeftOuterJoin {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - rightOuterJoin&leftOuterJoin

        val rdd1 = sc.makeRDD(List(
            ("a", 1), ("b", 2)//, ("c", 3)
        ))

        val rdd2 = sc.makeRDD(List(
            ("a", 4), ("b", 5),("c", 6)
        ))
        //val leftJoinRDD = rdd1.leftOuterJoin(rdd2)
        val rightJoinRDD = rdd1.rightOuterJoin(rdd2)

        //leftJoinRDD.collect().foreach(println)
        rightJoinRDD.collect().foreach(println)



        sc.stop()

    }
}
