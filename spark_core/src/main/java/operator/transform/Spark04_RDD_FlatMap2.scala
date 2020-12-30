package operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_FlatMap2 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - flatMap
        val rdd = sc.makeRDD(List(List(1,2),3,List(4,5)))

        val flatRDD = rdd.flatMap(
            data => {
                data match {
                    case list:List[_] => list
                    case dat => List(dat)
                }
            }
        )

        flatRDD.collect().foreach(println)



        sc.stop()

    }
}
