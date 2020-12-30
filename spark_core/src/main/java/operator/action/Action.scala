package operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Action {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4))

        // TODO - 行动算子
        // 所谓的行动算子，其实就是触发作业(Job)执行的方法
        // 底层代码调用的是环境对象的runJob方法
        // 底层代码中会创建ActiveJob，并提交执行。

        // 算子 ： Operator（操作）
        //         RDD的方法和Scala集合对象的方法不一样
        //         集合对象的方法都是在同一个节点的内存中完成的。
        //         RDD的方法可以将计算逻辑发送到Executor端（分布式节点）执行
        //         为了区分不同的处理效果，所以将RDD的方法称之为算子。
        //        RDD的方法外部的操作都是在Driver端执行的，而方法内部的逻辑代码是在Executor端执行。

        rdd.collect()

        sc.stop()

    }
}
