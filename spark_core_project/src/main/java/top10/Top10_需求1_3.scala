package top10

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by wang on 2020/12/14.
 */
//先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数。
/*进一步优化
1.存在大量reduceByKey中存在大量的shuffle
*/

object Top10_需求1_3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Top10")
    val sc = new SparkContext(conf)
    //读取原始数据
    val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    rdd.cache()

    //转换数据结构（品类id，（1,0,0））
    val clickCount = rdd.flatMap(action => {
      val strings: Array[String] = action.split("_")

      if(strings(6) != "-1"){
        List((strings(6), (1,0,0)))
      } else if(strings(8) != "null"){
        val orderids: Array[String] = strings(8).split(",")
        orderids.map(id=>(id,(0,1,0)))
      }else if(strings(10) != "null"){
        val payids: Array[String] = strings(10).split(",")
        payids.map(id=>(id,(0,0,1)))
      }else {
        Nil
      }
    })

    //只有一个reduceByKey，只有一个shuffer
    val mapvaluerdd = clickCount.reduceByKey((t1,t2)=>{
      (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
    })

    val resultrdd = mapvaluerdd.sortBy(_._2,false).take(10)

    //打印到控制台
    resultrdd.foreach(println)

    sc.stop()
  }
}
