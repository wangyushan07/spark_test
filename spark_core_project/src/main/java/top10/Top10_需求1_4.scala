package top10

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * Created by wang on 2020/12/14.
 */
//先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数。
/*进一步优化
还是有一个reduce，不如用累加器
*/

object Top10_需求1_4 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Top10")
    val sc = new SparkContext(conf)
    //读取原始数据
    val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    rdd.cache()

    val accumulator = new Top10Accumulator()
    sc.register(accumulator)

    rdd.foreach {
      action => {
        val strings: Array[String] = action.split("_")
        if (strings(6) != "-1") {
          accumulator.add(strings(6), "click")
        } else if (strings(8) != "null") {
          val orderids: Array[String] = strings(8).split(",")
          orderids.map { id => {
            accumulator.add(id, "order")
          }
          }
        } else if (strings(10) != "null") {
          val payids: Array[String] = strings(10).split(",")
          payids.map { id => {
            accumulator.add(id, "pay")
          }
          }
        }

      }
    }

    val accVal: mutable.Map[String, HotCategory] = accumulator.value
    val categories: mutable.Iterable[HotCategory] = accVal.map(_._2)

    val sort = categories.toList.sortWith(
      (left, right) => {
        if (left.clickCnt > right.clickCnt) {
          true
        } else if (left.clickCnt == right.clickCnt) {
          if (left.orderCnt > right.orderCnt) {
            true
          } else if (left.orderCnt == right.orderCnt) {
            if (left.payCnt > right.payCnt) {
              true
            } else {
              false
            }
          } else {
            false
          }
        } else {
          false
        }
      }
    )
    //    //打印到控制台
    sort.take(10).foreach(println)

    sc.stop()
  }

  case class HotCategory(id: String, var clickCnt: Int, var orderCnt: Int, var payCnt: Int)

  /*
  1.继承AccumulatorV2
  IN (品类id,行为类型)
  OUT (mutable.Map[String,HotCategory])
  2.重写6个方法
  * */

  class Top10Accumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] {
    private val outMap = mutable.Map[String, HotCategory]()

    override def isZero: Boolean = {
      outMap.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
      new Top10Accumulator
    }

    override def reset(): Unit = {
      outMap.clear()
    }

    override def add(v: (String, String)): Unit = {
      val id = v._1
      val optype = v._2
      val category: HotCategory = outMap.getOrElse(id, HotCategory(id, 0, 0, 0))
      if (optype == "click") {
        category.clickCnt += 1
      } else if (optype == "order") {
        category.orderCnt += 1
      } else {
        category.payCnt += 1
      }
      outMap.update(id, category)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      val map1 = this.outMap
      val map2 = other.value

      map2.foreach {
        case (id, cg) => {
          val category: HotCategory = map1.getOrElse(id, HotCategory(id, 0, 0, 0))
          category.payCnt += cg.payCnt
          category.orderCnt += cg.orderCnt
          category.clickCnt += cg.clickCnt
          map1.update(id, category)
        }
      }

    }

    override def value: mutable.Map[String, HotCategory] = outMap
  }

}
