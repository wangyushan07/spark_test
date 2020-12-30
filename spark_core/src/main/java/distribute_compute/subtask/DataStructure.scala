package distribute_compute.subtask

/**
 * Created by wang on 2020/12/10.
 */
class DataStructure extends Serializable {
  val data = List(1,2,3,4)
  val logic:(Int)=>Int = _ * 2

}
