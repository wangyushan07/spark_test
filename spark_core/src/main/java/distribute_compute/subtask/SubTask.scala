package distribute_compute.subtask

/**
 * Created by wang on 2020/12/10.
 */
class SubTask extends Serializable {
  var datas:List[Int] = _
  var logic: (Int) => Int = _
  // 计算
  def compute() = {
    datas.map(logic)
  }
}
