package distribute_compute.subtask

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

/**
 * Created by wang on 2020/12/10.
 */
object Driver_Task {
  def main(args: Array[String]): Unit = {
    // 连接服务器
    val client1 = new Socket("localhost", 9999)
    val client2 = new Socket("localhost", 8888)

    val dataStructure = new DataStructure()

    val out1: OutputStream = client1.getOutputStream()
    val objOut1 = new ObjectOutputStream(out1)

    val subTask = new SubTask()
    subTask.logic = dataStructure.logic
    subTask.datas = dataStructure.data.take(2)

    objOut1.writeObject(subTask)
    objOut1.flush()
    objOut1.close()
    client1.close()

    val out2: OutputStream = client2.getOutputStream()
    val objOut2 = new ObjectOutputStream(out2)

    val subTask1 = new SubTask()
    subTask1.logic = dataStructure.logic
    subTask1.datas = dataStructure.data.takeRight(2)
    objOut2.writeObject(subTask1)
    objOut2.flush()
    objOut2.close()
    client2.close()
    println("客户端数据发送完毕")
  }
}
