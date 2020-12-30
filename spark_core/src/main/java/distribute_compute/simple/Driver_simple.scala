package distribute_compute.simple

import java.net.Socket

/**
 * Created by wang on 2020/12/10.
 */
object Driver_simple {
  def main(args: Array[String]): Unit = {
    val socket = new Socket("localhost",9999)
    val stream = socket.getOutputStream()
    stream.write(2)
    stream.close()
    socket.close()
    println("数据传输完成")
  }
}
