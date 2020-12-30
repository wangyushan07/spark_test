package distribute_compute.simple

import java.net.ServerSocket

/**
 * Created by wang on 2020/12/10.
 */
object Executer_simple {
  def main(args: Array[String]): Unit = {
    val server = new ServerSocket(9999)
    println("服务端启动")
    val socket = server.accept()
    val stream = socket.getInputStream()
    println(stream.read())
  }
}
