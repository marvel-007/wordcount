package com.mocha.spark.socket

import java.io.PrintWriter
import java.net.ServerSocket

import scala.io.Source

/**
  * 模拟套接字服务端发送数据
  *
  * @author Yangxq
  * @version 2017/5/14 13:46
  */
object DataSourceSocket {
  def index(length: Int) = {

    val rdm = new java.util.Random

    rdm.nextInt(length)
  }

  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("Usage: <filename> <port> <millisecond>")
      System.exit(1)
    }

    val fileName = args(0)
    //文件路径
    val lines = Source.fromFile(fileName).getLines.toList
    val rowCount = lines.length

    val listener = new ServerSocket(args(1).toInt) //端口
    while (true) {
      val socket = listener.accept()
      new Thread() {
        override def run = {
          println("Got client connected from: " + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream(), true)
          while (true) {
            Thread.sleep(args(2).toLong)
            //延迟时间
            val content = lines(index(rowCount))
            println(content)
            out.write(content + '\n')
            out.flush()
          }
          socket.close()
        }
      }.start()
    }
  }
}
