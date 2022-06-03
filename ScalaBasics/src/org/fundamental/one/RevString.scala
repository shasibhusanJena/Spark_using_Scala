package org.fundamental.one

import scala.io.StdIn._
object RevString {
  def main(args: Array[String]): Unit = {
  
    val input = readLine()
    //1st output
    val output = input.reverse
    print("output")
    
    //2nd output
    val output2 = input.split(" ").map(x => x.reverse)
    println(output2.mkString(" "))
    
    val outpt3 = input.split(" ").reverse
    println(outpt3.mkString(" "))
  }
}