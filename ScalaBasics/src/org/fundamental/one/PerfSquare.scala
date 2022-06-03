package org.fundamental.one

import scala.io.StdIn._
class PerfSquare {
  
  def main(args: Array[String]){
    //taking number of input as customer
    var numcustomers: Int = readInt()
    
    //taking second line as the customer bill amount
    var billAmount: String = readLine()
    
    // Split the values and then convert them into Int type , and then store in the array
    val billAmt:Array[Int]= billAmount.split(" ").map(x => x.toInt)
    
    // Find the perfect square
    var count =0
    for (i <- billAmt){
      val sqrt = Math.sqrt(i)
      if(sqrt.ceil -sqrt == 0){
        count  = count +1
      }
      print(count)
    }
    
  }
}