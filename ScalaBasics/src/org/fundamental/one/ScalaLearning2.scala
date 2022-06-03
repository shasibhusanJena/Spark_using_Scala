package org.fundamental.one

object calculator {
  def main(args: Array[String]): Unit = {
    
  }
  def squareIt(x: Int): Int = x*x
  println(squareIt(4))  
  
  def cubeIt(x: Int): Int = x*x*x
  println(cubeIt(4))
  
  def transformInt(x: Int, f:Int => Int ): Int = {
    f(x)
  }
  println(transformInt(8, squareIt))
  println(transformInt(8, cubeIt))
  // Anonymous  class
  println(transformInt(2, x => x*x))
  println(transformInt(2, x => x*x*x))
  println(transformInt(2, x => x*x*x*x))
  
  def divideByTwo(x: Int ) = {
    x/2
  }
  println(divideByTwo(10))
  println(transformInt(4, x => x/2))
  println(transformInt(4, x => {val y= x*2; y * y}))
  
}