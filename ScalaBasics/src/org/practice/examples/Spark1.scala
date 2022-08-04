package org.practice.examples

import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
// Read input data and then process it and print Y and N  of age is less/more then 18
object Spark1 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","Demo")
  val input =sc.textFile("./files/others/input.txt")
  
  val rdd2 = input.map(line => {
    
    val fields = line.split("	")
    if(fields(0).toInt >18)
      (fields(0),fields(1),fields(2),"Y")
    else
      (fields(0),fields(1),fields(2),"N")
  })
  
  rdd2.collect().foreach(println)
  
}