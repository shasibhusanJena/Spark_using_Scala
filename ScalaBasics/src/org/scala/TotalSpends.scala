package org.scala

import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
//04
// count total spending ,remove second column as product ID
object TotalSpends extends App{
	
	Logger.getLogger("org").setLevel(Level.ERROR)
	val sc = new SparkContext("local[*]","TotalSpends")
	
  //read file input
  val input = sc.textFile("./files/customerorders-201008-180523.csv")
  // we are makeing toFloat as it was taking input as a string and was appending the second column with it
  val mappedInput = input.map(x => (x.split(",")(0),x.split(",")(2).toFloat))
  //reduce by key
  val totalByCustomer = mappedInput.reduceByKey((x,y) =>(x+y))
  val sortedTotal =totalByCustomer.sortBy(x =>x._2)
  sortedTotal.collect.foreach(println)
  
}