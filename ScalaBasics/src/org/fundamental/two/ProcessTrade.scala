package org.fundamental.two

import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object ProcessTrade extends App {
  //logger setup
  Logger.getLogger("org").setLevel(Level.ERROR)
  //spark context object
  val sc = new SparkContext("local[*]","GetCountSortByValue")
  //read file input
  val input = sc.textFile("./files/others/trade.txt")
  //split by " "
  val words = input.flatMap(x=> x.split(" "))
  //(x,1)
  val toWordMap = words.map(x => (x,1))
  // final count after applying reduce by key
  val finalCount = toWordMap.reduceByKey((x,y)=> x+y)
  val sortedResults =finalCount.sortBy(x => x._2,false)
  // now remove the () from the result for better printing
  val results =sortedResults.collect
  for(result <- results){
    val word = result._1
    val count = result._2
    println(s"$word,$count")
  }
  scala.io.StdIn.readLine()
}