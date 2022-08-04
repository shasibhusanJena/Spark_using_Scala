package org.fundamental.two

import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object ProcessTrade2 extends App {
  //logger setup
  Logger.getLogger("org").setLevel(Level.ERROR)
  //spark context object
  val sc = new SparkContext("local[*]","Process Trade")
  //read file input
  val input = sc.textFile("./files/others/trade.txt")
  //split by " "
  val array = input.flatMap(x=> x.split(" "))
}