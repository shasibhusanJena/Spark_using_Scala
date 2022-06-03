package org.fundamental.two

import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
//01
object UserCount extends App{
	
	Logger.getLogger("org").setLevel(Level.ERROR)
	val sc = new SparkContext("local[*]","wordCount")
	val input = sc.textFile("./files/sample.txt")
	// means what very we have as input make them split by " "
	val words = input.flatMap(_.split(" "))
	val toLowercase = words.map(_.toLowerCase())
	// x => (x,1) means everything comes as ("i",1)("me",1)("to",1)
	val wordMap = toLowercase.map(x => (x,1))
	val finalCount = wordMap.reduceByKey((x,y)=> x+y)
	finalCount.collect.foreach(println)
	
	scala.io.StdIn.readLine()
}