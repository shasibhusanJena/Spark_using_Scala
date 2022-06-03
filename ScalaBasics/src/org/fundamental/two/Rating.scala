package org.fundamental.two

import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
//05
//we will get only rating's column and find count of number of time rating 1,2,3,4,5 given by user
// Here values are tab separated

/*
196	242	3	881250949
186	302	3	891717742
22	377	1	878887116
244	51	2	880606923
166	346	1	886397596
*/

object Rating extends App{

	Logger.getLogger("org").setLevel(Level.ERROR)
	val sc = new SparkContext("local[*]","Rating")
	val input = sc.textFile("./files/moviedata-201008-180523.data")
	//get rating column from the input
	val mappedInput = input.map(x => x.split("\t")(2))
	// earlier we used to do (x=> (x,1)) now we can do countByValue, it will work exactly same.
	// we should use countByValue if we know it is the last operation , that will happen on local.
	val results = mappedInput.countByValue()
	results.foreach(println)
	scala.io.StdIn.readLine()
}