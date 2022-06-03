package org.spark.w10.broadcast
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object LogLevel extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","LogLevel")
  
  var myList = List("WARN: Tuesday 4 september 0405",
					  "ERROR: Tuesday 4 september 0405",
					  "ERROR: Tuesday 4 september 0405",	
					  "ERROR: Tuesday 4 september 0405",
					  "ERROR: Tuesday 4 september 0405")
	var originalLogRdd = sc.parallelize(myList)
	var newPairRdd = originalLogRdd.map(x => {
		val columns = x.split(":")
		val logLevel = columns(0)
		(logLevel,1)
	})
	val resultantRdd = newPairRdd.reduceByKey((x,y) => x+y)
	resultantRdd.collect().foreach(println)
}