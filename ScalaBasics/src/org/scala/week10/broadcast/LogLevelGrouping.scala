package org.scala.week10.broadcast
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
object LogLevelGrouping extends App {
    Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","LogLevel")
    /*
     * 
      WARN: Sun Aug 16 10:37:51 BST 2015
      ERROR: Tue Nov 08 10:37:51 GMT 2016
      ERROR: Thu Aug 03 10:37:51 BST 2017
      WARN: Fri Jan 12 10:37:51 GMT 2018
      WARN: Sat Aug 23 10:37:51 BST 2014
     * */
  val baseRdd = sc.textFile("./files/bigLog.txt")
	val mappedRdd = baseRdd.map(x=> {
	  
	  val  fields = x.split(":")
	  (fields(0),1)
	})
	mappedRdd.reduceByKey(_+_).collect().foreach(println)

	scala.io.StdIn.readLine()
}