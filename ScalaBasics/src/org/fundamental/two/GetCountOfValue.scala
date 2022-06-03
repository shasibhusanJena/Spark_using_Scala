package org.fundamental.two
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

//02
object GetCountOfValues extends App {
  //logger setup
  Logger.getLogger("org").setLevel(Level.ERROR)
  //spark context object
  val sc = new SparkContext("local[*]","GetCountOfValues")
  //read file input
  val input = sc.textFile("./files/sample.txt")
  //split by " "
  val words = input.flatMap(x=> x.split(" "))
  //(x,1)
  val toWordMap = words.map(x => (x,1))
  
  // final count after applying reduce by key
  val finalCount = toWordMap.reduceByKey((x,y)=> x+y)
  
  // we want tuple to reversh (big,1) to (1,big)
  val reverseTuple =  finalCount.map(x => (x._2,x._1))
  
  val sortedResults =reverseTuple.sortByKey(false).map(x=> (x._2,x._1))
  
  // now remove the () from the result for better printing
  val results =sortedResults.collect
  
  for(result <- results){
    val word = result._1
    val count = result._2
    println(s"$word,$count")
  }
  //scala.io.StdIn.readLine()
  
}