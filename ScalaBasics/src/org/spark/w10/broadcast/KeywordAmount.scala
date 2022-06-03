package org.spark.w10.broadcast
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.io.Source
import org.spark_project.dmg.pmml.False

object KeywordAmount extends App {
  
  def loadBoaringWords(): Set[String]={
    
    var boringWords: Set[String] = Set()
    val lines = Source.fromFile("./files/boringwords-201014-183159.txt").getLines()
    for (line <- lines){
      //println("line:   "+line)
      boringWords = boringWords +line
    }
    boringWords
  }
  Logger.getLogger("org").setLevel(Level.ERROR)
	val sc = new SparkContext("local[*]","Keyword Amount")
  var nameSet =sc.broadcast(loadBoaringWords)
	val initial_rdd = sc.textFile("./files/bigdatacampaigndata-201014-183159.csv")
	/*
	 * (24.06,big data contents)
    (59.94,spark training with lab access)
    (28.45,online hadoop training institutes in hyderabad)
    (24.64,coursera data analytics)
    (34.86,ameerpet big data training cost)
    (60.94,good comment on big data trainer)
	 * */
	val mapped_input = initial_rdd.map(x => (x.split(",")(10).toFloat,x.split(",")(0)))
/*
 *  (24.75,analyst)
    (24.75,courses)
    (24.9,azure)
    (24.9,data)
    (24.9,lake)
    (24.9,training)
 * 
 * */
	val words = mapped_input.flatMapValues(x => x.split(" "))
	val finalMapped = words.map(x=> (x._2.toLowerCase(),x._1))
	/*
	 * Here we will add filter and will remove the boring words
	 * what ever is not there in the nameSet we want to keep it
	 * */
	finalMapped.filter(x => !nameSet.value(x._1))
	val total = finalMapped.reduceByKey(_+_)
	val sorted = total.sortBy(x => x._2,false)
	sorted.take(20).foreach(println)
	scala.io.StdIn.readLine()
	/*
	 * Output Data top 20 words removing broadcast
	 * (data,16394.64)
    (big,12889.278)
    (in,5774.84)
    (hadoop,4818.34)
    (course,4191.5903)
    (training,4099.3696)
    (online,3484.4202)
    (courses,2565.78)
    (intellipaat,2081.22)
    (analytics,1458.51)
    (tutorial,1383.3701)
    (hyderabad,1118.16)
	 * */
}