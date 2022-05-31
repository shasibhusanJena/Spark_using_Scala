package org.scala

import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
//06
/* 
 * Row_id name age number_of_connections
	 We need to find average number of connections for each age ,sorted by connections
	 expected output :- (62,500)
	 										(55,400)
	 										(33,600)
*/

object FriendsByAge extends App{

  def parseLine(line:String)={
    val fields = line.split("::")
    val age =fields(2).toInt
    val numFriends = fields(3).toInt
    (age, numFriends)
    
  }
	Logger.getLogger("org").setLevel(Level.ERROR)
	val sc = new SparkContext("local[*]","FriendsByAge")
	val input = sc.textFile("./files/friendsdata-201008-180523.csv")
	val mappedInput = input.map(parseLine)
	
/*input is 
  (33,100)
  (33,200)
  (33,300)
  output is expected as
  (33,(100,1))
  (33,(200,1))
  (33,(300,1))
*/
	
	val mappedfinal =mappedInput.map(x=> (x._1,(x._2,1)))
	val totalsByAge = mappedfinal.reduceByKey((x,y)=> (x._1+y._1 , x._2+y._2))
	val avgByAge = totalsByAge.map(x => (x._1, x._2._1 / x._2._2)).sortBy(x=>x._2)
	
	avgByAge.collect.foreach(println)
	
}



