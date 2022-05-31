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

object FriendsByAge_modified extends App{

  def parseLine(line:String)={
    val fields = line.split("::")
    val age =fields(2).toInt
    val numFriends = fields(3).toInt
    (age, numFriends)
    
  }
	Logger.getLogger("org").setLevel(Level.ERROR)
	val sc = new SparkContext("local[*]","FriendsByAge_modified")
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
//	in below line we are trying do changes in the value using map function instead of that we can use mapValue, as shown below 

// means instead of doing map(x=> (x._1,(x._2,1))) we can do mapValues(x =>(x,1)).
// val mappedFinal =mappedInput.map(x=> (x._1,(x._2,1)))
	val mappedFinal =mappedInput.mapValues(x =>(x,1))
	val totalsByAge = mappedFinal.reduceByKey((x,y)=> (x._1+y._1 , x._2+y._2))
/**
 * input 
 * (33,(600,3))
 * output
 * (33,200)
 * bellow line can be modified as	
 */
//	val avgByAge = totalsByAge.map(x => (x._1, x._2._1 / x._2._2)).sortBy(x=>x._2)
	val avgByAge = totalsByAge.mapValues(x => x._1/ x._2).sortBy(x=>x._2)
	avgByAge.collect.foreach(println)
	scala.io.StdIn.readLine()
	
}



