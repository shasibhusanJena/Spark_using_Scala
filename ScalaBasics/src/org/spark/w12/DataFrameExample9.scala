package org.spark.w12

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.log4j.Level
import org.apache.log4j.Logger

object DataFrameExample9 extends App {
 
 Logger.getLogger("org").setLevel(Level.ERROR)
 var sparkconf= new SparkConf()
  sparkconf.set("spark.app.name", "my first Application")
  sparkconf.set("spark.master", "local[2]")
  val spark = SparkSession.builder()
              .config(sparkconf)
              .getOrCreate()

  val orderDf = spark
  .read.format("csv")
  .option("header", true)
  .option("inferSchema",true)
  .option("path","./files/w11/orders-201019-002101.csv")
  .load()

  println("Order DF has "+ orderDf.rdd.getNumPartitions)
  val orderRep =orderDf.repartition(4)  
  
  println("Order Rep DF has "+ orderRep.rdd.getNumPartitions)
  // created separate files in the folder location with the separate status 
  // , we are not storeing status in the file as the file name itself is with status type
  orderRep
  .write
  .format("csv")
  .partitionBy("order_status")
  .mode(SaveMode.Overwrite).option("path","./files/w11/output1").save()
  
  scala.io.StdIn.readLine()
  spark.stop()
}