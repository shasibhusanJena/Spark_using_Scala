package org.spark.w12.dataframe.sessions

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.expressions.Window
import org.apache.hadoop.hive.ql.plan.JoinDesc

/*
 * 
 * Structured Apis Session 19
 *There are two kinds of joins 
 * 1. Simple Join
 * 2. Broadcast Join
 * 	
 * We have two dataset
 * 		orders - order_customer_id
 * 		customers - customer_id
 * */

object SparkDataFrame13 extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "My Application")
  sparkConf.set("spark.master","local[2]")
  
  // create a spark context
  val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()  

  val customersDf = spark.read
      .format("csv")
      .option("path", "C:/Desktop/customers")
      .load()    

      val orderDf = spark.read
      .format("csv")
      .option("path", "C:/Desktop/orders")
      .load()    
 
   val joinCondition = orderDf.col("order_customer_id") === customersDf.col("customer_id")
   val join_type = "inner"
   orderDf.join(customersDf,joinCondition,join_type).show()
   
   
}