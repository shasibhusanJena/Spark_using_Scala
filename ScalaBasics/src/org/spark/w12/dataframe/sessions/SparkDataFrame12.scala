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

/*
 * 
 * Structured Apis Session 17
 * Aggregate Transformation
   * Window Aggregator
     * ================================
     * 1. Partition column - country
     * 2. ordering column - weeknum
     * 3. The windows size - from 1st row to the current row
 */

object SparkDataFrame12 extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "My Application")
  sparkConf.set("spark.master","local[2]")
  
  // create a spark context
  val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()  

  // reading a file using dataFrame  reader api
  val invoiceDf = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", true)
      .option("path", "C:/workspace_data_engineering/week12- Apache Spark - Structured API Part-2/dataset/windowdata-201025-223502.csv")
      .load()    

   val myWindow =  Window.partitionBy("country")
   .orderBy("weeknum")
   .rowsBetween(Window.unboundedPreceding,Window.currentRow)
      
   val myDf =invoiceDf.withColumn("RunningTotal", sum("invoicevalue").over(myWindow))
   myDf.show()
   
   
}