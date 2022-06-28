package org.spark.w12.dataframe.sessions

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.sum

/*
 * 
 * Structured Apis Session 17
 * Aggregate Transformation
   * Grouping Aggregator
     * ================================
     * 1. want total Quantity for each Group
     * 2. do it using column object expression
     * 3. do it using string expression
     * 4. do it using spark SQL
 */

object SparkDataFrame11 extends App {
  
   Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "My Application")
  sparkConf.set("spark.master","local[2]")
  
  // create a spark context
  val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()  

  // reading a file using dataFrame  reader api
  val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", true)
      .option("path", "C:/workspace_data_engineering/week12- Apache Spark - Structured API Part-2/dataset/order_data-201025-223502.csv")
      .load()    
  
  val summaryDf =df.groupBy("Country", "InvoiceNo")
  .agg(sum("Quantity").as("TotalQuantity"),
      sum(expr("Quantity * UnitPrice").as("InvoiceValue") )).show()

  val summaryDf1 =df.groupBy("Country", "InvoiceNo")
  .agg(expr("sum(Quantity) as TotalQuantity"),
      expr("sum(Quantity * UnitPrice) as InvoiceValue") ).show()
      
  
}