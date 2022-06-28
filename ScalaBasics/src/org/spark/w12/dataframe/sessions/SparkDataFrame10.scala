package org.spark.w12.dataframe.sessions

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.sum

/*
 * Structured Apis Session 16
 * Aggregate Transformation
 * ============================================
 * 1. Simple Aggregation
 * 			After doing Aggregation we get single row as the output
 * 			or total records , sum of all quantities
 * 2. Grouping Aggregation
 * 			in the output there can be more then one Record
 * 3. Window Aggregation
 *      so we will be dealing with fixed size window
 * Simple Aggregation
 * ===================
 * 1. Load the file and create a Data Frame, we should do it using standard dataFrame reader API
 * 2. totalNumberOfRows, totalQuantity, avgUnitPrice, numberOfUniqueInvoices
 * 3. calculate it using column object expression 
 * 4. do the same using string expression
 * 5. do it using spark SQL
 * 
 */
object SparkDataFrame10 extends App {
  
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
   

   df.select(
       count("*").as("RowCount"), 
       sum("Quantity").as("TotalQuantity"),
       avg("UnitPrice").as("AvgPrice"),
       countDistinct("InvoiceNo").as("CountDistinct")
       ).show()
       
   df.selectExpr("count(*) as RowCount", 
       "sum(Quantity) as TotalQuantity",
       "avg(UnitPrice) as AvgPrice",
       "count(Distinct(InvoiceNo)) as countDistinct").show()
   
   df.createOrReplaceTempView("Sales")
   spark.sql("select count(*), sum(Quantity) , avg(UnitPrice),count(distinct(InvoiceNo)) from from sales").show()
}