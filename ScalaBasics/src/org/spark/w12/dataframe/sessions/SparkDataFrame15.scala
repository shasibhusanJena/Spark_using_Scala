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
 * Structured APIS - Session 22
* ===================================
* 1.simple - shuffle
* 2.broadcast - this does not require a shuffle
* when ever we are joining two large dataframes then it will invoke a simple join., and shuffle will be require
* when ever we have one large dataframe and other is a smaller dataframe , in that case we can go with the broadcast join
* 
* if we want to do broadcas then two changes 
* spark.sql("SET spark.sql.autoBroadcastJoinThreshold=-1")
* orderNew.join(broadcast(customersDf),joinCondition,join_type).drop(orderNew.col("cust_id"))
 */

object SparkDataFrame15 extends App {
  
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
      .option("header", "true")
      .option("inferSchema", true)
      .option("path", "C:/workspace_data_engineering/week12- Apache Spark - Structured API Part-2/dataset/customers-201025-223502.csv")
      .load()    


      val orderDf = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", true)
      .option("path", "C:/workspace_data_engineering/week12- Apache Spark - Structured API Part-2/dataset/orders-201025-223502.csv")
      .load()    
       /*
       * Here is an example of how to rename to column  * imp  withColumnRenamed() method
       */
 val orderNew = orderDf.withColumnRenamed("order_customer_id", "cust_id")
 
 val joinCondition = orderNew.col("cust_id") === customersDf.col("customer_id")
 val join_type = "outer"
 /*
  * As we have repeating customer_id so lets drop one of them
  * then add sort based on order_id
  * null value should be shown as '-' for that we can use "coalesce"
  */
      orderNew.join(customersDf,joinCondition,join_type).drop(orderNew.col("cust_id"))
      .sort("order_id")
      .withColumn("order_id", expr("coalesce(order_id,-1)"))
      .withColumn("order_status", expr("coalesce(order_status,-1)"))
      .show(100)
}