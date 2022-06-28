package org.spark.w12.dataframe.sessions

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
/*
 *1,"2013-07-25",11318,"CLOSED"
  2,"2013-07-25",256,"PENDING_PAYMENT"
  3,"2013-07-25",12111,"COMPLETE"
  4,"2013-07-25",8827,"CLOSED"
  5,"2013-07-25",11318,"COMPLETE"
 
 * 1. want to create a scala list
 * 2.from scala list want to create a data Frame "orderId","orderDate","customerId","status"
 * 3.Want to convert ordinary field to epoch timestamp (unix time stamp) number of seconds after 1st jan 1970
 * 4.create a olumn with the name "newid" and make sure it has unique IDs
 * 5.Drop Duplicates - (orderdate, customerID)
 * 6.Drop Order ID column
 * 7. Sort it based on Order Date
 * */
object SparkDataFrame9 extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "My Application")
  sparkConf.set("spark.master","local[2]")
  
  // create a spark context
  val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()
  val myList = List((1,"2013-07-25",11318,"CLOSED"),
                    (2,"2013-07-26",256,"PENDING_PAYMENT"),
                    (3,"2013-07-27",12111,"COMPLETE"),
                    (4,"2013-07-23",8827,"CLOSED"),
                    (5,"2013-07-25",11318,"COMPLETE"))
/*
 * Here we want to create a list into DF
 */
                    /*
 * val orderDf = spark.createDataFrame(myList)
 * But problem with above code is it will not provide any column names , to fix it follow below code
 * 
 */
  val orderDf = spark
                .createDataFrame(myList)
                .toDF("orderId","orderDate","customerId","status")
    val newDf =orderDf
    .withColumn("orderDate", unix_timestamp(col("orderDate").cast(DateType)))
    .withColumn("newId", monotonically_increasing_id())
    .dropDuplicates("orderDate","customerId")
    .drop("orderId")
    .sort("orderDate")
              
    
  newDf.printSchema()
  newDf.show()

}