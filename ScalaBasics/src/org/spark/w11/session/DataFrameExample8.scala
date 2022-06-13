package org.spark.w11.session

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
/*
 * Explicit schema - Programatic approach 
 * 
 * */
object DataFrameExample8 extends App {
      // Here creating the spark object using spark Configuration object.
  var sparkconf= new SparkConf()
  sparkconf.set("spark.app.name", "my first Application")
  sparkconf.set("spark.master", "local[2]")
  val spark = SparkSession.builder()
              .config(sparkconf)
              .getOrCreate()
              
  val orderDF = spark
  .read
//  .format("json")
  // defalut format is parquet , we cannot read the file but the application can read the file
  .option("path","./files/w11/users-201019-002101.parquet")
  .option("mode", "FAILFAST")
  .load

  orderDF.printSchema()
  orderDF.show()
  
  scala.io.StdIn.readLine()
  spark.stop()
}