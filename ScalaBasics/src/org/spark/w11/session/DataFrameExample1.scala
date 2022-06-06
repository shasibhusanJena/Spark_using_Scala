package org.spark.w11.session

import org.apache.spark.sql.SparkSession


object DataFrameExample1 extends App {
  // creating the object using Spark Session instead of Spark context
  val spark =SparkSession
            .builder()
            .appName("My first Spark Session Application")
            .master("local[2]")
            .getOrCreate()
            
  spark.close()
}