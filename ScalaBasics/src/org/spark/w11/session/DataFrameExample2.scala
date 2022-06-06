package org.spark.w11.session

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DataFrameExample2 extends App {
  
  // Here creating the spark object using spark Configuration object.
  var sparkconf= new SparkConf()
  sparkconf.set("spark.app.name", "my first Application")
  sparkconf.set("spark.master", "local[2]")
  val spark = SparkSession.builder()
              .config(sparkconf)
              .getOrCreate()
  // showing data using read CSV method in spark
  val orderDF = spark.read.csv("./files/customerorders-201008-180523.csv")
  // by default it will show 20 records.
  orderDF.show()
  // get schema
  orderDF.printSchema()
  spark.stop()
  
}