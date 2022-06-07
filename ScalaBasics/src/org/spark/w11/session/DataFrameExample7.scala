package org.spark.w11.session

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DataFrameExample7 extends App {

      // Here creating the spark object using spark Configuration object.
  var sparkconf= new SparkConf()
  sparkconf.set("spark.app.name", "my first Application")
  sparkconf.set("spark.master", "local[2]")
  val spark = SparkSession.builder()
              .config(sparkconf)
              .getOrCreate()
              
  val orderDF = spark
  .read
  .format("json")
  .option("header", false)
  .option("inferSchema", true)
  .csv("./files/w11/players-201019-002101.json")

  orderDF.printSchema()
  orderDF.show()
  
  scala.io.StdIn.readLine()
  spark.stop()
  
}