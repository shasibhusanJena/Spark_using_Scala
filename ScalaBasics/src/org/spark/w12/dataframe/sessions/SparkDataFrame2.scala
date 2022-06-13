package org.spark.w12.dataframe.sessions

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode

/*
 * Spark DataFrames Session-11
 * 
 */
object SparkDataFrame2 extends App {
    
 Logger.getLogger("org").setLevel(Level.ERROR)
 var sparkconf= new SparkConf()
  sparkconf.set("spark.app.name", "my first Application")
  sparkconf.set("spark.master", "local[2]")
  val spark = SparkSession.builder()
              .config(sparkconf)
              .getOrCreate()

  val orderDf = spark
  .read.format("csv")
  .option("header", true)
  .option("inferSchema",true)
  .option("path","./files/w11/orders-201019-002101.csv")
  .load()
 /** here we are storing the data but it is not saved as atemporary mode
  *  so we dont want this knid of metaStore , we need Hive metaStore next example 
  */
  orderDf.write
  .format("csv")
  .mode(SaveMode.Overwrite)
  .saveAsTable("order1")

  scala.io.StdIn.readLine()
  spark.stop()
}