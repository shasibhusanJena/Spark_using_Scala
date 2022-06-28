package org.spark.w12.dataframe.sessions

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

/*
 * Spark DataFrames Session-11

 * data            metadata
 * spark warehouse catalog metaStore
 * spark.sql.warehouse.dir in memory(on terminating application it is gone)
 * we can use Hive metaStore to handle spark data , so tha it will persist even after application stopped.
 * for that have to add a jar file spark hive 2.4.4 jar
 * .enableHiveSupport() this property helps to store data in Hive metaStore
 * */
object SparkDataFrame3 extends App {
   
 Logger.getLogger("org").setLevel(Level.ERROR)
 var sparkconf= new SparkConf()
  sparkconf.set("spark.app.name", "my first Application")
  sparkconf.set("spark.master", "local[2]")
  val spark = SparkSession.builder()
              .config(sparkconf)
              .enableHiveSupport()
              .getOrCreate()

  val orderDf = spark
  .read.format("csv")
  .option("header", true)
  .option("inferSchema",true)
  .option("path","./files/w11/orders-201019-002101.csv")
  .load()

  spark.sql("create database if not exists retail8")
  
  orderDf.write
  .format("csv")
  .bucketBy(4, "order_customer_id")
  .sortBy("order_customer_id")
  .mode(SaveMode.Overwrite)
  .saveAsTable("retail8.orders")
  
  spark.catalog.listTables("retail").show()

  scala.io.StdIn.readLine()
  spark.stop()
}