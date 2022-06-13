package org.spark.w12.dataframe.sessions

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
/*
 * Spark DataFrames Session-10
 * */
object SparkDataFrame1 extends App {
  
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
  /*
   * createOrReplaceTempView will create a temporary table across the HDFS for SQL query support
   * */
  orderDf.createOrReplaceTempView("orders")
  // val resultDf =spark.sql("select order_status, count(*) as status_count from orders group by order_status order by status_count")
  // get the top 20 orders with order type as closed
  val resultDf =spark.sql("select order_customer_id, count(*) as total_orders from orders where "+
      "order_status='CLOSED' group by order_customer_id order by total_orders desc")

  resultDf.show()

  scala.io.StdIn.readLine()
  spark.stop()
}