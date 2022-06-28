package org.spark.w12.dataframe.sessions

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

/** 
 *  How to refer a column in a dataSet
 *  Column String
 *  	orderDf.select("order_id", "order_status").show()  everything in the () are string format
 *  Column Object
 *  	orderDf.select(column("order_id"), col("order_status")).show()  here we are providing the column/ col prefix.
 *  	different type of column object expression are
 *  		$"order_id"
 *  		'order_id
 *  		column("order_id")
 *  		col('order_id')
 *  
 *  Note: - we cannot mix both column String and Column Object together
 *  	  	import spark.implicits._
 				  orderDf.select("order_id",column("order_status"))
 		Note: below three cannot mix with each other
 					column String - select("order_id")
 					column object - select(column("order_id"))
 					column expression - concat(x,y)
 */
object SparkDataFrame5 extends App {
  
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

  
  /**
   * Column String example
   * 
   */
  // orderDf.select("order_id", "order_status").show()
   
  /**
   * Column Object example
   */
  import spark.implicits._
  
//  orderDf.select(column("order_id"),col("order_date"),$"order_customer_id",'order_status)
  orderDf.select("order_id", "order_date","concat(order_status,'_STATUS')").show(false)
  scala.io.StdIn.readLine()
  spark.stop()
}