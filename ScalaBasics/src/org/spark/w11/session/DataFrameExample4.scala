package org.spark.w11.session

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DataFrameExample4 extends App {

    // Here creating the spark object using spark Configuration object.
  var sparkconf= new SparkConf()
  sparkconf.set("spark.app.name", "my first Application")
  sparkconf.set("spark.master", "local[2]")
  val spark = SparkSession.builder()
              .config(sparkconf)
              .getOrCreate()
              
  val orderDF = spark
  .read
  .option("header", true)
  .option("inferSchema", true)
  .csv("./files/w11/orders-201019-002101.csv")
  
// where order ID is more then 10000 then followed by select and groupBy
  
  val groupOrderDF = orderDF
  .repartition(4)
  .where("order_customer_id > 12000")
  .select("order_id","order_customer_id")
  .groupBy("order_customer_id")
  .count()
  
  groupOrderDF.show()

  scala.io.StdIn.readLine()
  spark.stop()
}
