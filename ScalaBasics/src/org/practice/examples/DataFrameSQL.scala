package org.practice.examples

import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
object DataFrameSQL extends App {
  
  Logger.getLogger("Org").setLevel(Level.ERROR)

  var sparkConf = new SparkConf()
   sparkConf.set("spark.app.name", "my first application in SQL")
   sparkConf.set("spark.master", "local[2]")
 
   var spark =SparkSession
               .builder()
               .config(sparkConf)
               .getOrCreate()
   val orderDF = spark
                 .read
                 .option("header", true)
                 .option("inferSchema", true)
                 .csv("./files/w11/orders-201019-002101.csv")
   
    val groupOrderDF = orderDF
                  .repartition(4)
                  .where("order_customer_id >12000")
                  .select("order_id","order_customer_id")
                  .groupBy("order_customer_id")
                  .count()
    groupOrderDF.show()
scala.io.StdIn.readLine()
spark.stop()
}