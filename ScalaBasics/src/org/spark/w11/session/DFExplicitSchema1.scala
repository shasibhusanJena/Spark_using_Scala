package org.spark.w11.session

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField

// explicit schema - Programatically
object DFExplicitSchema1 extends App {
  
  // Here creating the spark object using spark Configuration object.
  var sparkconf= new SparkConf()
  sparkconf.set("spark.app.name", "my first Application")
  sparkconf.set("spark.master", "local[2]")
  val spark = SparkSession.builder()
              .config(sparkconf)
              .getOrCreate()
/*
 * ("orderId",IntegerType,true) here true means nullable not allowed
 * */
  val ordersSchema = StructType(List(
      
      StructField("orderId",IntegerType,true),
      StructField("orderdate",TimestampType),
      StructField("customerid",IntegerType),
      StructField("status",StringType)
      ))

// defalut format is parquet , we cannot read the file but the application can read the file
  val orderDF = spark
  .read.format("csv")
  .option("header", true)
  .schema(ordersSchema)
  .option("path","./files/w11/orders-201019-002101.csv").load()

  orderDF.printSchema()
  orderDF.show()
  
  scala.io.StdIn.readLine()
  spark.stop()
}