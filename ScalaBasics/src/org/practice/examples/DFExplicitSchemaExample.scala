package org.practice.examples

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.log4j.Logger
import org.apache.log4j.Level

// explicit schema - Programatically
object DFExplicitSchemaExample extends App {
  
  Logger.getLogger("Org").setLevel(Level.ALL)
  val sparkConf = new SparkConf()
	sparkConf.set("spark.app.name","First Example")
	sparkConf.set("spark.master","local[2]")
	
val spark =SparkSession
	.builder()
	.config(sparkConf)
	.getOrCreate()

val orderSchema = StructType(List(
					StructField("orderId",IntegerType,true),
					StructField("order_date",TimestampType),
					StructField("order_customer_id",IntegerType),
					StructField("status",StringType)
			))

val orderDF =spark.read.format("csv")
      .option("path","./files/w11/orders-201019-002101.csv")
			.option("header",true)
			.schema(orderSchema)
			.load()

orderDF.printSchema()
orderDF.show()

scala.io.StdIn.readLine()
spark.stop()

}