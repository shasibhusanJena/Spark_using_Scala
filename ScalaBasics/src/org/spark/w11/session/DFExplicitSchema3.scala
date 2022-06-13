package org.spark.w11.session

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import java.sql.Timestamp

case class Orders(order_id: Int,order_date :Timestamp ,order_customer_id: Int,order_status:String)
/*
 * in programatically we have to write lot of forlats , as struct , structType , that gets simpler in DDL
 * explicit schema - DDL String
 * 
 * */
object DFExplicitSchema3 extends App {
  // Here creating the spark object using spark Configuration object.
  var sparkconf= new SparkConf()
  sparkconf.set("spark.app.name", "my first Application")
  sparkconf.set("spark.master", "local[2]")
  val spark = SparkSession.builder()
              .config(sparkconf)
              .getOrCreate()

   //scala ddl type
   val orderSchemaDDL = "orderID Int, orderdate String ,customerid Int ,status String" 
// defalut format is parquet , we cannot read the file but the application can read the file
  val orderDf = spark
  .read.format("csv")
  .option("header", true)
  .schema(orderSchemaDDL)
  .option("path","./files/w11/orders-201019-002101.csv").load()

  import spark.implicits._
  val ordersDs = orderDf.as[Orders]
  
  orderDf.printSchema()
  orderDf.show()
  
  scala.io.StdIn.readLine()
  spark.stop()
}