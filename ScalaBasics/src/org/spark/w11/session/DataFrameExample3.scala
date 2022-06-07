package org.spark.w11.session

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DataFrameExample3 extends App {
  
  // Here creating the spark object using spark Configuration object.
  var sparkconf= new SparkConf()
  sparkconf.set("spark.app.name", "my first Application")
  sparkconf.set("spark.master", "local[2]")
  val spark = SparkSession.builder()
              .config(sparkconf)
              .getOrCreate()
/* in order to get first row as our table headers 
 * +--------+--------------------+-----------------+---------------+
|     _c0|                 _c1|              _c2|            _c3|
+--------+--------------------+-----------------+---------------+
|order_id|          order_date|order_customer_id|   order_status|
 
 * infer will automatically guess the data types for each field.
 * option("header", true) means please treat it as header
 * option("inferSchema", true) means system if you are intelligent , 
 * please infer the data for us.but in production we should not use it, 
 * as it may end up with other issues.
 * * */              
              
  val orderDF = spark
  .read
  .option("header", true)
  .option("inferSchema", true)
  .csv("./files/w11/orders-201019-002101.csv")
  
      orderDF.show()
      orderDF.printSchema()
  scala.io.StdIn.readLine()
  spark.stop()
  
}