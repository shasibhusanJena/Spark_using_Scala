package org.spark.w11.session

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.sql.Timestamp
/*Note 
 *  DataFrame are more prefered over DataSets..
    to convert from DataFrame to DataSet there is an overhead involved and this is for the casting it to a particular type
    Serialization: conveting data into a binary form
    
    When we are dealing with DataFrame then serialization is managed by tungsten binary format..
    when we are delaing with dataSets then the serialization is managed by java serialization.

 * */
object DataFrameExample6 extends App {

  case class orderData(order_ids: Int,order_date: Timestamp,order_customer_id: Int,order_status: String)
  
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

  /*
 * How to convert DataFrame to DataSet
 * to convert DF to DS we have to cast DF 
 * import spark.implicits._
 * and we cannot put it in the beginning of the import statement at top
 * */
  import spark.implicits._
  // we are casting the dataFrame "orderDF" to a particular type as "orderData"
  val ordersDS = orderDF.as[orderData]
  
  // below gives compile time error
  // ordersDS.filter(x => x.order_ids <10 ).show()
  
  // it give runtime error instead of compile time error
  orderDF.filter("order_ids < 10").show()
  scala.io.StdIn.readLine()
  spark.stop()
}