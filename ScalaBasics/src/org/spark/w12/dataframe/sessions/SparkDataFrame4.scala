package org.spark.w12.dataframe.sessions

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkDataFrame4 extends App {
 
 Logger.getLogger("org").setLevel(Level.ERROR)
  var sparkconf= new SparkConf()
  sparkconf.set("spark.app.name", "my first Application")
  sparkconf.set("spark.master", "local[2]")
  val spark = SparkSession.builder()
              .config(sparkconf)
              .getOrCreate()

 val linesRdd = spark.sparkContext.textFile("./files/w11/orders-201019-002101.csv")
 val myregex = """^(\S+) (\S+)\t(\S+)\,(\S+)""".r
 case class Orders(order_id:Int,customer_id:Int,order_status:String)
 
 def parser(line: String){
   line match{
     case myregex(order_id,date,customer_id,order_status)=>
       Orders(order_id.toInt,customer_id.toInt,order_status)
   }
 }

//   import spark.implicits._
//   var structuredRDD = linesRdd.map(parser)
//   val ordersDS = structuredRDD.toDS().cache()
//   ordersDS.printSchema()
//   ordersDS.groupBy("order_status").count()
//   ordersDS.filter(x => x.customer_ids)
//   
//  scala.io.StdIn.readLine()
//  spark.stop()
//  
}