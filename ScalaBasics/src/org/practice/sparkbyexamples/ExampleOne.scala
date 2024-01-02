package org.practice.sparkbyexamples

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
/*
 * How to creatr Hive table using Spark.
 */
object ExampleOne extends App {
  
  var conf = new SparkConf()
  conf.set("spark.app.name", "Spark Example")
  conf.set("spark.master", "local[2]")
  conf.set("spark.sql.warehouse.dir", "<path>/spark-warehouse")
  var spark =SparkSession
                  .builder()
                  .config(conf)
                  .enableHiveSupport()
                  .getOrCreate()
  
  var df = spark.createDataFrame(List(("scala",250000),("Spark",240000),("PHP",150000)))
  df.show()

  // Using SparkSession we can use the spark SQL and do some SQL queries.
  df.createOrReplaceTempView("sample_table")
  val df1 = spark.sql("SELECT * FROM sample_table")
  df1.show()
  //create Hive Tables
  
  spark.table("sample_table").write.saveAsTable("sample_hive_table")
  val df3 = spark.sql("SELECT * FROM sample_hive_table" )
  df3.show()
}