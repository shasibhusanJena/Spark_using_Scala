package org.spark.w12.dataframe.sessions

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.expr


  /* SQL/String Expression UDF we moving to next example
   * ======================================
   * 
   * 
   */
case class Person2(name: String , age:Int, city: String)

object SparkDataFrame8 extends App {
  // log level setting
  Logger.getLogger("org").setLevel(Level.ERROR)
  def ageCheck(age: Int):String = {
    
    if(age > 18) "y" else "N"
  }
  // create Spark conf
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "My Application")
  sparkConf.set("spark.master","local[2]")
  
  // create a spark context
  val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()
  
  // reading a file using dataFrame  reader api
  val df = spark.read
      .format("csv")
      .option("inferSchema", true)
      .option("path", "C:/workspace_data_engineering/week12- Apache Spark - Structured API Part-2/dataset/file.dataset1")
      .load()
  val df1 : Dataset[Row] = df.toDF("name","age","city")  

  spark.udf.register("parseAgeFunction",ageCheck(_:Int):String)
  val df2 =df1.withColumn("Adult", expr("parseAgeFunction(age)") )  
  df2.show
  /*
   * As UDF is registered in the catelog  we can fire a SQL Query.
   * 
   */
  spark.catalog.listFunctions().filter(x => x.name =="parseAgeFunction").show
  df1.createOrReplaceTempView("peopletable")
  spark.sql("select name, age, city, parseAgeFunction(age) as adult from peopletable").show()

}