package org.spark.w12.dataframe.sessions

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

/**
 * Structured API's session -14
 * create user defined function in DataFrame
 * it is to convert from DF to DS and vice versa
 */
case class Person(name: String , age:Int, city: String)
object SparkDataFrame6 extends App{
  // log level setting
  Logger.getLogger("org").setLevel(Level.ERROR)
  
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

  /**
   * Print Data Frame Schema after InferSchema    
   */
  df.printSchema()
/*
 * but we are not happy with the above df .printSchema() output 
 *  	|-- _c0: string (nullable = true)
      |-- _c1: integer (nullable = true)
      |-- _c2: string (nullable = true)
 * 
 */
  import spark.implicits._
  val df1 = df.toDF("name","age","city")
  df1.printSchema()
  /**
   * important :Next we will convert df1 to DataSet type ds1
   * important : main difference between DF and DS is DS gives us compile time data validation, where as DF dont give that benifit
   * 			ex: ds1.groupByKey(x => x.name)
   * we cannot use this groupByKey while working with data frame
   */
  val df2: Dataset[Row] = df1 
  val ds1 = df2.as[Person]
      ds1.groupByKey(x => x.name)
      ds1.printSchema()
  
  /*
   * Next if we want to convert dataset to dataFrame then use toDF()
   * and again want to convert it to person type Dataset then as[Person]
   * again want to convert to dataFrame then toDF()
   */
  val df3 = ds1.toDF().as[Person].toDF()
  
  
  
}