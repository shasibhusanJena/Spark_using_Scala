package org.spark.w12.dataframe.sessions

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

/**
 * Structured API's session -14
 */
case class Person1(name: String , age:Int, city: String)

object SparkDataFrame7 extends App{
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

/* ## column object expression UDF
 * ========================================
 * Whenever we want to add a new column we use .withColumn()   
 * First we have to register the column name using udf() method 
 * 	Note: basically we register the function with the driver and the driver will serialize the function and will send it to the executor 
 * example: this is what we want
 * sumit,30,Blore,Y
 * Kapil,32,Hyd,N
 * 
 * in order to make it executable we need two pkges
 * import org.apache.spark.sql.functions.udf
 * import org.apache.spark.sql.functions.column
 */
  import spark.implicits._
  val parseAgeFunction  = udf(ageCheck(_:Int):String)
  val df2 =df1.withColumn("Adult", parseAgeFunction(column("age")))
  df2.show()
  
  /* SQL/String Expression UDF we moving to next example
   * ======================================
   * 
   * 
   */
}