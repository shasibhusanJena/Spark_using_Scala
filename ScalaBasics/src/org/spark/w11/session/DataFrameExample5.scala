package org.spark.w11.session

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.SparkContext

/**
 * Programmatically Specifying schema in pySpark
 * 
 */
object DataFrameExample5 extends App {
  
 val sparkConfig = new SparkConf()
     sparkConfig.set("spark.app.name", "Programatically specify the schema")
     sparkConfig.set("spark.master", "local[2]")
     
 val sc = SparkSession
     .builder()
     .config(sparkConfig)
     .getOrCreate()
  
val stringJsonRdd_new = sc.sparkContext.emptyRDD



}