package org.spark.w12

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Data_assignemnt12 extends App {
 
 Logger.getLogger("org").setLevel(Level.ERROR)
 var sparkconf= new SparkConf()
  sparkconf.set("spark.app.name", "my first Application")
  sparkconf.set("spark.master", "local[2]")
  val spark = SparkSession.builder()
              .config(sparkconf)
              .getOrCreate()

  val deptDf = spark
  .read.format("json")
  .option("header", true)
  .option("inferSchema",true)
  .option("path","./files/w12/dept.json")
  .load()

  val employeeDf = spark
  .read.format("json")
  .option("header", true)
  .option("inferSchema",true)
  .option("path","./files/w12/employee.json")
  .load()
  
val joinCondition = deptDf.col("deptid") === employeeDf.col("deptid")
val joinType = "left"
val joinedDf = deptDf.join(employeeDf, joinCondition, joinType)
val joinedDfNew = joinedDf.drop(employeeDf.col("deptid"))
joinedDfNew.groupBy("deptid").agg(count("empname").as("empcount"),first("deptName").as ("deptName")).dropDuplicates("deptName").show()
spark.stop()

}