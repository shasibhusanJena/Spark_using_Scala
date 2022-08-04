package org.practice.examples

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

object ProcessData extends App {
  
  Logger.getLogger("Org").setLevel(Level.ERROR)
  
  var sparkconf= new SparkConf()
  sparkconf.set("spark.app.name", "my first Application")
  sparkconf.set("spark.master", "local[2]")
  val spark = SparkSession.builder()
              .config(sparkconf)
              .getOrCreate()

  
  val orderDf = spark
                .read.format("txt")
                .option("path","./files/trade26.txt")
                .load()
orderDf.show()
}