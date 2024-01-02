package org.practice.sparkbyexamples

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

object SparkDataFrame166 extends App {
    
  case class Logging(level:String , datetime:String)
  
  def mapper(line:String): Logging={
    
    val fields = line.split(",")
    val logging:Logging = Logging(fields(0),fields(1))
    return logging
  }
  
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession
    .builder
    .appName("sparkSQL")
    .master("local[2]")
    .getOrCreate()
    
    import spark.implicits._
    /*
    val mylist = List("DEBUG,2015-2-6 16:24:07",
                      "WARN,2016-7-26 18:54:43",
                      "INFO,2012-10-18 14:35:19",
                      "DEBUG,2012-4-26 14:26:50",
                      "DEBUG,2013-9-28 20:27:13",
                      "INFO,2017-8-20 13:17:27",
                      "INFO,2015-4-13 09:28:17")

    val rdd1 = spark.sparkContext.parallelize(mylist) 
    val rdd2 = rdd1.map(mapper)
    val df1 =rdd2.toDF()
    
    
    df1.createOrReplaceTempView("logging_table")
//    spark.sql("select level,count(datetime) from logging_table group by level order by level")
//    .show(false)
    val df2 =spark.sql("select level,date_format(datetime,'MMM') as month form logging_table")
    df2.createGlobalTempView("new_logging_table")
    spark.sql("select level,month, count(1) from new_logging_table group by level, month").show()
    * *
    */
    
    val df3 = spark
              .read
              .option("header",true)
              .csv("C:/workspace_data_engineering/week12- Apache Spark - Structured API Part-2/dataset/biglog-201105-152517.txt")
       df3.createOrReplaceTempView("my_new_logging_table")
       
   val result = spark.sql(""" select level,date_format(datetime,'MMM') as month, count(1) as total 
                           from my_new_logging_table group by level, month""")
      
      spark.sql(""" select level,date_format(datetime,'MMM') as month, count(1) as total 
                           from my_new_logging_table group by level, month""")
      spark.sql("""select level,date_format(datetime,'MMM') as month,
                    date_format(datetime,'M') as monthnum 
                           from my_new_logging_table group by level, datetime order by monthnum""")
                           .groupBy("level")
                           .pivot("monthnum")
                           .count()
                           .show()

}