package org.practice.sparkbyexamples

import org.apache.spark.sql.SparkSession

object repartitionAndcoalesce extends App {
  
  var spark = SparkSession.builder()
    .master("local[5]")
    .appName("SparkByExamples.com")
    .getOrCreate();

  var rdd = spark.sparkContext.parallelize(Range(0,20))
  println("From local[5]"+rdd.partitions.size)

  var rdd1 = spark.sparkContext.parallelize(Range(0,25), 6)
  println("parallelize : "+rdd1.partitions.size)

  var rddFromFile = spark.sparkContext.textFile("./files/others/test.txt",10)
  println("TextFile : "+rddFromFile.partitions.size)
  //  RDD repartition()
  val rdd2 = rdd1.repartition(4)
  println("Repartition size after repartition: "+rdd2.partitions.size)
  // RDD coalesce()
   val rdd3 = rdd1.coalesce(7)
  println("Repartition size After coalesce: "+rdd3.partitions.size)
  rdd3.saveAsTextFile("/tmp/coalesce")
}