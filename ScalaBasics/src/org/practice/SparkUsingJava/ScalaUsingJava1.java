package org.practice.SparkUsingJava;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
/*
 * This example is to read a file and  and do multiple operations and check the DAG(Directied Acyclic Graph) too
 */
public class ScalaUsingJava1 {
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		
		//Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf  = new SparkConf();
		conf.setAppName("Scala using java");
		conf.setMaster("local[*]");
		JavaSparkContext sc =new JavaSparkContext(conf);
		
		// read input file from the folder location
		JavaRDD<String> initialRdd = sc.textFile("./files/others/bigLog.txt");
		JavaRDD<String> lettersOnlyRdd = initialRdd.map( sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase() );

        JavaRDD<String> removedBlankLines = lettersOnlyRdd.filter( sentence -> sentence.trim().length() > 0 );

        JavaRDD<String> justWords = removedBlankLines.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());

        JavaRDD<String> blankWordsRemoved = justWords.filter(word -> word.trim().length() > 0);

        JavaRDD<String> justInterestingWords = blankWordsRemoved.filter(word -> Util.isNotBoring(word));

        JavaPairRDD<String, Long> pairRdd = justInterestingWords.mapToPair(word -> new Tuple2<String, Long>(word, 1L));

        JavaPairRDD<String, Long> totals = pairRdd.reduceByKey((value1, value2) -> value1 + value2);

        JavaPairRDD<Long, String> switched = totals.mapToPair(tuple -> new Tuple2<Long, String> (tuple._2, tuple._1 ));

        JavaPairRDD<Long, String> sorted = switched.sortByKey(false);

        List<Tuple2<Long,String>> results = sorted.take(10);

        results.forEach(System.out::println);

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

        sc.close();
		
	}
}
