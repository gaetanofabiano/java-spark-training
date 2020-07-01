package it.fabiano.bigdata.spark.es15	;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
/**
* Java-Spark-Training-Course
*
* @author  Gaetano Fabiano
* @version 1.1.0
* @since   2019-07-19 
* @updated 2020-07-01 
*/ 
public class SortedWordCount {

	public static void main(String[] args) throws Exception {

		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName("SortedWordCountSolution").setMaster("local[3]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile("in/i_promessi_sposi.text");
		JavaRDD<String> wordRdd = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		
		
		JavaPairRDD<String, Integer> wordPairRdd = wordRdd.mapToPair(word -> new Tuple2<>(word, 1));
		JavaPairRDD<String, Integer> wordToCountPairs = wordPairRdd.reduceByKey((x, y) -> x + y);

		JavaPairRDD<Integer, String> countToWordParis = wordToCountPairs.mapToPair(wordToCount -> new Tuple2<>(wordToCount._2(),
				wordToCount._1()));
		JavaPairRDD<Integer, String> sortedCountToWordParis = countToWordParis.sortByKey(false);

		JavaPairRDD<String, Integer> sortedWordToCountPairs = sortedCountToWordParis
				.mapToPair(countToWord -> new Tuple2<>(countToWord._2(), countToWord._1()));

		//print only 1000
		int i=0;
		for (Tuple2<String, Integer> wordToCount : sortedWordToCountPairs.collect()) {
			i++;
			if(i<=1000)
				System.out.println(wordToCount._1() + " : " + wordToCount._2());
		}
		sc.close();
	}
}
