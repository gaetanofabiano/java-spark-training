package it.fabiano.bigdata.spark.es09;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
/**
* Java-Spark-Training-Course
*
* @author  Gaetano Fabiano
* @version 1.0.0
* @since   2019-07-19 
*/
public class LazyEvaluation {
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("uppercase").setMaster("local[*]");

		JavaSparkContext sc = new JavaSparkContext(conf);

		

		//NOthing appens when Spark see textFile() statement
		JavaRDD<String> lines = sc.textFile("in/uppercase.text");

		//NOthing appens when Spark see filter() trasformation
		JavaRDD<String> linesWithF = lines.filter(line -> line.startsWith("F"));
		
		//Spark only start loading "in/uppercase.text" file when firtst action is called on linesWithF
		String first = linesWithF.first();
		
		
		//Spark scans the file only until the first line starting with Friday is detected
		//It doesn't even need to go thought the entire file
		System.out.println(first); 
		
		sc.close();
	}
}
