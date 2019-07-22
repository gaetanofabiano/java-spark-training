package it.fabiano.bigdata.spark.es02;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
/**
* Java-Spark-Training-Course
*
* @author  Gaetano Fabiano
* @version 1.0.0
* @since   2019-07-19 
*/
public class CreateRDDFromInputFile {
	public static void main(String[] args) {
			
			Logger.getLogger("org").setLevel(Level.ERROR);
	        SparkConf conf = new SparkConf().setAppName("InputFileName").setMaster("local[*]");

	        SparkContext sparkContext = new SparkContext(conf);
	        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);

	        JavaRDD<String> inputFileRDD = javaSparkContext.textFile("in/2016-stack-overflow-survey-responses.csv");
	        
	        
	     
	        System.out.println("the RDD size is:"+ inputFileRDD.count());

	        javaSparkContext.close();
	}
}
