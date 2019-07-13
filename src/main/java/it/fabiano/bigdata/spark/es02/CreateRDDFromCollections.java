package it.fabiano.bigdata.spark.es02;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
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
public class CreateRDDFromCollections {
	public static void main(String[] args) {

		 	Logger.getLogger("org").setLevel(Level.ERROR);
	        SparkConf conf = new SparkConf().setAppName("RDD-creator").setMaster("local[*]");
	        JavaSparkContext sc = new JavaSparkContext(conf);

	        List<String> inputStrings = Arrays.asList("ciao", "come", "state", "?");
	        JavaRDD<String> integerRdd = sc.parallelize(inputStrings);
	        
	        System.out.println("the RDD size is:"+ integerRdd.count());
	        sc.close();
	}
}
