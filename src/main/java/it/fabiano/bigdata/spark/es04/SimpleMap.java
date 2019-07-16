package it.fabiano.bigdata.spark.es04;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

public class SimpleMap {
	
	public static void main(String... args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("SimpleMap").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> inputNumbers = new ArrayList<Integer>();
        inputNumbers.add(1);      
        inputNumbers.add(2);    
        inputNumbers.add(3);    
        inputNumbers.add(4);    
        inputNumbers.add(5);    
        inputNumbers.add(6);    
        
        JavaRDD<Integer> integerRdd = sc.parallelize(inputNumbers);
        
        JavaRDD<Integer> mapped = integerRdd.map(i ->(i*i));
        
        integerRdd.foreach((str) -> System.out.println(str));
        
       
        //mapped.foreach((str) -> System.out.println(str));
       
       
        sc.close();

	}
}
