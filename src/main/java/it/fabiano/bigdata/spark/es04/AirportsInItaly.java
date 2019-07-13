package it.fabiano.bigdata.spark.es04;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import it.fabiano.bigdata.spark.Utils;


/**
* Java-Spark-Training-Course
*
* @author  Gaetano Fabiano
* @version 1.0.0
* @since   2019-07-19 
*/
/* 
 * 
 
 
 Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in Italy
and output the airport's name and the city's name to out/airports_in_italy.text.


*/
public class AirportsInItaly {

    public static void main(String[] args) throws Exception {

    	/*
    	 * Try to change local[2] to local[*]
    	 */
    	SparkConf conf = new SparkConf().setAppName("airports");
    	if(args.length==0) {
    		conf.setMaster("local[2]");
    	}
    	
        

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airports = sc.textFile("in/airports.text");

        JavaRDD<String> airportsInUSA = airports.filter(line -> line.split(Utils.COMMA_DELIMITER)[3].equals("\"Italy\""));

        
        //Map Transformation here
        JavaRDD<String> airportsNameAndCityNames = airportsInUSA.map(line -> {
                    String[] splits = line.split(Utils.COMMA_DELIMITER);
                    return StringUtils.join(new String[]{splits[1], splits[2]}, ",");
                }
        );
        airportsNameAndCityNames.saveAsTextFile("out/airports_in_italy.text");
        
        sc.close();
    }
}
