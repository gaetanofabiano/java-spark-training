package it.fabiano.bigdata.spark.es01;


import java.util.Arrays;
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
* @version 1.1.0
* @since   2019-07-19 
* @updated 2020-07-01 
*/
public class WordCount4Cluster {

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

    	//Logger
        Logger.getLogger("org").setLevel(Level.INFO);
       

        //Spark Context
        //SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[*]");
        SparkConf conf = new SparkConf().setAppName("wordCounts");
        
        JavaSparkContext sc = new JavaSparkContext(conf);

        
        //RDD
        //JavaRDD<String> lines = sc.textFile("in/i_promessi_sposi.text");
        
        JavaRDD<String> lines = sc.textFile(args[0]);
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

    
       
        //WordCount
        Map<String, Long> wordCounts = words.countByValue();
        

        //Stampa
        for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
            
        }
        sc.close();
    }
}
