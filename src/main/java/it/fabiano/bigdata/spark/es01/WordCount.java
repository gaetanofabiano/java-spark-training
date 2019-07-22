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
* @version 1.0.0
* @since   2019-07-19 
*/
public class WordCount {

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

    	//Logger
        Logger.getLogger("org").setLevel(Level.INFO);
       

        //Spark Context
        SparkConf configuration = new SparkConf().setAppName("wordCounts").setMaster("local[2]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(configuration);

        
        //RDD
        JavaRDD<String> lines = javaSparkContext.textFile("in/i_promessi_sposi.text");
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        System.out.println(words.count());
    
        //WordCount
        Map<String, Long> wordCounts = words.countByValue();
        
 
        //Stampa
        for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
        javaSparkContext.close();
    }
}
