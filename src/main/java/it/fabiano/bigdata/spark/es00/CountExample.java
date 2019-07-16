package it.fabiano.bigdata.spark.es00;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
/**
* Java-Spark-Training-Course
*
* @author  Gaetano Fabiano
* @version 1.0.0
* @since   2019-07-19 
*/
public class CountExample {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.INFO);
        
        SparkConf conf = new SparkConf().setAppName("count");
       
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> inputWords = Arrays.asList("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop");
        JavaRDD<String> wordRdd = sc.parallelize(inputWords);

        System.out.println("Count: " + wordRdd.count());

        Map<String, Long> wordCountByValue = wordRdd.countByValue();

        System.out.println("CountByValue:");
        

        for (Map.Entry<String, Long> entry : wordCountByValue.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
        Thread.sleep(60000);
        sc.close();
    }
}
