package it.fabiano.bigdata.spark.es07;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
* Java-Spark-Training-Course
*
* @author  Gaetano Fabiano
* @version 1.0.0
* @since   2019-07-19 
*/
public class CollectExample {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("collect").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> inputWords = Arrays.asList("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop");
        JavaRDD<String> wordRdd = sc.parallelize(inputWords);

        List<String> words = wordRdd.collect();
        
        for (String  word : words) {
            System.out.println(word);
        }
        
        sc.close();
    }
}
