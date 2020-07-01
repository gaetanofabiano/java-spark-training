package it.fabiano.bigdata.spark.es13;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;
/**
* Java-Spark-Training-Course
*
* @author  Gaetano Fabiano
* @version 1.1.0
* @since   2019-07-19 
* @updated 2020-07-01 
*/ 
public class WordCountPairRDD {

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/i_promessi_sposi.text");
        JavaRDD<String> wordRdd = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairRDD<String, Integer> wordPairRdd = wordRdd.mapToPair((PairFunction<String, String, Integer>) word -> new Tuple2<>(word, 1));

        JavaPairRDD<String, Integer> wordCounts = wordPairRdd.reduceByKey((Function2<Integer, Integer, Integer>) (x, y) -> x + y);

        Map<String, Integer> worldCountsMap = wordCounts.collectAsMap();

        for (Map.Entry<String, Integer> wordCountPair : worldCountsMap.entrySet()) {
            System.out.println(wordCountPair.getKey() + " : " + wordCountPair.getValue());

        }
        sc.close();
    }
}
