package it.fabiano.bigdata.spark.es11;
/**
* Java-Spark-Training-Course
*
* @author  Gaetano Fabiano
* @version 1.0.0
* @since   2019-07-19 
*/
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class PairRddFromRegularRdd {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("create").setMaster("local[1]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> inputStrings = Arrays.asList("Gaetano 34", "Francesca 35", "Maria 29", "Jamcopo 8");

        JavaRDD<String> regularRDDs = sc.parallelize(inputStrings);

        JavaPairRDD<String, Integer> pairRDD = regularRDDs.mapToPair(getPairFunction());

        pairRDD.coalesce(1).saveAsTextFile("out/pair_rdd_from_regular_rdd");
        
        sc.close();
    }

    private static PairFunction<String, String, Integer> getPairFunction() {
        return s -> new Tuple2<>(s.split(" ")[0], Integer.valueOf(s.split(" ")[1]));
    }
}
