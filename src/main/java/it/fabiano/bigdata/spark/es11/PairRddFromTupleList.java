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
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class PairRddFromTupleList {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("create").setMaster("local[1]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> tuples = Arrays.asList(new Tuple2<>("Gaetano", 23),
                                                            new Tuple2<>("Francesca", 29),
                                                            new Tuple2<>("Maria", 29),
                                                            new Tuple2<>("Jacopo",8));

        JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(tuples);

        pairRDD.coalesce(1).saveAsTextFile("out/pair_rdd_from_tuple_list");
        
        sc.close();
    }
}
