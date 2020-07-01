package it.fabiano.bigdata.spark.es08;

import java.util.Arrays;
import java.util.List;

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
public class ReduceExample {

    public static void main(String[] args) throws Exception {
       
        SparkConf conf = new SparkConf().setAppName("reduce").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> inputIntegers = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> integerRdd = sc.parallelize(inputIntegers);

        Integer product = integerRdd.reduce((x, y) -> x * y);

        System.out.println("product is :" + product);
        
        sc.close();
    }
}
