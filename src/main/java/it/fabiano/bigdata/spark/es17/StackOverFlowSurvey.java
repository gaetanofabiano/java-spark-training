package it.fabiano.bigdata.spark.es17;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import it.fabiano.bigdata.spark.Utils;
import scala.Option;
/**
* Java-Spark-Training-Course
*
* @author  Gaetano Fabiano
* @version 1.0.0
* @since   2019-07-19 
*/
public class StackOverFlowSurvey {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        //SparkConf conf = new SparkConf().setAppName("StackOverFlowSurvey").setMaster("local[1]");
        SparkConf conf = new SparkConf().setAppName("StackOverFlowSurvey");

        SparkContext sparkContext = new SparkContext(conf);
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);

        final LongAccumulator total = new LongAccumulator();
        final LongAccumulator missingSalaryMidPoint = new LongAccumulator();
        final LongAccumulator numberOfBytes = new LongAccumulator();

        total.register(sparkContext, Option.apply("total"), false);
        missingSalaryMidPoint.register(sparkContext, Option.apply("missing salary middle point"), false);

        numberOfBytes.register(sparkContext, Option.apply("processed bytes"), false);
        //JavaRDD<String> responseRDD = javaSparkContext.textFile("in/2016-stack-overflow-survey-responses.csv");
        JavaRDD<String> responseRDD = javaSparkContext.textFile(args[0]);
        
        JavaRDD<String> responseFromCanada = responseRDD.filter(response -> {
            String[] splits = response.split(Utils.COMMA_DELIMITER, -1);

            total.add(1);

            if (splits[14].isEmpty()) {
                missingSalaryMidPoint.add(1);
            }

            numberOfBytes.add(response.getBytes().length);
            return splits[2].equals("Canada");

        });

        Thread.sleep(60000);
        System.out.println("Count of responses from Canada: " + responseFromCanada.count());
        System.out.println("Total count of responses: " + total.value());
        System.out.println("Count of responses missing salary middle point: " + missingSalaryMidPoint.value());
        System.out.println("Count of processed bytes: " + numberOfBytes.value());
        javaSparkContext.close();
    }
}
