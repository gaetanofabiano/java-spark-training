package it.fabiano.bigdata.spark.es06;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;




/* 

"in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
"in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.

Keep in mind, that the original log files contains the following header lines.
host	logname	time	method	url	response	bytes

Make sure the head lines are removed in the resulting RDD.
*/
/**
* Java-Spark-Training-Course
*
* @author  Gaetano Fabiano
* @version 1.1.0
* @since   2019-07-19 
* @updated 2020-07-01 
*/
public class DataUnion {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("unionLogs").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> julyFirstLogs = sc.textFile("in/nasa_19950701.tsv");
        JavaRDD<String> augustFirstLogs = sc.textFile("in/nasa_19950801.tsv");

        JavaRDD<String> aggregatedLogLines = julyFirstLogs.union(augustFirstLogs);

        JavaRDD<String> cleanLogLines = aggregatedLogLines.filter(line -> isNotHeader(line));

        JavaRDD<String> sample = cleanLogLines.sample(true, 0.1);

        sample.saveAsTextFile("out/sample_nasa_logs.csv");
        
        sc.close();
    }

    private static boolean isNotHeader(String line) {
        return !(line.startsWith("host") && line.contains("bytes"));
    }
}