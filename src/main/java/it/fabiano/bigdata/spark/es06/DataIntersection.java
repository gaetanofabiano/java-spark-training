package it.fabiano.bigdata.spark.es06;


/**
* Java-Spark-Training-Course
*
* @author  Gaetano Fabiano
* @version 1.0.0
* @since   2019-07-19 
*/
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
"in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

Example output:
vagrant.vf.mmc.com
www-a1.proxy.aol.com
.....

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
public class DataIntersection {
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("sameHosts").setMaster("local[1]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> julyFirstLogs = sc.textFile("in/nasa_19950701.tsv");
		JavaRDD<String> augustFirstLogs = sc.textFile("in/nasa_19950801.tsv");
		
		JavaRDD<String> julyFirstHosts = julyFirstLogs.map(line -> line.split("\t")[0]);
		
		JavaRDD<String> augustFirstHosts = augustFirstLogs.map(line -> line.split("\t")[0]);
		
		JavaRDD<String> intersection = julyFirstHosts.intersection(augustFirstHosts);
		
		JavaRDD<String> cleanedHostIntersection = intersection.filter(host -> !host.equals("host"));
		
		cleanedHostIntersection.saveAsTextFile("out/nasa_logs_same_hosts.csv");
		
		sc.close();
	}

}
