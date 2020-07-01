package it.fabiano.bigdata.spark.es12;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import it.fabiano.bigdata.spark.Utils;
import scala.Tuple2;

/**
* Java-Spark-Training-Course
*
* @author  Gaetano Fabiano
* @version 1.0.0
* @since   2019-07-19 
*/
/* Create a Spark program to read the airport data from in/airports.text;
generate a pair RDD with airport name being the key and country name being the value.
Then remove all the airports which are located in United States and output the pair RDD to out/airports_not_in_usa_pair_rdd.text

Each row of the input file contains the following columns:
Airport ID, Name of airport, Main city served by airport, Country where airport is located,
IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

Sample output:

("Kamloops", "Canada")
("Wewak Intl", "Papua New Guinea")
...
*/

/**
* Java-Spark-Training-Course
*
* @author  Gaetano Fabiano
* @version 1.1.0
* @since   2019-07-19 
* @updated 2020-07-01 
*/ 
public class AirportsNotInUsa {

	// ./spark-submit --class it.fabiano.bigdata.spark.es12.AirportsNotInUsa /tmp/data/java-spark-training-1.0.0.jar /tmp/data/airports.text /tmp/data/output.txt --master spark://master:7077
    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("airports");

        JavaSparkContext sc = new JavaSparkContext(conf);

        //JavaRDD<String> airportsRDD = sc.textFile("in/airports.text");

        JavaRDD<String> airportsRDD = sc.textFile(args[0]);

        JavaPairRDD<String, String> airportPairRDD = airportsRDD.mapToPair(getAirportNameAndCountryNamePair());

        JavaPairRDD<String, String> airportsNotInUSA = airportPairRDD.filter(keyValue -> !keyValue._2().equals("\"United States\""));

        //airportsNotInUSA.saveAsTextFile("out/airports_not_in_usa_pair_rdd.text");
        airportsNotInUSA.saveAsTextFile(args[1]);
        sc.close();
    }

    private static PairFunction<String, String, String> getAirportNameAndCountryNamePair() {
        return (PairFunction<String, String, String>) line -> new Tuple2<>(line.split(Utils.COMMA_DELIMITER)[1],
                                                                           line.split(Utils.COMMA_DELIMITER)[3]);
    }
}
