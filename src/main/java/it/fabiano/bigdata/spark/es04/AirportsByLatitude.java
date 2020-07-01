package it.fabiano.bigdata.spark.es04;

/**
* Java-Spark-Training-Course
*
* @author  Gaetano Fabiano
* @version 1.1.0
* @since   2019-07-19 
* @updated 2020-07-01 
*/
public class AirportsByLatitude {

	public static void main(String[] args) throws Exception {
		/*
    	SparkConf conf = new SparkConf().setAppName("airports").setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airports = sc.textFile("in/airports.text");

        JavaRDD<String> airportsInUSA = airports.filter(line -> Float.valueOf(line.split(Utils.COMMA_DELIMITER)[6]) > 40);

       // JavaRDD<String> othersFilter = airportsInUSA.filter(line -> Float.valueOf(line.split(Utils.COMMA_DELIMITER)[7]) < 15);

        JavaRDD<String> airportsByLatitude = airportsInUSA.map(line -> {
                    String[] splits = line.split(Utils.COMMA_DELIMITER);
                    return StringUtils.join(new String[]{splits[1], splits[6]}, ",");
                }
        );
        airportsByLatitude.saveAsTextFile("out/airports_by_latitude.text");
		 */
	}
}
