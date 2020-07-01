package it.fabiano.bigdata.spark.es05;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Function2;

/**
* Java-Spark-Training-Course
*
* @author  Gaetano Fabiano
* @version 1.1.0
* @since   2019-07-19 
* @updated 2020-07-01 
*/
public class FunctionExamples {

    @SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setAppName("uppercase").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/uppercase.text");
        
        //filter Mode  1
        //Anonymous Inner Class
        lines.filter(new Function<String,Boolean>(){       	
			@Override
        	public Boolean call(String line) throws Exception{
        		return line.contains("Friday");
        	}
        });
        
        
        lines.filter(new Function<String,Boolean>(){

			@Override
			public Boolean call(String v1) throws Exception {
				// TODO Auto-generated method stub
				return null;
			}
        	
        });
        
        //filter Mode 2
      
        
        //filter Mode 3
        //Lambda Function
        lines.filter(line->line.contains("A"));
        
        //filter Mode 4 
        //Lambda Function with static inner function
        lines.filter(line->dontStartWithZeta(line));
        
        
        
        JavaRDD<String> lowerCaseLines = lines.map(line -> line.toUpperCase());

        lowerCaseLines.saveAsTextFile("out/uppercase.text");
        sc.close();
    }
    
    
    
    private static boolean dontStartWithZeta(String line) {
        return !(line.startsWith("Z"));
    }
    
    
    @SuppressWarnings("serial")
	public static class CustomFilter implements Function<Persona,Boolean>{

		

		@Override
		public Boolean call(Persona v1) throws Exception {
			// TODO Auto-generated method stub
			return v1.getCognome()!=null && v1.getNome()!=null && v1.getEta()!=0;
		}
    	
    }
    
    @SuppressWarnings("serial")
    public static class CustomFilter2 implements Function2<String,String,Boolean>{

		@Override
		public Boolean apply(String v1, String v2) {
			// TODO Auto-generated method stub
			return null;
		}

		
    	
    }
}
