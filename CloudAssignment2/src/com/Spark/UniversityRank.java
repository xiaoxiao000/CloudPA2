package com.Spark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

import com.google.common.collect.Iterables;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

public final class UniversityRank {

  private static int partitions = 18;
  
  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Invalid input arguments. Exit!");
      System.exit(1);
    }
    
    SparkSession spark = SparkSession
      .builder()
      .appName("RankUniversity")
      .getOrCreate();

   
    JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

    // Partition input file and loads all title from input file and initialize linked titles
    // Store JavaPairRDD on disc to save memory.
    JavaPairRDD<Integer, Iterable<Integer>> links = lines.mapToPair(s -> {
      String[] parts = s.split("\t");
      return new Tuple2<>(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
    }).distinct().groupByKey(partitions).cache();

    // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    JavaPairRDD<Integer, Double> ranks = links.mapValues(rs -> 1.0);

    int iterations = Integer.parseInt(args[1]);
    
    // Calculates and updates URL ranks continuously using PageRank algorithm.
    for (int current = 0; current < iterations; current++) {
      // Calculates URL contributions to the rank of other URLs.    	
      JavaPairRDD<Integer, Double> contribs = links.join(ranks).values()
        .flatMapToPair(s -> {
          int urlCount = Iterables.size(s._1());
          List<Tuple2<Integer, Double>> results = new ArrayList<>();
          for (Integer n : s._1) {
            results.add(new Tuple2<>(n, s._2() / urlCount));
          }
          return results.iterator();
        });

      // Re-calculates URL ranks based on neighbor contributions.
      ranks = contribs.reduceByKey(new AddDouble(), partitions).mapValues(sum -> 0.15 + sum * 0.85);
    }

   
    Map<Integer,String> universityMap = spark.read().textFile(args[2]).javaRDD().mapToPair(s->{
    	String[] parts = s.split(",");
    	return new Tuple2<>(Integer.parseInt(parts[1]), parts[0].trim().toLowerCase());
    }).distinct().collectAsMap();
    
    
    // Collects all URL ranks and dump them to console.
    List<Tuple2<Integer, Double>> output = ranks.collect();
    
    PrintTop100Pages(output,universityMap);

    spark.stop();
  }
  
  // List top 100 pages
  public static void PrintTop100Pages(List<Tuple2<Integer, Double>> output,
		  Map<Integer,String> universityMap )
  {
	  List<Tuple2<Integer, Double>> sortedByRank = new ArrayList<Tuple2<Integer, Double>>(output);
	  
	  System.out.println("*************Output Top 100 University Start***********************");
	    int outputLimit = 100;
	    int counter = 0;
	    Collections.sort(sortedByRank, new SortByRankInt());
	    
	    for (Tuple2<Integer,Double> tuple : sortedByRank) {
	    	
	    	if(!universityMap.containsKey(tuple._1))
	    	{
	    		continue;
	    	}
	    	
	        if(counter >= outputLimit)
	    	  break;
	        counter++;
	        System.out.println( universityMap.get(tuple._1) +" pageid="+ tuple._1() + " has rank: " + tuple._2() + ".");
	    }
	    System.out.println("*************Output Top 100 University End***********************");	    
  }
 
  
  private static class AddDouble implements Function2<Double, Double, Double> {
		private static final long serialVersionUID = 1L;

		@Override
	    public Double call(Double a, Double b) {
	      return a + b;
	    }
	  }
}
