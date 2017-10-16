package com.Spark;

import java.util.Comparator;

import scala.Tuple2;

public class SortByRankInt implements Comparator<Tuple2<Integer, Double>> 
{
	@Override
	public int compare(Tuple2<Integer, Double> o1, Tuple2<Integer, Double> o2) {
		
		double rank1 = o1._2() * 10000;
		double rank2 = o2._2() * 10000;
		
	    if(rank1 > rank2)
	    {
	    	return -1;
	    }
	    else if(rank1 < rank2)
	    {
	    	return 1;
	    }
	    else{
	    	return 0;
	    }   
	}
}
