package com.cragbit.certification.join;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

public class RDDJoin {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.ERROR);
		RDDJoinSpark rddJoinSpark  = new RDDJoinSpark();
		rddJoinSpark.rddJoin();
	}
}

class RDDJoinSpark implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private SparkConf sparkConf;
	private transient JavaSparkContext javaSparkContext;

	public RDDJoinSpark() {
		getSparkContext(setSpecialConf(getSparkconf()));
	}

	public SparkConf getSparkconf() {
		sparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]");
				//.setMaster("local[*]");
		return sparkConf;
	}

	public SparkConf setSpecialConf(SparkConf sparkConf) {
		// This is for recreating directory
		sparkConf.set("spark.hadoop.validateOutputSpecs", "false");
		return sparkConf;
	}

	public JavaSparkContext getSparkContext(SparkConf sparkConf) {
		try {
			javaSparkContext = new JavaSparkContext(sparkConf);
		} catch (Exception e) {
			e.getStackTrace();
		}
		return javaSparkContext;
	}

	public void closeSparkconf() {
		javaSparkContext.close();
	}

	public void rddJoin() {
		
		List<Tuple2<Integer, Integer>> visitRaw = new ArrayList<>();
		visitRaw.add(new Tuple2<>(4, 18));
		visitRaw.add(new Tuple2<>(6, 4));
		visitRaw.add(new Tuple2<>(10, 9));
		
		
		List<Tuple2<Integer, String>> userRaw = new ArrayList<>();
		userRaw.add(new Tuple2<>(1, "sanjiv"));
		userRaw.add(new Tuple2<>(2, "ashok"));
		userRaw.add(new Tuple2<>(3, "aarush"));
		userRaw.add(new Tuple2<>(4, "kalindi"));
		userRaw.add(new Tuple2<>(4, "test"));
		userRaw.add(new Tuple2<>(4, "xyz"));
		userRaw.add(new Tuple2<>(5, "aarav"));
		userRaw.add(new Tuple2<>(6, "arnab"));
		
		JavaPairRDD<Integer, Integer> visitRawRDD = javaSparkContext.parallelizePairs(visitRaw);
		JavaPairRDD<Integer, String> userRawRDD = javaSparkContext.parallelizePairs(userRaw); 
		
		JavaPairRDD<Integer, Tuple2<Integer, String>> joinRDD = visitRawRDD.join(userRawRDD);
		joinRDD.foreach(value -> System.out.println(value));
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> leftJoinRDD = visitRawRDD.leftOuterJoin(userRawRDD).sortByKey();
		leftJoinRDD.foreach(value -> System.out.println(value)); 
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		leftJoinRDD.foreach(value -> System.out.println(value._2._2.orElse("Not Found")));
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> rightJoinRDD = visitRawRDD.rightOuterJoin(userRawRDD).sortByKey();
		rightJoinRDD.foreach(value -> System.out.println(value));  
		
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<String>>> fullJoinRDD = visitRawRDD.fullOuterJoin(userRawRDD);
		fullJoinRDD.foreach(value -> System.out.println(value)); 
		
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> cartesianJoinRDD = visitRawRDD.cartesian(userRawRDD);
		cartesianJoinRDD.foreach(value -> System.out.println(value)); 
		 
		
		
		
		
		closeSparkconf();
	}
	
	
	
	
}
