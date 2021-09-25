package com.cragbit.certification.rdd;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class LogCountExample {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		LogCount logCount = new LogCount();
		logCount.logExample();
	}

}

class LogCount implements Serializable {

	private static final long serialVersionUID = 1L;
	private SparkConf sparkConf;
	private transient JavaSparkContext javaSparkContext;

	public LogCount() {
		getSparkContext(setSpecialConf(getSparkconf()));
	}

	public SparkConf getSparkconf() {
		sparkConf = new SparkConf().setAppName("LogCount").setMaster("local[*]");
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

	public void logExample() {

		List<String> logs = new ArrayList<String>();
		logs.add("WARN: 4th Dec 2020");
		logs.add("ERROR: 5th Dec 2020");
		logs.add("INFO: 4th Dec 2020");
		logs.add("ERROR: 9th Dec 2020");
		logs.add("WARN: 5th Dec 2020");
		logs.add("WARN: 7th Dec 2020");
		logs.add("ERROR: 9th Dec 2020");
		logs.add("INFO: 2nd Dec 2020");
		logs.add("ERROR: 3rd Dec 2020");
		logs.add("WARN: 6th Dec 2020");

		JavaRDD<String> linesRDD = javaSparkContext.parallelize(logs);

		JavaPairRDD<String, Integer> wordTupleRDD = linesRDD.mapToPair(line -> {

			if (line.contains("WARN")) {
				return new Tuple2<String, Integer>("WARN", 1);
			} else if (line.contains("ERROR")) {
				return new Tuple2<String, Integer>("ERROR", 1);
			} else if (line.contains("INFO")) {
				return new Tuple2<String, Integer>("INFO", 1);
			} else {
				return new Tuple2<String, Integer>("Not Matching", 1);
			}
		});

		JavaPairRDD<String, Integer> reduceWordTupleRDD = wordTupleRDD.reduceByKey((value1, value2) -> value1 + value2);
		List<Tuple2<String, Integer>> tupleNumber = reduceWordTupleRDD.collect();

		for (Tuple2 t : tupleNumber) {
			System.out.println(t._1 + " , count : " + t._2);
		}

		closeSparkconf();
	}
}
