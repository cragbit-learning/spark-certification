package com.cragbit.certification.rdd;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class TupleExampleObject {

	public static void main(String[] args) {

		TupleExampleSpark tupleExampleSpark = new TupleExampleSpark();
		tupleExampleSpark.tupleExample();
	}
}

class TupleExampleSpark implements Serializable {

	private static final long serialVersionUID = 1L;
	private SparkConf sparkConf;
	private transient JavaSparkContext javaSparkContext;

	public TupleExampleSpark() {
		getSparkContext(setSpecialConf(getSparkconf()));
	}

	public SparkConf getSparkconf() {
		sparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]");
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

	public void tupleExample() {

		List<Integer> numbers = new ArrayList<Integer>();
		numbers.add(10);
		numbers.add(20);
		numbers.add(30);
		numbers.add(9);
		numbers.add(25);

		JavaRDD<Integer> numberRDD = javaSparkContext.parallelize(numbers);
		
		//1st way to do like Object
		JavaRDD<NumberTuple> numberAndSqrt = numberRDD.map(number -> new NumberTuple(number));
		List<NumberTuple> numbersList = numberAndSqrt.collect();
		
		for (NumberTuple numberTuple : numbersList) {
			System.out.println("Number : " + numberTuple.getNumber());
			System.out.println("sqart number : " + numberTuple.getSqrt());
		}
		
		//2nd way to do from tuple
		JavaPairRDD<Integer, Double> numberTupleRDD = numberRDD.mapToPair(number -> new Tuple2<Integer, Double>(number, Math.sqrt(number)));
		List<Tuple2<Integer, Double>> tupleNumber = numberTupleRDD.collect();
		
		for(Tuple2 t : tupleNumber) {
			System.out.println("Number : " + t._1 + " , sqart number : " + t._2);
		}

		closeSparkconf();
	}
}

class NumberTuple implements Serializable {

	private static final long serialVersionUID = 1L;
	private Integer number;
	private Double sqrt;

	public NumberTuple(Integer num) {
		this.number = num;
		this.sqrt = Math.sqrt(num);
	}

	public Integer getNumber() {
		return number;
	}

	public void setNumber(Integer number) {
		this.number = number;
	}

	public Double getSqrt() {
		return sqrt;
	}

	public void setSqrt(Double sqrt) {
		this.sqrt = sqrt;
	}
}
