package com.cragbit.certification.rdd;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount {

	public static void main(String[] args) {

		Logger.getLogger("org.apache").setLevel(Level.ERROR);
		WordCountSpark wordCountSpark = new WordCountSpark();
		wordCountSpark.wordCount();
	}
}

class WordCountSpark implements Serializable {

	private static final long serialVersionUID = 1L;
	private SparkConf sparkConf;
	private transient JavaSparkContext javaSparkContext;

	public WordCountSpark() {
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

	public void wordCount() {

		JavaRDD<String> setenceRDD = javaSparkContext
				.textFile("/Users/kumars3/learn-spark/spark-certification/data/input/");
		JavaRDD<String> wordsRDD = setenceRDD.flatMap(setence -> Arrays.asList(setence.split(" ")).iterator());
		JavaRDD<String> filteredWordsRDD = wordsRDD.filter(word -> removeBadWords(word));
		JavaPairRDD<String, Integer> tupleWordRDD = filteredWordsRDD
				.mapToPair(word -> new Tuple2<String, Integer>(word, 1));
		JavaPairRDD<String, Integer> reducedWordByKey = tupleWordRDD.reduceByKey((value1, value2) -> value1 + value2);

		JavaPairRDD<Integer, String> switchedTuple = reducedWordByKey
				.mapToPair(tupleWord -> new Tuple2<Integer, String>(tupleWord._2, tupleWord._1));
		JavaPairRDD<Integer, String> sortedSwitchedTuple = switchedTuple.sortByKey(false);
		List<Tuple2<Integer, String>> listWords = sortedSwitchedTuple.collect();

		for (Tuple2<Integer, String> t : listWords) {
			System.out.println(t._2 + " : " + t._1);
		}

		// System.out.println(listWords);

		closeSparkconf();
	}

	public boolean removeBadWords(String word) {

		String removeWord[] = { ",", " ", ";", "-", "@", "_", "a", "and", "is", "in", "of", "are" };
		if (Arrays.asList(removeWord).contains(word)) {
			return false;
		} else {
			return true;
		}
	}

}
