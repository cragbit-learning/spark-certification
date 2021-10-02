package com.cgarbit.certification.sql;

import static org.apache.spark.sql.functions.col;

import java.io.Serializable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CreateTempView {

	public static void main(String[] args) {
		CreateTempViewSpark createTempViewSpark = new CreateTempViewSpark();
		createTempViewSpark.sqlOperation();
	}
}

class CreateTempViewSpark implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private transient SparkSession sparkSession;

	public CreateTempViewSpark() {
		
		sparkSession = SparkSession.builder().appName("BASICSQL").master("local[*]").getOrCreate();
	}

	public void closeSparkSession() {
		sparkSession.close();
	}

	public void sqlOperation() {
		
		Dataset<Row> records = sparkSession.read().option("header", true).csv("/Users/kumars3/learn-spark/spark-certification/data/input/students.csv");
		records.show(false);
		
		System.out.println("Total number of partiton : " + records.rdd().getNumPartitions() );
		System.out.println("Total records : " + records.count()); 
		
		//distinct using APIs
		records.select(col("year")).distinct().show();
		
		//create temp view
		records.createOrReplaceTempView("students"); 
		Dataset<Row> results= sparkSession.sql("SELECT DISTINCT(year) FROM students WHERE subject = 'Math' ORDER BY year DESC ");
		results.show(false);
		
		try {
			Thread.sleep(1000000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		closeSparkSession();
	}
	
	
	
	
}

