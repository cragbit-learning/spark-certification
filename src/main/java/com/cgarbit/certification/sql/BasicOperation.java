package com.cgarbit.certification.sql;

import java.io.Serializable;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.*;

public class BasicOperation {

	public static void main(String[] args) {
		BasicOperationSpark basicOperationSpark = new BasicOperationSpark();
		basicOperationSpark.sqlOperation();
	}

}

class BasicOperationSpark implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private transient SparkSession sparkSession;

	public BasicOperationSpark() {
		
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
		Row firstRecord = records.first();
		System.out.println("Subject : " + firstRecord.getAs("subject").toString());
		
		// 1st way 
		records.filter(" subject = 'Math' AND year >=2007 ").show();
			
		//2nd way
		records.filter(record -> record.getAs("subject").equals("Math") && Integer.parseInt(record.getAs("year")) >= 2007).show();
		
		// 3rd way
		Column subjectColumn = records.col("subject");
		Column yearColumn = records.col("year");
		
		records.filter(subjectColumn.equalTo("Math").and( yearColumn.geq(2007))).show();
		
		//4th way,
		Column subCol = functions.col("subject");  
		Column yearCol = functions.col("year");
		records.filter(subCol.equalTo("Math").and(yearCol.geq(2007))).show();
		
		//OR static import
		records.filter( col("subject").equalTo("Math").and( col("year").geq(2007) ).and(col("year")) ).show();
		
		
		try {
			Thread.sleep(1000000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		closeSparkSession();
	}
	
	
	
	
}

