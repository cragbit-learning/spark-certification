package com.cgarbit.certification.sql;

import static org.apache.spark.sql.functions.col;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SchemaSql {

	public static void main(String[] args) {
		
		SchemaSqlSpark schemaSqlSpark = new SchemaSqlSpark();
		schemaSqlSpark.sqlOperation();
	}

}

class SchemaSqlSpark implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private transient SparkSession sparkSession;

	public SchemaSqlSpark() {
		
		sparkSession = SparkSession.builder().appName("SCHEMASQL").master("local[*]").getOrCreate();
	}

	public void closeSparkSession() {
		sparkSession.close();
	}

	public void sqlOperation() {
		
		List<Row> data = new ArrayList<>();
		data.add(RowFactory.create("sanjiv",40));
		data.add(RowFactory.create("kumar",40));
		data.add(RowFactory.create("ashok",40));
		data.add(RowFactory.create("rajiv",50));
		data.add(RowFactory.create("aarush",12));
		data.add(RowFactory.create("kalindi",5));
		data.add(RowFactory.create("aarav",4));
		
		StructField[] fields  = new StructField[] {
				new StructField("name", DataTypes.StringType, false, Metadata.empty()),
				new StructField("age", DataTypes.IntegerType, false, Metadata.empty())
				
		};
		StructType schema =  new StructType(fields);
		Dataset<Row> results = sparkSession.createDataFrame(data, schema);
		results.show();
		
		results.select(col("name")).filter(col("age").equalTo(40)).orderBy(col("name").desc()).show();
		
		closeSparkSession();
	}
	
	
	
	
}


