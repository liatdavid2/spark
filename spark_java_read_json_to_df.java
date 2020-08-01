package spark_job;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
public class App {

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder()
				.appName("Combione 2 Datasets")
				.master("local[*]")
				.getOrCreate();

		Dataset<Row> durhamDf = buildDurhamParksDataFrame(spark);
		durhamDf.printSchema();
		durhamDf.show(10);
	}

	private static Dataset<Row> buildDurhamParksDataFrame(SparkSession spark) {
		Dataset<Row> df = spark.read().format("json").option("multiline",true)
						  .load("src/main/resources/durham-parks.json");
		
		df = df.withColumn("park_id", concat(df.col("datasetid"),lit("_"),
				df.col("fields.objectid"),lit("_Durham")))
				.withColumn("park_name", df.col("fields.park_name"))
				.withColumn("city",lit("Durham"))//city allways Durham
				.withColumn("has_playground", df.col("fields.playground"))
				.withColumn("zipcode", df.col("fields.zip"))
				.withColumn("land_in_acres", df.col("fields.acres"))
				.withColumn("geoX", df.col("geometry.coordinates").getItem(0))
				.withColumn("geoY", df.col("geometry.coordinates").getItem(1))
				.drop("fields").drop("geometry").drop("record_timestamp")
				.drop("recordid").drop("datasetid");
		
		return df;
	}

}