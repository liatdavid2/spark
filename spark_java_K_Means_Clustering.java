package spark_job;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import mapper.HouseMapper;
import pojos.House;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

public class K_Means_Clustering_Example_30 {

public static void main(String[] args) {

//set log level to only print errors
Logger.getLogger("org").setLevel(Level.ERROR);
SparkSession spark = SparkSession.builder()
.appName("kmeans Clustering")
.master("local[*]")
.getOrCreate();

String fileName ="src/main/resources/Wholesale-customers-data.csv";
Dataset<Row> wholeSaleDf = spark.read().format("csv")
.option("inferSchema","true")
.option("header",true)
.load(fileName);


wholeSaleDf.show(10);
Dataset<Row> FeatureDf = wholeSaleDf
.select("Channel","Region","Fresh","Milk"
,"Grocery","Frozen","Detergents_Paper","Delicassen"); //select all Column
FeatureDf = FeatureDf.na().drop();//drop any rows that have no value
FeatureDf.show();

VectorAssembler assembler = new VectorAssembler()
.setInputCols(new String [] {"Channel","Region","Fresh","Milk"
,"Grocery","Frozen","Detergents_Paper","Delicassen"})
.setOutputCol("features");

Dataset<Row> trainingData = assembler.transform(FeatureDf).select("features");

KMeans KMean = new KMeans().setK(3);

KMeansModel model = KMean.fit(trainingData);
System.out.println(model.computeCost(trainingData));
model.summary().predictions().show();

}