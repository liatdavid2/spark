package com.App

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

case class Credits(id:Integer,tconst:String,titleType:String,primaryTitle:String,
                   originalTitle:String,isAdult:Integer,startYear:Integer,endYear:Integer
                   ,runtimeMinutes:Integer,genres:String)

object datasetCsvApp {

    def datasetCsv():Unit={       
       val sparkSession = SparkSession.builder()
      .appName("App")
      .master("local")
      .getOrCreate()
      
       import sparkSession.implicits._
      
       val startTimeDS=System.currentTimeMillis();
       val moviesDS=sparkSession.read.option("header", "true").
option("lowerBound", "0"). // start csv row 0 
option("upperBound", "100000"). // end csv row 100000
option("numPartitions", "5") // 5 clusters
    
    .csv("C:/final backup react angular projects/big_data/spark/title.basics.csv")
    .as("Credits");
    val res=moviesDS.filter("id <= 10000 and isAdult=0");
    res.foreach(x =>println(x))
    //moviesDS.show();
    val endTimeDS=System.currentTimeMillis();
    println((endTimeDS-startTimeDS)/1000);//time to execute
        
    sparkSession.stop();
  }