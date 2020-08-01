package com.App

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

object OracleDbApp {
 def readFromOracleDb(): Unit = {
    
    val conf = new SparkConf().setAppName("OracleDb Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.format("jdbc").
    option("url", "jdbc:oracle:thin:@localhost:1521/xe").
    //option("driver", "oracle.jdbc.Driver.OracleDriver").

    option("user", "johny").
    option("password", "admin").
    option("dbtable", "tblpoint").
    load()

    df.registerTempTable("achat")
    val someRows = sqlContext.sql("select * from TBLPOINT").head()
    println("--------see here!------->" + someRows.mkString(" "))
  }
} 
}