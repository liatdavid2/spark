//in bucket good also cluster bucket
// .csv("gs://dataproc-a618ac02-dd7b-47aa-8b10-043d7c2edef8-asia-east1/people.csv")


package com.fff
import org.apache.spark.sql.SparkSession

object DfWithCsv {
   def main(args: Array[String]): Unit = {
    //Creating Spark Context and RDD in Spark 2 x style
     val sparkSession=SparkSession.builder()
    .appName("DF CSV")
  // .master("local[*]")
    .getOrCreate()
    
    val df=sparkSession.read
    .option("header", "true")
    .option("inferSchema", "true")
    //.csv("people.csv")
    .csv("gs://dataproc-a618ac02-dd7b-47aa-8b10-043d7c2edef8-asia-east1/people.csv")
    
    df.printSchema()
    df.show()
    
   }
}
