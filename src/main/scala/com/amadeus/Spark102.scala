package com.amadeus

import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.log4j.{Level, Logger}

/* high level api data frame*/

object Spark102 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SparkHighLevel")
      .master("local[4]")
      .getOrCreate()

    val dataFrame = spark.read.format("csv")
      .option("header", value = true) // we are telling to spark that our first line refers to header row.
      .option("inferSchema", value = true) // while spark is reading the data, we are saying that as spark you should take in account of data types
      .load("C:\\Users\\eyucel\\Documents\\hadoop\\spark\\simple_data.csv")

    dataFrame.show(5) // we can specify the number of rows in which we would like to read it from file

    dataFrame.printSchema()

    // salaries which are greater than 4000
    dataFrame.filter(dataFrame.col("aylik_gelir").gt(4000)).show()

    dataFrame.filter(dataFrame.col("meslek").isNull).show()

    // creating a new column and removing the existing one
    val trimmedData = dataFrame.withColumn("meslek_filled", F.when(dataFrame.col("meslek").isNull, value = "Others")
      .otherwise(dataFrame.col("meslek")))
      .drop("meslek")

    // group by of data frame
    trimmedData.groupBy("meslek_filled")
      .agg(F.mean("aylik_gelir").as("avg_salary"))
      .orderBy(F.desc("avg_salary"))
        .show()

    // the most earned city
    dataFrame.groupBy("sehir")
      .agg(F.sum("aylik_gelir").as("monthly_income"))
      .orderBy(F.desc("monthly_income"))
      .show(1)
  }
}
