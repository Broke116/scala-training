package com.amadeus

import com.amadeus.Spark101.sc
import org.apache.spark.sql.SparkSession
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
  }
}
