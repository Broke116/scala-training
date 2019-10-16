package com.amadeus

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}

object Spark101 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("SparkExample")
    .master("local[4]")
    .getOrCreate()

  println(spark.version)

  var sc = spark.sparkContext

  // reading from a sequence.
  val numbers = sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8))
  numbers.take(2).foreach(println) // take is an action

  // creating an rdd from a file
  val textFile = sc.textFile("C:\\Users\\eyucel\\Documents\\hadoop\\spark\\lipsum.txt")
  textFile.first().foreach(print)
}
