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

  println(" ")

  var sc = spark.sparkContext

  // reading from a sequence.
  val numbers = sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8))
  numbers.take(2).foreach(println) // take is an action

  println(" ")

  // creating an rdd from a file
  val textFile = sc.textFile("C:\\Users\\eyucel\\Documents\\hadoop\\spark\\simple_data.csv")
  textFile.take(5)
    .foreach(println)

  println(" ")

  // filter operation
  textFile.filter(x => !x.contains("sirano"))
    .foreach(println)

  println(" ")

  // converting to uppercase char
  textFile.map(x => x.toUpperCase)
    .take(5)
    .foreach(println)
}
