package com.amadeus

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}

/* RDD */

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

  // converting to uppercase char. it directly maps the each item one by one. it does no change the structure of the underlying structure.
  textFile.map(_.toUpperCase)
    .take(5)
    .foreach(println)

  println(" ")

  // flatMap example. it changes the underlying structure of data. we cant do this using pure map function
  // if line contains blank character, flatMap helps us to remove those characters.
  // it applies operation on each single character
  textFile.flatMap(_.split(" "))
    .foreach(println)
}
