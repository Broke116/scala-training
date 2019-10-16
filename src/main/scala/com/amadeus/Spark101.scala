package com.amadeus

object Spark101 extends App {
  // hello world
  println("Hello first Scala")

  // maths
  val a = 12
  val b = 13
  val c = a + b

  print(s"I am c: $c")
  println(" ")

  // string ops
  val sentence = "Ali ata bak"

  for (word <- sentence) {
    print(word)
  }

  println(" ")
  val sentence2 = "Emel'in fisi"

  val combinedSentences = sentence.concat(sentence2)
  println(combinedSentences)

  println(" ")

  // arrays. even though we define array as val, array is mutable.
  val array1 = Array(1, 2, 3, 4, 5)

  for(i <- array1) {
    println(i)
  }

  array1(0) = 11
  array1.foreach(println) // iterating the elements of array by passing println function

  // lists. list is an immutable collection so that it s not possible to change its value.
  val myList1 = List(10, 20, 30, 40, 50, 60)
  myList1.foreach(println)
}
