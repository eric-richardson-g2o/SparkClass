package com.icct.ais.sparkbootcamp

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author ${user.name}
  */
object SparkWordCountExercise {

  def foo(x: Array[String]) = x.foldLeft("")((a, b) => a + b)

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("RDD Application").setMaster("local")
    val sc = new SparkContext(conf)

    val dataFile = "/data/syntheticdata.csv"
    val lines = sc.textFile(dataFile).sample(false, 0.05D, 12345678L)

    //make words out of line
    // Hint: use the java.lang.String.split(String regex) method
    // https://docs.oracle.com/javase/8/docs/api/java/lang/String.html#split-java.lang.String-
    val words= lines
    //.yourcode here

    // Make a pair of (word, count)
    val wordPairs=words
    //.yourcodehere

    // now get the counts by word
    val wordCount=wordPairs
      //.yourcodehere
    System.out.println
    System.out.println
    System.out.println("uniqueWords: " + wordCount.count)
    System.out.println
    System.out.println


    sc.stop
  }

}
