package com.icct.ais.sparkbootcamp

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author ${user.name}
  */
object SparkWordCountSolution {

  def foo(x: Array[String]) = x.foldLeft("")((a, b) => a + b)

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("RDD Application").setMaster("local")
    val sc = new SparkContext(conf)

    val dataFile = "/data/syntheticdata.csv"
    val lines = sc.textFile(dataFile).sample(false, 0.05D, 12345678L)
    //wordcount
    val words= lines.flatMap(line => line.split("\\s+"))
    val wordPairs=words.map(word => (word,1L))
    val wordCount=wordPairs.reduceByKey((sum,value)=>sum+value)
    System.out.println
    System.out.println
    System.out.println("uniqueWords: " + wordCount.count)
    System.out.println
    System.out.println


    sc.stop
  }

}
