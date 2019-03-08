package com.icct.ais.sparkbootcamp

import org.apache.spark.{SparkConf, SparkContext}

/**
  * This class is to show how to view the Spark DAG and the application master Web UI
  */
object SparkRDDDagExample {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("RDD Application").setMaster("local")
    val sc = new SparkContext(conf)

    val dataFile = "/data/syntheticdata.csv"
    val lines = sc.textFile(dataFile).sample(false, 0.05D, 12345678L)
    val words = lines.flatMap(line => line.split("\\s+"))
    val wordPairs = words.map(word => (word, 1L))
    val wordCount = wordPairs.reduceByKey((sum, value) => sum + value)
    val repartitioned = wordCount.repartition(3)
    val result = repartitioned.collect

    val uniqueWords = result.size
    val totalWords = words.count
    //lines.unpersist();
    System.out.println
    System.out.println
    System.out.println("TotalWords: " + totalWords + ", uniqueWords: " + uniqueWords)
    System.out.println
    System.out.println

    sc.defaultMinPartitions
    lines.getNumPartitions
    sc.stop
  }
}