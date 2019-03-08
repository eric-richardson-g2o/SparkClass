package com.icct.ais.sparkbootcamp

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author ${user.name}
  */
object SparkRDDSolution {

  def foo(x: Array[String]) = x.foldLeft("")((a, b) => a + b)

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("RDD Application").setMaster("local")
    val sc = new SparkContext(conf)

    val dataFile = "/data/syntheticdata.csv"
    val lines = sc.textFile(dataFile).sample(false, 0.05D, 12345678L)

    val words= lines.flatMap(line => line.split("\\s+"))
    val wordPairs=words.map(word => (word,1L))
    val wordCount=wordPairs.reduceByKey((sum,value)=>sum+value)
    val repartitioned = wordCount.repartition(3)
    System.out.println
    System.out.println
    System.out.println("uniqueWords: " + repartitioned.count)
    System.out.println
    System.out.println

    val pairs = lines.map(s => (s, 1))
    val counts = pairs.reduceByKey((a,b) => a + b)
    val result = counts.collect
    val uniqueLines = result.size
    val totalLines = lines.count
    //lines.unpersist();
    System.out.println
    System.out.println
    System.out.println("TotalLines: " + totalLines + ", uniqueLines: " + uniqueLines)
    System.out.println
    System.out.println



    sc.stop
  }

}
