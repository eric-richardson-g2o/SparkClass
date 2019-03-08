package com.icct.ais.sparkbootcamp

import org.apache.spark.sql._

import org.apache.spark.sql.Row

import org.apache.spark.sql.functions.desc

import collection.JavaConverters._


object SparkSqlExercise {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = getSparkSession
    val data= loadData(spark)
    getDistinctClaimIds(data)
    getTop10Diagnosis(data)
    spark.stop()
  }

  /**
    * Make the spark session.  Normally you wouldn't do this in a separate method, but for testing purposes...
    *
    * @return the Spark session.
    */
  def getSparkSession():SparkSession={
    //remember how to create a Spark Session, make it local and give it a name
    val spark: SparkSession = SparkSession.builder.
    // yourcodehere.
    getOrCreate()
    spark
  }

  /**
    * Make a Dataset from the data in the file.  Sample at 0.05% to make the Dataset managable on a laptop and seed the sample with 1234567890L to make it testable
    * @param spark
    * @return
    */
  def loadData (spark:SparkSession):Dataset[Row]={
    // Where did you put the data file? it is in the git project at <project root>/src/main/resources/syntheticdata.csv

    val dataFile = "/data/syntheticdata.csv"
    //use spark to read the csv, use the header row to define the schema
    //see the read method here https://spark.apache.org/docs/2.3.2/api/scala/index.html#org.apache.spark.sql.SparkSession
    // see the csv method here https://spark.apache.org/docs/2.3.2/api/scala/index.html#org.apache.spark.sql.DataFrameReader
    val data :Dataset[Row] = spark.read
      //.yourcodehere
      .load(dataFile).sample(0.05D,1234567890L).cache
    data
  }

  /**
    *
    *  How manny
    *
    * @param data
    * @return the number of distinct claim ids in the file
    */
  def getDistinctClaimIds(data:Dataset[Row]):Long= {
    val clmids = data
      //.yourcodehere
    clmids.count
  }

  /**
    *
    * Find the top ten primary diagnosis with a count of each
    *
    *
    * @param data
    * @return a list containing the top ten primary diagnosis ICD9 codes
    */
  def getTop10Diagnosis(data:Dataset[Row]): List[Row] ={
    // hint how would you count the occurances in sql?
    val groupedIcd9 = data
      //.yourcodehere.limit(10)
    val result = groupedIcd9.collectAsList().asScala.toList
    result
  }


}
