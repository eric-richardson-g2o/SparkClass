package com.icct.ais.sparkbootcamp

import org.apache.spark.sql._

import org.apache.spark.sql.Row


import collection.JavaConverters._

object SparkSqlSolution {
  def main(args: Array[String]): Unit = {
    val spark = getSparkSession
    val data=loadData(spark)
    getDistinctClaimIds(data)
    getTop10Diagnosis(data)
    data.unpersist()
    spark.stop()
  }

  def getSparkSession(): SparkSession = {
    val spark = SparkSession.builder.master("local").appName("SQL App").getOrCreate
    spark
  }

  def loadData(spark: SparkSession): Dataset[Row] = {
    val dataFile = "/data/syntheticdata.csv"
    val data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(dataFile).sample(0.05D, 1234567890L).cache
    data
  }

  def getDistinctClaimIds(data: Dataset[Row]): Long = {
    val clmids: Dataset[Row] = data.select("CLM_ID").distinct
    val uniqueIds: Long = clmids.count
    System.out.println()
    System.out.println()
    System.out.println(uniqueIds)
    System.out.println()
    System.out.println()
    uniqueIds
  }

  def getTop10Diagnosis(data: Dataset[Row]): List[Row] = {
    val groupedIcd9 = data.groupBy("ICD9_DGNS_CD_1").count.sort(functions.desc("count")).limit(10).cache
    val result = groupedIcd9.collectAsList.asScala.toList
    System.out.println()
    System.out.println()
    data.printSchema()
    System.out.println()
    System.out.println()
    groupedIcd9.show()
    System.out.println()
    System.out.println()
    groupedIcd9.unpersist
    result
  }
}

