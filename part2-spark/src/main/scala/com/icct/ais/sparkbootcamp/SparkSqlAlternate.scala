package com.icct.ais.sparkbootcamp

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

object SparkSqlAlternate {
  def main(args: Array[String]): Unit = {


    val dataFile = "/data/syntheticdata.csv"
    val spark = SparkSession.builder.master("local").appName("SQL App").getOrCreate
    val data = spark.read.format("csv").option("header", "true").load(dataFile).sample(0.05D, 1234567890L).cache
    data.createOrReplaceTempView("claims")
    val clmids = spark.sql("select distinct CLM_ID from claims limit 10").cache
    val groupedIcd9 = spark.sql("select ICD9_DGNS_CD_1, count(ICD9_DGNS_CD_1) as count from claims group by ICD9_DGNS_CD_1 order by count desc limit 10").cache
    clmids.count
    groupedIcd9.count
    System.out.println()
    System.out.println()
    data.printSchema()
    System.out.println()
    System.out.println()
    clmids.show()
    System.out.println()
    System.out.println()
    groupedIcd9.show()
  }
}
