package com.icct.ais.sparkbootcamp

import org.apache.spark.sql._
import org.junit.Assert._
import org.junit._


@Test
class SparkSqlExerciseTest {

  @Test
  def testOK() = {
    val spark: SparkSession = SparkSqlExercise.getSparkSession
    val data= SparkSqlExercise.loadData(spark)
    val claimIdCount = SparkSqlExercise.getDistinctClaimIds(data)
    assertEquals(118790, claimIdCount)

    val topDiagnosis:List[Row]=SparkSqlExercise.getTop10Diagnosis(data)
    assertEquals(10,topDiagnosis.size)
    assertEquals("4019",topDiagnosis(0).get(0))
    assertEquals(2880L,topDiagnosis(0).get(1))

    assertEquals("4011",topDiagnosis(1).get(0))
    assertEquals(2804L,topDiagnosis(1).get(1))

    assertEquals("78659",topDiagnosis(2).get(0))
    assertEquals(747L,topDiagnosis(2).get(1))

    assertEquals("78651",topDiagnosis(3).get(0))
    assertEquals(725L,topDiagnosis(3).get(1))

    assertEquals("78650",topDiagnosis(4).get(0))
    assertEquals(659L,topDiagnosis(4).get(1))

    assertEquals("7802",topDiagnosis(5).get(0))
    assertEquals(577L,topDiagnosis(5).get(1))

    assertEquals("2724",topDiagnosis(6).get(0))
    assertEquals(543L,topDiagnosis(6).get(1))

    assertEquals("2721",topDiagnosis(7).get(0))
    assertEquals(533L,topDiagnosis(7).get(1))

    assertEquals("2720",topDiagnosis(8).get(0))
    assertEquals(529L,topDiagnosis(8).get(1))

    assertEquals("2723",topDiagnosis(9).get(0))
    assertEquals(529L,topDiagnosis(9).get(1))
    spark.stop()

  }


}


