package com.icct.ais.sparkbootcamp

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

/**
  * @author ${user.name}
  */
object SparkStreamingKafkaExercise {
  /**
    * Subscribe to a Kafka topic
    * Count up the words that show up in the
    * @param args
    */
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaRocks")
    val spark = SparkSession.builder.config(conf).getOrCreate

    //Spark can read a stream...
    //format name is kafka
    //needs the two options: kafka.bootstrap.servers, subscribe
    // values for options are as you used for console consumer
    val df = spark.yourcodehere.load
    import spark.implicits._
    val query=df.selectExpr( "CAST(value AS STRING)").as[String]
  //  val do the streaming aggregation
    val aggregation=query.yourcodehere
    //experiment with different output modes, complete, update, append
    val stream=aggregation.writeStream.outputMode(OutputMode.Append()).format("console").start()
    stream.awaitTermination()
  }
}
