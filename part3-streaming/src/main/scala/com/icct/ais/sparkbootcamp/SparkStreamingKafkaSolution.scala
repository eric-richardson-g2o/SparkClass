package com.icct.ais.sparkbootcamp

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

/**
  * @author ${user.name}
  */
object SparkStreamingKafkaSolution {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaRocks")

    val spark = SparkSession.builder.config(conf).getOrCreate
    val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "consoleTopic").load
    import spark.implicits._
    val query=df.selectExpr( "CAST(value AS STRING)").as[String]
  ///  val do the streaming aggregation
    val aggregation=query.flatMap(_.split(" ")).groupBy("value").count()
    val stream=aggregation.writeStream.outputMode("complete").format("console").start()
    stream.awaitTermination()
  }
}
