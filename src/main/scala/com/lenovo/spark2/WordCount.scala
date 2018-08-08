package com.lenovo.spark2

import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {

    LoggerLevels.setStreamingLogLevels()
    val spark = SparkSession.builder()
      .master("local")
      .appName("WordCount")
      //.config("spark.some.config.option","some-value")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._
    spark.read.text("c://words.txt").as[String].flatMap(value => value.split(" ")).groupBy($"value" as "word").agg(count("*") as "counts")
    spark.stop()
  }
}
