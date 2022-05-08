package lab7

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._

object SparkProject {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("Moja-applikacja")
      .getOrCreate()

    val filePath = args(0)
    var moviesDf = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(filePath)

    moviesDf = moviesDf.withColumn("unix_timestamp", unix_timestamp().as("unix_timestamp"))
      .withColumn("year_int", col("year").cast(IntegerType))
      .withColumn("how_long_ago", lit(2022) - col("year_int"))

    moviesDf = moviesDf.withColumn("budget_num", regexp_extract(col("budget"), "\\d+", 0))
    moviesDf = moviesDf.na.drop("any")

    moviesDf.show()
  }

}