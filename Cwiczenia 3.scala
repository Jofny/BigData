// Databricks notebook source
// MAGIC %md Names.csv 
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę w której wyliczysz wzrost w stopach (feet)
// MAGIC * Odpowiedz na pytanie jakie jest najpopularniesze imię?
// MAGIC * Dodaj kolumnę i policz wiek aktorów 
// MAGIC * Usuń kolumny (bio, death_details)
// MAGIC * Zmień nazwy kolumn - dodaj kapitalizaję i usuń _
// MAGIC * Posortuj dataframe po imieniu rosnąco

// COMMAND ----------

import org.apache.spark.sql.functions._
import java.util.Calendar


val filePath = "dbfs:/FileStore/tables/data/names.csv"
var namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

namesDf = namesDf.withColumn("unix_timestamp",unix_timestamp().as("unix_timestamp"))
          .withColumn("height_in_feet",col("height")*0.3937007874)
          .withColumn("to_date",to_date(col("date_of_birth")).as("to_date"))
          .drop("bio")
          .drop("death_details")

for( i <- 0 to namesDf.columns.size - 1) {
    namesDf = namesDf.withColumnRenamed(namesDf.columns(i), namesDf.columns(i).toUpperCase.replaceAll ("_", ""))
}
display(namesDf)

// COMMAND ----------

val namesDf2 = namesDf.withColumn("extractedName", split(col("BIRTHNAME")," ").getItem(0))
               .orderBy($"birth_name")

namesDf2.groupBy("extractedName").count().orderBy($"count".desc).first()
//John, 1052

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import months_between, to_date, floor
// MAGIC from datetime import date
// MAGIC from pyspark.sql.functions import lit
// MAGIC filePath = "dbfs:/FileStore/tables/data/names.csv"
// MAGIC namesDf = spark.read.format("csv").option("header","true").option("inferSchema","true").load(filePath)
// MAGIC namesDf.withColumn('age', floor(months_between( lit(date.today()),to_date('date_of_birth','dd.mm.yyyy' ))/12)).display()

// COMMAND ----------

// MAGIC %md Movies.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę która wylicza ile lat upłynęło od publikacji filmu
// MAGIC * Dodaj kolumnę która pokaże budżet filmu jako wartość numeryczną, (trzeba usunac znaki walut)
// MAGIC * Usuń wiersze z dataframe gdzie wartości są null

// COMMAND ----------

import org.apache.spark.sql.types.IntegerType
val filePath = "dbfs:/FileStore/tables/data/movies.csv"
var moviesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

moviesDf = moviesDf.withColumn("unix_timestamp",unix_timestamp().as("unix_timestamp"))
                  .withColumn("year_int",col("year").cast(IntegerType))
                  .withColumn("how_long_ago",lit(2022) - col("year_int"))

moviesDf = moviesDf.withColumn("budget_num", regexp_extract($"budget", "\\d+", 0))
moviesDf = moviesDf.na.drop("any")

display(moviesDf)

// COMMAND ----------

// MAGIC %md ratings.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dla każdego z poniższych wyliczeń nie bierz pod uwagę `nulls` 
// MAGIC * Dodaj nowe kolumny i policz mean i median dla wartości głosów (1 d 10)
// MAGIC * Dla każdej wartości mean i median policz jaka jest różnica między weighted_average_vote
// MAGIC * Kto daje lepsze oceny chłopcy czy dziewczyny dla całego setu
// MAGIC * Dla jednej z kolumn zmień typ danych do `long` 

// COMMAND ----------

import org.apache.spark.sql.types.LongType

val filePath = "dbfs:/FileStore/tables/data/ratings.csv"
var ratingsDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)



ratingsDf = ratingsDf.withColumn("unix_timestamp",unix_timestamp().as("unix_timestamp"))
                     .na.drop("any")
                     .withColumn("mean",(col("votes_10")*10 + col("votes_9")*9 + col("votes_8")*8 + col("votes_7")*7 + col("votes_6")*6 + col("votes_5")*5 + col("votes_4")*4 + col("votes_3")*3 + col("votes_2")*2 + col("votes_1"))/col("total_votes"))
                     //.withColumn("median",)
                     .withColumn("mean_diff", col("weighted_average_vote")-col("mean"))
                     .withColumn("are_bois_kinder", col("males_allages_avg_vote") > col("females_allages_avg_vote"))
                     .withColumn("weighted_average_vote_long",col("weighted_average_vote").cast(LongType))
display(ratingsDf)

// COMMAND ----------

val names = List("votes_1","votes_2","votes_3","votes_4","votes_5","votes_6","votes_7","votes_8","votes_9","votes_10")
var i = List()
for( i <- names){
  var score = ratingsDf.stat.approxQuantile(i, Array(0.5), 0.25)
  var string = score.mkString(",")
  println(i + " " + string)
}
