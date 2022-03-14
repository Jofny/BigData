// Databricks notebook source
//Ćw1
import org.apache.spark.sql.types.{IntegerType,StringType,StructType,StructField}

val schemaActors = StructType(Array(StructField("imdb_title_id", StringType, true), StructField("ordering", IntegerType, true), StructField("imdb_name_id", StringType, true), StructField("category", StringType, true), StructField("job", StringType, true), StructField("characters", StringType, true)))

val filePath = "dbfs:/FileStore/tables/data/actors.csv"
val actorsDf = spark.read.format("csv")
            .option("header","true")
            .schema(schemaActors)
            .load(filePath)
display(actorsDf)

// COMMAND ----------

//Ćw2
actorsDf.write.mode("overwrite").json(path=filePath)

val filePathJson = "dbfs:/FileStore/tables/data/actors.json"
val jsonFile = spark.read.json(filePathJson)

print(jsonFile.schema)
jsonFile.show()

// COMMAND ----------


//Ćw.4
spark.read.format("csv").option("mode","PERMISSIVE").schema(schemaActors).load(filePath)
spark.read.format("csv").option("mode","DROPMALFORMED").schema(schemaActors).load(filePath)
spark.read.format("csv").option("mode","FAILFAST").schema(schemaActors).load(filePath)
spark.read.format("csv").option("mode","FAILFAST").schema(schemaActors).option("badRecordsPath", "dbfs:/FileStore/badRecordsPath").load(filePath)

// COMMAND ----------

//Ćw.5

val outputFileParquet = "dbfs:/FileStore/tables/data/actors.parquet"
actorsDf.write.json(path=outputFileParquet)
spark.read.schema(schemaActors).option("enforceSchema",true).parquet(path=outputFileParquet)

