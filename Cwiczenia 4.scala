// Databricks notebook source
// MAGIC %md 
// MAGIC Wykożystaj dane z bazy 'bidevtestserver.database.windows.net'
// MAGIC ||
// MAGIC |--|
// MAGIC |SalesLT.Customer|
// MAGIC |SalesLT.ProductModel|
// MAGIC |SalesLT.vProductModelCatalogDescription|
// MAGIC |SalesLT.ProductDescription|
// MAGIC |SalesLT.Product|
// MAGIC |SalesLT.ProductModelProductDescription|
// MAGIC |SalesLT.vProductAndDescription|
// MAGIC |SalesLT.ProductCategory|
// MAGIC |SalesLT.vGetAllCategories|
// MAGIC |SalesLT.Address|
// MAGIC |SalesLT.CustomerAddress|
// MAGIC |SalesLT.SalesOrderDetail|
// MAGIC |SalesLT.SalesOrderHeader|

// COMMAND ----------

//INFORMATION_SCHEMA.TABLES

val jdbcHostname = "bidevtestserver.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "testdb"

val table = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query","SELECT * FROM INFORMATION_SCHEMA.TABLES")
  .load()
display(table)

// COMMAND ----------

// MAGIC %md
// MAGIC 1. Pobierz wszystkie tabele z schematu SalesLt i zapisz lokalnie bez modyfikacji w formacie delta

// COMMAND ----------

val SalesLT = table.where("TABLE_SCHEMA == 'SalesLT'")

val names=SalesLT.select("TABLE_NAME").as[String].collect.toList

var i = List()
for( i <- names){
  val tab = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query",s"SELECT * FROM SalesLT.$i")
  .load()
  
  tab.write.format("delta").mode("overwrite").saveAsTable(i)
  
}

// COMMAND ----------

// MAGIC %md
// MAGIC  Uzycie Nulls, fill, drop, replace, i agg
// MAGIC  * W każdej z tabel sprawdź ile jest nulls w rzędach i kolumnach
// MAGIC  * Użyj funkcji fill żeby dodać wartości nie występujące w kolumnach dla wszystkich tabel z null
// MAGIC  * Użyj funkcji drop żeby usunąć nulle, 
// MAGIC  * wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]
// MAGIC  * Użyj tabeli [SalesLT].[Product] i pogrupuj według ProductModelId, Color i ProductCategoryID i wylicz 3 wybrane funkcje agg() 
// MAGIC    - Użyj conajmniej dwóch overloded funkcji agregujących np z (Map)

// COMMAND ----------

import org.apache.spark.sql.functions.{col, when, count}
import org.apache.spark.sql.Column

def countCols(columns:Array[String]):Array[Column]={
    columns.map(c=>{
      count(when(col(c).isNull,c)).alias(c)
    })
}

// COMMAND ----------

var i = List()
val names_lower=names.map(x => x.toLowerCase())
for( i <- names_lower){
  val filePath = s"dbfs:/user/hive/warehouse/$i"
  val df = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)
print(i)
df.select(countCols(df.columns):_*).show()
}

// COMMAND ----------

for( i <- names_lower){
  val filePath = s"dbfs:/user/hive/warehouse/$i"
  val df = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)
  val col = df.columns
  val df0 = df.na.fill("0", col)  
  display(df0)
}


// COMMAND ----------

val filePath = s"dbfs:/user/hive/warehouse/customer"
var df = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)
val col = df.columns
df = df.na.fill("0", col)
display(df)

// COMMAND ----------

for( i <- names_lower){
  val filePath = s"dbfs:/user/hive/warehouse/$i"
  val df = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)
  val col = df.columns
  val dfDropped = df.na.drop("any")  
  
}

// COMMAND ----------

val filePath = s"dbfs:/user/hive/warehouse/customer"
var df = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)
val col = df.columns
df = df.na.drop("any")
display(df)

// COMMAND ----------

val filePath = s"dbfs:/user/hive/warehouse/salesorderheader"
val dfSOH = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)
display(dfSOH)

// COMMAND ----------

import org.apache.spark.sql.functions._

dfSOH.select(avg($"Freight")).show()
dfSOH.select(variance($"Freight")).show()
dfSOH.select(skewness($"Freight")).show()
dfSOH.select(avg($"TaxAmt")).show()
dfSOH.select(variance($"TaxAmt")).show()
dfSOH.select(skewness($"TaxAmt")).show()

// COMMAND ----------

 val filePath = s"dbfs:/user/hive/warehouse/product"
 val dfPROD = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(dfPROD)

// COMMAND ----------

display(dfPROD.groupBy("ProductModelId").avg())

// COMMAND ----------

display(dfPROD.groupBy("ProductCategoryID").count())

// COMMAND ----------

display(dfPROD.groupBy("ProductModelId").mean())

// COMMAND ----------

display(dfPROD.groupBy($"ProductModelId").agg(Map("StandardCost" -> "mean","ListPrice" -> "sum")))

// COMMAND ----------

import org.apache.spark.sql.types._
val filePath = "dbfs:/FileStore/tables/data/names.csv"
var namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath) 

val feet = udf((m: Double) => m * 3.2808)
val mean = udf((a: Integer, b:Integer) => (a+b)/2 )
val UpperWithFloor = udf((s: String) => s.toUpperCase.replaceAll (" ", "_") )

val dfUDF = namesDf.select(feet($"height") as "feet",
            mean($"spouses",$"divorces") as "mean",
            UpperWithFloor($"name") as "name")
display(dfUDF)

// COMMAND ----------

var jsonNotHandsome = spark.read.option("multiline", "true")
  .json("/FileStore/tables/brzydki.json")


val Json3 = jsonNotHandsome.selectExpr("features")
