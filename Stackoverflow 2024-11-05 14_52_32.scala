// Databricks notebook source
import org.apache.log4j.{Level, Logger}

// COMMAND ----------

val csvFile= "/FileStore/tables/stackoverflow.csv"
val df = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .csv(csvFile)

// COMMAND ----------

val lineCount = df.count()

// COMMAND ----------

df.printSchema()

// COMMAND ----------

df.show()
