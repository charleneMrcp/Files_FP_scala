// Databricks notebook source
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

// COMMAND ----------

val csvFile= "/FileStore/tables/stackoverflow.csv"

// COMMAND ----------

val schema = new StructType()
  .add("postTypeId", IntegerType, nullable = true)
  .add("id", IntegerType, nullable = true)
  .add("acceptedAnswer", StringType, nullable = true)
  .add("parentId", IntegerType, nullable = true)
  .add("score", IntegerType, nullable = true)
  .add("tag", StringType, nullable = true)

// COMMAND ----------

val df = spark.read
  .option("header", "false")
  .schema(schema)
  .csv(csvFile)
  .drop("acceptedAnswer")

println(s"\nCount of records in CSV file: ${df.count()}")
df.printSchema()
df.show(5) 


