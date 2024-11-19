// Databricks notebook source
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

// COMMAND ----------

val schema = new StructType()
  .add("postTypeId", IntegerType, nullable = true)
  .add("id", IntegerType, nullable = true)
  .add("acceptedAnswer", StringType, nullable = true)
  .add("parentId", IntegerType, nullable = true)
  .add("score", IntegerType, nullable = true)
  .add("tag", StringType, nullable = true)

// COMMAND ----------

val csvDataFile = "/FileStore/tables/stackoverflow.csv/stackoverflow.csv"

// COMMAND ----------

val df = spark.read
      .option("header", "false")
      .schema(schema)
      .csv(csvDataFile)
      .drop("acceptedAnswer")

// COMMAND ----------

println(s"\nCount of records in CSV file: ${df.count()}")

// COMMAND ----------

df.printSchema()

// COMMAND ----------

df.show(5)

// COMMAND ----------

val highScorePosts = df.filter(col("score") > 20)
highScorePosts.show(5)

// import spark.implicits._
// val highScorePosts = df.filter($"score" > 20)
// highScorePosts.show(5)

// COMMAND ----------

df.createOrReplaceTempView("stackoverflow")

// COMMAND ----------

val top5Scores = spark.sql("""
SELECT id, tag, score
FROM stackoverflow
ORDER BY score DESC
LIMIT 5
""")
top5Scores.show()

// COMMAND ----------

val top5ScoresWithTag = spark.sql("""
SELECT id, tag, score
FROM stackoverflow
WHERE tag IS NOT NULL
ORDER BY score DESC
LIMIT 5
""")
top5ScoresWithTag.show()

// COMMAND ----------

val popularTags = spark.sql("""
SELECT tag, COUNT(*) as frequency
FROM stackoverflow
WHERE tag IS NOT NULL
GROUP BY tag
ORDER BY frequency DESC
LIMIT 10
""")
top5ScoresWithTag.show()
