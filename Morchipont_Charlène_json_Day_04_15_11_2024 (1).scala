// Databricks notebook source
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

// COMMAND ----------

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object JSON {
  def execute(spark: SparkSession, inputDir: String): Unit = {
    try {
      val schema = StructType(Array(
        StructField("cve", StructType(Array(
          StructField("CVE_data_meta", StructType(Array(
            StructField("ID", StringType, nullable = true)
          ))),
          StructField("description", StructType(Array(
            StructField("description_data", ArrayType(StructType(Array(
              StructField("lang", StringType, nullable = true),
              StructField("value", StringType, nullable = true)
            ))))
          ))),
          StructField("problemtype", StructType(Array(
            StructField("problemtype_data", ArrayType(StructType(Array(
              StructField("description", ArrayType(StructType(Array(
                StructField("lang", StringType, nullable = true),
                StructField("value", StringType, nullable = true)
              ))))
            ))))
          )))
        ))),
        StructField("impact", StructType(Array(
          StructField("baseMetricV2", StructType(Array(
            StructField("cvssV2", StructType(Array(
              StructField("baseScore", DoubleType, nullable = true),
              StructField("severity", StringType, nullable = true)
            ))),
            StructField("exploitabilityScore", DoubleType, nullable = true),
            StructField("impactScore", DoubleType, nullable = true)
          )))
        ))),
        StructField("publishedDate", StringType, nullable = true),
        StructField("lastModifiedDate", StringType, nullable = true)
      ))

      var mergedData: DataFrame = spark.emptyDataFrame

      for (year <- 2003 to 2024) {
        val filePath = s"$inputDir/nvdcve_1_1_$year.json"
        println(s"Reading file: $filePath")

        val jsonDF = spark.read.option("multiLine", true).schema(schema).json(filePath)

        val extractedData = jsonDF
          .withColumn("Description", col("cve.description.description_data").getItem(0).getField("value"))
          .withColumn("ProblemType", explode(col("cve.problemtype.problemtype_data")))
          .withColumn("ProblemTypeValue", col("ProblemType.description").getItem(0).getField("value"))
          .select(
            col("cve.CVE_data_meta.ID").as("ID"),
            col("Description"),
            col("ProblemTypeValue").as("ProblemType"),
            col("impact.baseMetricV2.cvssV2.baseScore").as("BaseScore"),
            col("impact.baseMetricV2.cvssV2.severity").as("Severity"),
            col("impact.baseMetricV2.exploitabilityScore").as("ExploitabilityScore"),
            col("impact.baseMetricV2.impactScore").as("ImpactScore"),
            col("publishedDate").as("PublishedDate"),
            col("lastModifiedDate").as("LastModifiedDate")
          )

        mergedData = if (mergedData.isEmpty) extractedData else mergedData.union(extractedData)
      }

      println("Merged Dataset:")
      mergedData.show(truncate = false)

      val outputPath = s"$inputDir/merged_cve_data"
      mergedData.write.mode("overwrite").json(outputPath)
      println(s"Merged data saved to: $outputPath")

    } catch {
      case e: Exception => println(s"Error while executing Exercise 3: ${e.getMessage}")
    }
  }
}

// Initialize SparkSession
val spark = SparkSession.builder()
  .appName("CVE Data Extraction")
  .getOrCreate()

val inputDir = "/FileStore/tables"

JSON.execute(spark, inputDir)

// spark.stop()

