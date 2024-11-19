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
      // Define the schema for the JSON files
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

      // Initialize an empty DataFrame for merging
      var mergedData: DataFrame = spark.emptyDataFrame

      // Loop through the years 2002 to 2024
      for (year <- 2002 to 2024) {
        val filePath = s"$inputDir/nvdcve-1.1-$year.json"
        println(s"Reading file: $filePath")

        // Read the JSON file with the defined schema
        val jsonDF = spark.read.option("multiLine", true).schema(schema).json(filePath)

        // Extract the required fields with correct handling of nested arrays
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

        // Merge the extracted data into the main DataFrame
        mergedData = if (mergedData.isEmpty) extractedData else mergedData.union(extractedData)
      }

      // Display the merged DataFrame
      println("Merged Dataset:")
      mergedData.show(truncate = false)

      // Save the merged dataset as a single JSON file
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

// Define the input directory (Databricks paths start with "dbfs:/")
val inputDir = "dbfs:/FileStore/tables"

// Execute the exercise
JSON.execute(spark, inputDir)

// Stop Spark session if needed
// spark.stop()

