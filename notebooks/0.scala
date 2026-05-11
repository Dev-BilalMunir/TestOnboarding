// Databricks notebook source
// MAGIC %md
// MAGIC ## NeoGenomics — Lab data from NeoGenomics

// COMMAND ----------

// MAGIC %run "./common_v2"

// COMMAND ----------

dbutils.widgets.text("payload", """[]""", "job payload")
dbutils.widgets.text("output-bucket", "mdv-tmp", "output-bucket")
dbutils.widgets.text("datanator-id", "UNK", "datanator-id")

// COMMAND ----------

val payload = dbutils.widgets.get("payload")
val outputBucket = dbutils.widgets.get("output-bucket")
val datanatorId = dbutils.widgets.get("datanator-id").toInt

// COMMAND ----------

// MAGIC %md ### Variables

// COMMAND ----------

val version = "v1.0.0"
val outputPath = s"s3://${outputBucket}/intermediary/staged/${datanatorId}/${version}/"

// COMMAND ----------

// MAGIC %md ### Payload

// COMMAND ----------

val payloadDF = extractPayloadCompressed(payload)

// COMMAND ----------

display(payloadDF)

// COMMAND ----------

// MAGIC %md ### Run

// COMMAND ----------

val reader = spark
  .read
  .option("header","true")
  .option("charset", "UTF8")
  .option("delimiter","tab")
  .option("mode", "DROPMALFORMED")
  .option("ignoreTrailingWhiteSpace", "true")
  .option("ignoreLeadingWhiteSpace", "true")
  .schema(schema_0)

// COMMAND ----------

val redactor = new LabRedactor(
  dateOfBirthColumnName = "date_of_birth",
  patientStateColumnName = "",
  patientZipCodeColumnName = "",
  icdColumnNames = List(),
  datesColumnNames = List()
)

// COMMAND ----------

val df = unionCsvPayload(reader, payloadDF)
  .repartition(500)
  .transform(redactor.withRedactions())

// COMMAND ----------

df.printSchema()

// COMMAND ----------

display(df.limit(100))

// COMMAND ----------

writeStandardParquetOutput(df, outputPath)