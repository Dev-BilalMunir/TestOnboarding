// Databricks notebook source
// MAGIC %md
// MAGIC ## Acme Health — Monthly medical claims data from Acme Health

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
  .option("delimiter",",")
  .option("mode", "DROPMALFORMED")
  .option("ignoreTrailingWhiteSpace", "true")
  .option("ignoreLeadingWhiteSpace", "true")
  .schema(schema_1778820233)

// COMMAND ----------

val redactor = new ClaimsRedactor(
  dateOfBirthColumnName = "date_of_birth",
  patientStateColumnName = "address_us_state",
  patientZipCodeColumnName = "address_us_zip",
  icdColumnNames = List("icd_code_1", "icd_code_2"),
  datesColumnNames = List("date_of_service", "admission_date", "discharge_date")
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