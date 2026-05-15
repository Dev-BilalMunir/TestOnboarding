// Databricks notebook source
// MAGIC %md Notebook: stage/common_v2

// COMMAND ----------

spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
spark.conf.set("spark.sql.csv.parser.columnPruning.enabled", false)

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.util.Base64
import java.util.zip._
import java.nio.charset.StandardCharsets
import java.io.{ByteArrayOutputStream, ByteArrayInputStream}

// import com.google.api.client.util.DateTime;
import java.time.Instant
import java.time.ZoneId
import java.sql.Timestamp

import scala.concurrent.Future

// import ai.prognos.DataQuality.util.database._

// import ai.prognos.admissions_client._
import sys.process._

// COMMAND ----------

// ai.prognos.admissions_client.Config.setEnvironment("production")
// ai.prognos.admissions_client.Config.setAPIKey(dbutils.secrets.get(scope = "admissions-client", key = "production-api-key"))

// COMMAND ----------

// MAGIC %md ### Import Redaction Library

// COMMAND ----------

// MAGIC %run "./redactor/lab-redactor-production-11-18-24"

// COMMAND ----------

// MAGIC %md ### Payload Processing

// COMMAND ----------

def compress(p: String): String = {  
  val inData = p.getBytes()

  val deflater = new Deflater()
  deflater.setInput(inData)
  deflater.finish()

  val compressedData = new Array[Byte](inData.size * 2)

  var count = deflater.deflate(compressedData)
  deflater.end()

  val compressedBytes = compressedData.take(count)

  return new String(Base64.getEncoder().encode(compressedBytes))
}

// COMMAND ----------

def decompress(inData: Array[Byte]): Array[Byte] = {
  val inflater = new Inflater()
  inflater.setInput(inData)
  val decompressedData = new Array[Byte](inData.size * 2)
  var count = inflater.inflate(decompressedData)
  var finalData = decompressedData.take(count)
  while (count > 0) {
    count = inflater.inflate(decompressedData)
    finalData = finalData ++ decompressedData.take(count)
  }
  
  return finalData 
 }

// COMMAND ----------

def decompressPayload(p: String): String = {
  val decodedPayload = Base64.getDecoder().decode(p)
  new String(decompress(decodedPayload), StandardCharsets.UTF_8)
}

// COMMAND ----------

def extractPayload(p: String): DataFrame = {
  spark.read.option("multiline","true").json(Seq(p).toDS)
}

// COMMAND ----------

def extractPayloadCompressed(p: String): DataFrame = {
  extractPayload(decompressPayload(p))
}

// COMMAND ----------

// MAGIC %md ### Functions

// COMMAND ----------

def withParquetCompatibleColumnNames()(df: DataFrame): DataFrame = {    
    df.columns.foldLeft(df) { (tmpDF, col) =>
      val newName = col.replaceAll("[,;{}()\n\t=]", "").replaceAll(" ", "_").toLowerCase()
      tmpDF.withColumnRenamed(col, newName)
    }
}

// COMMAND ----------

def withMetaRecordHash()(df: DataFrame): DataFrame = {
  val cols = df.columns.map(c => {
    when(col(c).isNull, "").otherwise(col(c))
  })
    
  df.withColumn("meta_record_hash", md5(concat_ws("|", cols: _*)))
}

// COMMAND ----------

def withPayloadAttributes(r: Row)(df: DataFrame): DataFrame = {
  println("====================== withPayloadAttributes ======================")
  
  val registrationId = lit(r.getAs[String]("registration-id"))
  val datanatorId = lit(r.getAs[Long]("datanator-id"))
  val partner = lit(r.getAs[String]("partner"))

  println(s"registrationId: ${registrationId}")
  println(s"datanatorId: ${datanatorId}")
  println(s"partner: ${partner}")

  println("------------ resultDF ----------------")

  val resultDF = Registrar.getRecordByID(registrationId.toString)
    
  resultDF.show(false)

  val registrationTimestampAsString = resultDF.head().getAs[String]("created-at")

  val gdt = DateTime.parseRfc3339(registrationTimestampAsString);
  val jdt = Instant.ofEpochMilli(gdt.getValue()).atZone(ZoneId.of("UTC")).toLocalDateTime()

  val registrationTimestamp = java.sql.Timestamp.valueOf(jdt)
  val attributes = resultDF.head().getAs[String]("attributes")
  
  println("------------ new attributes ----------------")
  println(s"registrationTimestamp: ${registrationTimestamp}")
  println(s"attributes: ${attributes}")
  
  println("====================== END withPayloadAttributes ======================")
  
  df.withColumn("meta_registration_id", registrationId)
    .withColumn("meta_datanator_id", datanatorId)
    .withColumn("meta_partner", partner)
    .withColumn("meta_registration_timestamp", lit(registrationTimestamp))
    .withColumn("meta_staged_timestamp", current_timestamp())
    .withColumn("meta_attributes", lit(attributes).cast(StringType))
}

// COMMAND ----------

def loadCsv(reader: DataFrameReader, r: Row): DataFrame = {
  val path = r.getAs[String]("in")
  println(path)
  
  reader
    .csv(path)
    .transform(withParquetCompatibleColumnNames())
    .transform(withMetaRecordHash())
    .transform(withPayloadAttributes(r))
}

// COMMAND ----------

def unionCsvPayload(reader: DataFrameReader, payloadDF: DataFrame): DataFrame = {
  val dfs = payloadDF.collect().foldLeft(List[DataFrame]())((l, r) => {
    println(l)
    println(r)

    loadCsv(reader, r) :: l
  })
  
  dfs.reduce(_.union(_))
}

// COMMAND ----------

def loadParquet(r: Row): DataFrame = {
  val path = r.getAs[String]("in")
  println(path)
  
  val df = spark
    .read
    .parquet(path)
    .transform(withMetaRecordHash())
    .transform(withPayloadAttributes(r))
  
  val sortedCols = df.columns.sorted.map(str => col(str))
  
  df.select(sortedCols:_*)
}

// COMMAND ----------

def writeStandardParquetOutput(df: DataFrame, outputPath: String): Unit = {
  df.repartition(col("meta_registration_id"))
    .write
    .mode(SaveMode.Append)
    .partitionBy("meta_registration_id")
    .parquet(outputPath)
}

// COMMAND ----------

class ParquetPayloadDumper(val t: (DataFrame) => DataFrame = (df: DataFrame) => { df }) {

  val rand = scala.util.Random
  var hadError = false
  
  def dumpThreaded(r: Row, outputPath: String): Thread = {
    new Thread {
      override def run {
        try {
          val pool = rand.nextInt(20)
          spark.sparkContext.setLocalProperty("spark.scheduler.pool", s"pool_${pool}")

          println("thread run")
          println(r)
          
          val outDF = loadParquet(r).transform(t)
          
          outDF.printSchema
          
          //outDF.coalesce(1).write.mode(SaveMode.Append).parquet(outputPath)
          writeStandardParquetOutput(outDF, outputPath)
        }
        catch {
          case e: Exception => { e.printStackTrace ; hadError = true }
        }
      }
    }
  }
  
}

// COMMAND ----------

def writeParquetPayload(payloadDF: DataFrame, outputPath: String, t: (DataFrame) => DataFrame = (df: DataFrame) => { df }): Unit = {
  
  val dumper = new ParquetPayloadDumper(t)
  
  val threads = payloadDF.collect().map((r: Row) => {
    dumper.dumpThreaded(r, outputPath)
  })
  
  for(t <- threads) { t.start }
  for(t <- threads) { t.join }
  
  if (dumper.hadError) {
    throw new Exception("failed running one of the dump threads please see error log for details")
  }
}

// COMMAND ----------

case class ZipProcessor( 
    delimiter: String,
    hasHeader: Boolean,
    schema: StructType,
    datanatorId: Int,
    outputUnzipBucket: String // write out unzipped file to intermediary s3 location (auto-deletes based on bucket policy)
) {

  val reader =
    spark.read
      .option("header", hasHeader.toString)
      .option("charset", "UTF8")
      .option("delimiter", delimiter)
      .option("mode", "DROPMALFORMED")
      .option("ignoreTrailingWhiteSpace", "true")
      .option("ignoreLeadingWhiteSpace", "true")
      .schema(schema)

  def processPayloadRow(row: Row, payloadDF: DataFrame): DataFrame = {
    val s3InputPath = row.getAs[String]("in")
    val registrationId = row.getAs[String]("registration-id")

    val payloadDfCurrent = payloadDF.filter(col("registration-id") === registrationId)
    
    if (s3InputPath.contains(".zip")) {
      val dbfsInputPath = s"/notch/zipped/$registrationId.zip"
      val dbfsUnzipPath = s"/notch/unzipped/$registrationId/"
      val s3OutputPath = s"s3://$outputUnzipBucket/unzip/$datanatorId/$registrationId"

      println(s"Processing ZIP for: $registrationId")

      dbutils.fs.cp(s3InputPath, s"file:/dbfs$dbfsInputPath")
      println("Copied zip file from s3 to dbfs")
  
      dbutils.fs.mkdirs(s"file:/dbfs$dbfsUnzipPath")

      println("Unzipping file in dbfs")
      val unzipCmd = s"unzip /dbfs$dbfsInputPath -d /dbfs$dbfsUnzipPath"
      val result = unzipCmd.!
      println(result)

      dbutils.fs.cp(s"file:/dbfs$dbfsUnzipPath", s3OutputPath, recurse = true)
      println("Copied unzipped file(s) from dbfs to S3")
      dbutils.fs.rm(s"file:/dbfs$dbfsInputPath")
      println("Removed zipped file from dbfs")
      dbutils.fs.rm(s"file:/dbfs$dbfsUnzipPath", recurse = true)
      println("Removed unzipped file from dbfs")

      unionCsvPayload(reader, payloadDfCurrent.withColumn("in", lit(s3OutputPath)))

    } else {
      println(s"Processing non-ZIP for: $registrationId") 

      unionCsvPayload(reader, payloadDfCurrent)
    }
  }
}

// COMMAND ----------

// MAGIC %md ### Schemas

val schema_1778822171 = new StructType()
  .add("patient_id", "string")
  .add("date_of_birth", "string")
  .add("address_us_state", "string")
  .add("address_us_zip", "string")
  .add("icd_code_1", "string")
  .add("icd_code_2", "string")
  .add("date_of_service", "string")
  .add("admission_date", "string")
  .add("discharge_date", "string")
  .add("claim_type", "string")
  .add("amount_billed", "string")
  .add("amount_paid", "string")


// COMMAND ----------

val schema_10100 = new StructType()
  .add("Record ID", "string")
  .add("Date Authorized", "string")
  .add("Time Authorized", "string")
  .add("BIN Number", "string")
  .add("Version Number", "string")
  .add("Transaction Code", "string")
  .add("Processor Control Number", "string")
  .add("Transaction Count", "string")
  .add("Service Provider ID", "string")
  .add("Service Provider ID Qualifier", "string")
  .add("Date Of Service", "string")
  .add("Patient Date Of Birth", "string")
  .add("Patient Gender Code", "string")
  .add("Patient Location", "string")
  .add("Patient First Name", "string")
  .add("Patient Last Name", "string")
  .add("Patient Street Address", "string")
  .add("Patient State/Province", "string")
  .add("Patient Zip/Postal Zone", "string")
  .add("Patient ID Qualifier", "string")
  .add("Patient ID", "string")
  .add("Provider ID", "string")
  .add("Provider ID Qualifier", "string")
  .add("Prescriber ID", "string")
  .add("Primary Care Provider ID", "string")
  .add("Prescriber Last Name", "string")
  .add("Prescriber ID Qualifier", "string")
  .add("Primary Care Provider ID Qualifier", "string")
  .add("Group ID", "string")
  .add("Cardholder ID", "string")
  .add("Person Code", "string")
  .add("Patient Relationship Code", "string")
  .add("Eligibility Clarification Code", "string")
  .add("Home Plan", "string")
  .add("Coordination of Benefits Count", "string")
  .add("Other Payer Coverage Type", "string")
  .add("Other Payer ID Qualifier", "string")
  .add("Other Payer ID", "string")
  .add("Other Payer Amount Paid Qualifier", "string")
  .add("Other Payer Amount Paid Submitted", "string")
  .add("Other Payer Date", "string")
  .add("Carrier ID", "string")
  .add("Date Of Injury", "string")
  .add("Claim/Reference ID", "string")
  .add("Other Coverage Code", "string")
  .add("Prescription/Service Reference Number", "string")
  .add("Fill Number", "string")
  .add("Days Supply", "string")
  .add("Compound Code", "string")
  .add("Product/Service ID", "string")
  .add("Dispense As Written (DAW)/Product Select Code", "string")
  .add("Date Prescription Written", "string")
  .add("Number Of Refills Authorized", "string")
  .add("Level Of Service", "string")
  .add("Prescription Origin Code", "string")
  .add("Submission Clarification Code", "string")
  .add("Unit Dose Indicator", "string")
  .add("Product/Service ID Qualifier", "string")
  .add("Quantity Dispensed", "string")
  .add("Orig Prescribed Product/Service Code", "string")
  .add("Orig Prescribed Quantity", "string")
  .add("Orig Prescribed Product/Service Code Qualifier", "string")
  .add("Prescription/Service Reference Number Qualifier", "string")
  .add("Prior Authorization Type Code", "string")
  .add("Unit Of Measure", "string")
  .add("Reason For Service", "string")
  .add("Professional Service Code", "string")
  .add("Result Of Service Code", "string")
  .add("Coupon Type", "string")
  .add("Coupon Number", "string")
  .add("Coupon Value Amount", "string")
  .add("Ingredient Cost Submitted", "string")
  .add("Dispensing Fee Submitted", "string")
  .add("Basis of Cost Determination", "string")
  .add("Usual And Customary Charge", "string")
  .add("Patient Paid Amount Submitted", "string")
  .add("Gross Amount Due Submitted", "string")
  .add("Incentive Amount Submitted", "string")
  .add("Professional Service Fee Submitted", "string")
  .add("Other Amount Claimed Submitted Qualifier", "string")
  .add("Other Amount Claimed Submitted", "string")
  .add("Flat Sales Tax Amount Submitted", "string")
  .add("Percent Sales Tax Amount Submitted", "string")
  .add("Percent Sales Tax Rate Submitted", "string")
  .add("Percent Sales Tax Basis Submitted", "string")
  .add("Diagnosis Code", "string")
  .add("Diagnosis Code Qualifier", "string")
  .add("Response Code", "string")
  .add("Patient Pay Amount", "string")
  .add("Ingredient Cost Paid", "string")
  .add("Dispensing Fee Paid", "string")
  .add("Total Amount Paid", "string")
  .add("Accumulated Deductible Amount", "string")
  .add("Remaining Deductible Amount", "string")
  .add("Remaining Benefit Amount", "string")
  .add("Amount Applied to Periodic Deductible", "string")
  .add("Amount Of Copay/Coinsurance", "string")
  .add("Amount Attributed To Product Selection", "string")
  .add("Amount Exceeding Periodic Benefit Maximum", "string")
  .add("Incentive Amount Paid", "string")
  .add("Basis of Reimbursement Determination", "string")
  .add("Amount Attributed to Sales Tax", "string")
  .add("Tax Exempt Indicator", "string")
  .add("Flat Sales Tax Amount Paid", "string")
  .add("Percentage Sales Tax Amount Paid", "string")
  .add("Percent Sales Tax Rate Paid", "string")
  .add("Percent Sales Tax Basis Paid", "string")
  .add("Professional Service Fee Paid", "string")
  .add("Other Amount Paid Qualifier", "string")
  .add("Other Amount Paid", "string")
  .add("Other Payer Amount Recognized", "string")
  .add("Plan Identification", "string")
  .add("NCPDP Number", "string")
  .add("National Provider ID", "string")
  .add("Plan Type", "string")
  .add("Pharmacy Location (Postal Code)", "string")
  .add("Reject Code 1", "string")
  .add("Reject Code 2", "string")
  .add("Reject Code 3", "string")
  .add("Reject Code 4", "string")
  .add("Reject Code 5", "string")
  .add("Payer ID", "string")
  .add("Payer ID Qualifier", "string")
  .add("Plan Name", "string")
  .add("Type of Payment", "string")
  .add("Change Healthcare Claim ID", "string")
  .add("Filler", "string")