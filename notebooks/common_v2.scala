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

val schema_1778820233 = new StructType()
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