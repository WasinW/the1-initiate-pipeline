package the1.initiate.services

import com.fasterxml.jackson.databind.{ObjectMapper, DeserializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.cloud.storage.{Storage, StorageOptions}
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import the1.initiate.logging.GcsLogger

/**
 * Schema discovery and management service
 * Reads schema from mapping.json files
 */
class SchemaService(logger: GcsLogger) {
  
  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  
  case class MappingConfig(
    selectedColumns: Seq[String],
    columnMapping: Seq[ColumnMapping]
  )
  
  case class ColumnMapping(
    expr: String,
    as: String
  )
  
  /**
   * Read schema from mapping.json file
   * Can read from local file or GCS
   */
  def readSchemaFromMapping(mappingPath: String): (Seq[String], Map[String, String]) = {
    logger.info(s"Reading schema from: $mappingPath")
    
    val jsonContent = if (mappingPath.startsWith("gs://")) {
      readFromGcs(mappingPath)
    } else {
      new String(Files.readAllBytes(Paths.get(mappingPath)))
    }
    
    val mapping = mapper.readValue(jsonContent, classOf[MappingConfig])
    
    // Create column type map (inferring types from column names)
    val columnTypes = mapping.selectedColumns.map { col =>
      val colLower = col.toLowerCase
      val sqlType = colLower match {
        case c if c.contains("id") => "STRING"
        case c if c.contains("number") => "STRING"
        case c if c.contains("code") => "STRING"
        case c if c.contains("phone") => "STRING"
        case c if c.contains("postal") => "STRING"
        case c if c.contains("date") || c.contains("time") => "TIMESTAMP"
        case c if c.contains("amount") || c.contains("price") || c.contains("cost") => "NUMERIC"
        case c if c.contains("count") || c.contains("qty") || c.contains("quantity") => "INT64"
        case c if c.contains("flag") || c.contains("is_") || c.contains("has_") => "BOOL"
        case _ => "STRING"  // Default to STRING
      }
      (col, sqlType)
    }.toMap
    
    logger.info(s"Discovered ${mapping.selectedColumns.size} columns")
    (mapping.selectedColumns, columnTypes)
  }
  
  /**
   * Generate BigQuery schema DDL from mapping
   */
  def generateBigQuerySchema(mappingPath: String): String = {
    val (columns, types) = readSchemaFromMapping(mappingPath)
    
    // Read the mapping to get renamed columns
    val jsonContent = if (mappingPath.startsWith("gs://")) {
      readFromGcs(mappingPath)
    } else {
      new String(Files.readAllBytes(Paths.get(mappingPath)))
    }
    
    val mapping = mapper.readValue(jsonContent, classOf[MappingConfig])
    
    // Build schema with renamed columns
    val schemaFields = mapping.columnMapping.map { cm =>
      val originalType = types.getOrElse(cm.expr, "STRING")
      s"  ${cm.as} ${originalType}"
    }
    
    schemaFields.mkString(",\n")
  }
  
  /**
   * Generate column select expression for BigQuery
   */
  def generateColumnSelectExpression(mappingPath: String): String = {
    val jsonContent = if (mappingPath.startsWith("gs://")) {
      readFromGcs(mappingPath)
    } else {
      new String(Files.readAllBytes(Paths.get(mappingPath)))
    }
    
    val mapping = mapper.readValue(jsonContent, classOf[MappingConfig])
    
    if (mapping.columnMapping.isEmpty) {
      "*"
    } else {
      mapping.columnMapping.map { cm =>
        if (cm.expr == cm.as) {
          cm.expr
        } else {
          s"${cm.expr} AS ${cm.as}"
        }
      }.mkString(",\n  ")
    }
  }
  
  /**
   * Read file from GCS
   */
  private def readFromGcs(gcsPath: String): String = {
    val parts = gcsPath.replace("gs://", "").split("/", 2)
    val bucketName = parts(0)
    val blobName = parts(1)
    
    val storage = StorageOptions.getDefaultInstance.getService
    val blob = storage.get(bucketName, blobName)
    
    new String(blob.getContent(), "UTF-8")
  }
  
  /**
   * Validate schema compatibility between source and target
   */
  def validateSchemaCompatibility(
    sourceColumns: Seq[String],
    mappingPath: String
  ): (Boolean, Seq[String]) = {
    val (expectedColumns, _) = readSchemaFromMapping(mappingPath)
    
    val missingColumns = expectedColumns.filterNot(sourceColumns.contains)
    val extraColumns = sourceColumns.filterNot(expectedColumns.contains)
    
    val issues = Seq.newBuilder[String]
    
    if (missingColumns.nonEmpty) {
      issues += s"Missing columns in source: ${missingColumns.mkString(", ")}"
    }
    
    if (extraColumns.nonEmpty) {
      logger.warn(s"Extra columns in source (will be ignored): ${extraColumns.mkString(", ")}")
    }
    
    val isValid = missingColumns.isEmpty
    (isValid, issues.result())
  }
}