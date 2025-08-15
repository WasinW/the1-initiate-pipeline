package the1.initiate

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import com.fasterxml.jackson.databind.{ObjectMapper, DeserializationFeature}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory

// Google Cloud clients
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, TableId}
import com.google.cloud.storage.transfer.v1.{StorageTransferServiceClient, TransferJob}

/**
 * Entry point for the initiation pipeline.  This Spark driver (though Spark
 * functionality is not used) reads a YAML configuration file describing
 * tables to migrate, orchestrates a Storage Transfer Service (STS) copy from
 * AWS S3 to GCS, creates or refreshes BigLake external tables, loads data into
 * managed BigQuery tables and performs validation.
 *
 * The implementation here is intentionally light; replace the stubbed
 * methods with your own logic to integrate with Glue/Athena, STS, BigQuery
 * and Redshift as required.  Use this as a scaffold for your own code.
 */
object Main {

  /**
   * Configuration model representing the YAML file.  Only a subset of
   * fields is defined here; feel free to extend with additional options.
   */
  case class Config(
    projectId: String,
    datasetExternal: String,
    datasetFinal: String,
    gcsBucket: String,
    defaultEngine: String,
    tables: Seq[TableConfig],
    validation: ValidationConfig
  )
  case class TableConfig(
    name: String,
    source: SourceConfig,
    destination: DestinationConfig,
    engineOverrides: Option[EngineOverrides] = None,
    thresholds: Option[Thresholds] = None
  )
  case class SourceConfig(
    s3Bucket: String,
    prefix: String,
    format: String,
    schemaSource: String,
    glue: Option[GlueConfig] = None
  )
  case class GlueConfig(database: String, table: String)
  case class DestinationConfig(
    gcsPrefix: String,
    biglake: BigLakeConfig,
    finalTable: FinalTableConfig,
    columnMapping: Seq[ColumnMapping]
  )
  case class BigLakeConfig(hivePartitioning: Boolean, hivePartitionUriPrefix: String)
  case class FinalTableConfig(dataset: String, table: String)
  case class ColumnMapping(expr: String, as: String)
  case class EngineOverrides(preferred: String, fallbacks: Seq[String])
  case class Thresholds(maxSingleObjectTiB: Int, minCompactTargetMB: Int)
  case class ValidationConfig(compareWith: String, checksumColumns: Seq[String], sampleRatio: Double)

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: Main <config-yaml-path>")
      System.exit(1)
    }
    val configPath = args(0)
    val config = loadConfig(configPath)
    // initialise clients
    val bigQuery: BigQuery = BigQueryOptions.newBuilder().setProjectId(config.projectId).build().getService
    val stsClient: StorageTransferServiceClient = StorageTransferServiceClient.create()

    // Loop through tables in the config
    config.tables.foreach { tableCfg =>
      println(s"Processing table ${tableCfg.name} …")
      // 1. Read schema
      val schema = readSourceSchema(tableCfg)
      // 2. Create or refresh external table
      createOrRefreshExternalTable(bigQuery, config, tableCfg, schema)
      // 3. Copy data via STS
      runStsCopy(config, tableCfg, stsClient)
      // 4. Refresh external table again after copy
      createOrRefreshExternalTable(bigQuery, config, tableCfg, schema)
      // 5. Load into final table
      loadIntoFinalTable(bigQuery, config, tableCfg)
      // 6. Validate counts and values
      validateSourceAndTarget(config, tableCfg)
    }

    stsClient.close()
  }

  /**
   * Parse YAML configuration file into our Config case class.  Uses Jackson
   * YAML module.  Unknown properties in the YAML are ignored so you can
   * extend the file without updating the code.
   */
  def loadConfig(path: String): Config = {
    val mapper = new ObjectMapper(new YAMLFactory())
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    val bytes = Files.readAllBytes(Paths.get(path))
    mapper.readValue(bytes, classOf[Config])
  }

  /**
   * Read the source schema either from AWS Glue/Athena or by inferring it
   * from sample Parquet files on S3.  This implementation is a placeholder;
   * you'll need to add AWS SDK calls and schema conversion logic.
   */
  def readSourceSchema(tableCfg: TableConfig): String = {
    tableCfg.source.schemaSource.toLowerCase match {
      case "glue" =>
        // TODO: implement Glue catalog retrieval
        println(s"Retrieving schema for ${tableCfg.name} from Glue …")
        "id INT64, value STRING" // return a BigQuery SQL schema string or similar representation
      case "infer" =>
        // TODO: sample S3 files and infer schema using Parquet/Arrow
        println(s"Inferring schema for ${tableCfg.name} from S3 samples …")
        "id INT64, value STRING"
      case other =>
        throw new IllegalArgumentException(s"Unsupported schemaSource: $other")
    }
  }

  /**
   * Create or refresh a BigLake external table pointing at the destination GCS
   * prefix.  The schema should be converted to a BigQuery Schema if
   * necessary.  Here we simply print out a DDL; in your production code,
   * invoke the BigQuery client and run the DDL.
   */
  def createOrRefreshExternalTable(bigQuery: BigQuery, cfg: Config, tableCfg: TableConfig, schema: String): Unit = {
    val extTableId = TableId.of(cfg.datasetExternal, s"${tableCfg.name}_ext")
    val gcsUri = s"gs://${cfg.gcsBucket}/${tableCfg.destination.gcsPrefix}*"
    // Build a CREATE OR REPLACE EXTERNAL TABLE statement
    val ddl =
      s"""
         |CREATE OR REPLACE EXTERNAL TABLE `${extTableId.getDataset}.${extTableId.getTable}`
         |WITH CONNECTION `REGION.${cfg.projectId}.connection`
         |OPTIONS (
         |  format = '${tableCfg.source.format.toUpperCase}',
         |  uris = ['$gcsUri'],
         |  hive_partition_uri_prefix = 'gs://${cfg.gcsBucket}/${tableCfg.destination.gcsPrefix}',
         |  require_hive_partition_filter = false
         |) AS SELECT * FROM UNNEST([])
         |""".stripMargin
    println(s"Executing DDL for external table ${extTableId.getTable}:\n$ddl")
    // TODO: use bigQuery.query() to execute the DDL
  }

  /**
   * Trigger a Storage Transfer Service job to copy objects from S3 to GCS.
   * You should build a TransferJob definition, specifying the source S3
   * bucket, credentials stored in Secret Manager, and the destination GCS
   * bucket/prefix.  Use overwriteWhenDifferent to make it idempotent.
   */
  def runStsCopy(cfg: Config, tableCfg: TableConfig, stsClient: StorageTransferServiceClient): Unit = {
    println(s"Starting STS copy for ${tableCfg.name} from s3://${tableCfg.source.s3Bucket}/${tableCfg.source.prefix} to gs://${cfg.gcsBucket}/${tableCfg.destination.gcsPrefix} …")
    // TODO: build a TransferJob with schedule set to run immediately
    // and call stsClient.createTransferJob(job)
    // Poll job status until done
  }

  /**
   * Load data from the external table into the final managed BigQuery table.
   * This typically involves a MERGE or INSERT statement.  Use the
   * columnMapping defined in the config to select/cast columns.  For
   * idempotent re‑runs, MERGE on the business key and partition.
   */
  def loadIntoFinalTable(bigQuery: BigQuery, cfg: Config, tableCfg: TableConfig): Unit = {
    val extTableId = TableId.of(cfg.datasetExternal, s"${tableCfg.name}_ext")
    val finalTableId = TableId.of(tableCfg.destination.finalTable.dataset, tableCfg.destination.finalTable.table)
    val selectExprs = tableCfg.destination.columnMapping.map(cm => s"${cm.expr} AS ${cm.as}").mkString(", ")
    val sql = s"INSERT INTO `${finalTableId.getDataset}.${finalTableId.getTable}` ($selectExprs) SELECT $selectExprs FROM `${extTableId.getDataset}.${extTableId.getTable}`"
    println(s"Executing load into final table ${finalTableId.getTable}:\n$sql")
    // TODO: use bigQuery.query() to run the INSERT or MERGE
  }

  /**
   * Compare counts and optionally checksums between the source system and the
   * BigQuery external/final tables.  This implementation simply prints a
   * placeholder; replace with calls to Athena/Redshift and BigQuery to
   * compute statistics.
   */
  def validateSourceAndTarget(cfg: Config, tableCfg: TableConfig): Unit = {
    println(s"Validating ${tableCfg.name} …")
    // TODO: query Athena or Redshift for row counts and checksumColumns
    // TODO: query BigQuery for row counts and checksumColumns
    // Compare and report mismatches
  }
}