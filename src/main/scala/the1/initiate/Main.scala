package the1.initiate

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import com.fasterxml.jackson.databind.{ObjectMapper, DeserializationFeature}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory

// Google Cloud clients
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, TableId}
import com.google.cloud.storage.transfer.v1.{StorageTransferServiceClient, TransferJob}
// Additional imports for STS transfer job definition
import com.google.cloud.secretmanager.v1.{SecretManagerServiceClient, SecretVersionName}
import com.google.storagetransfer.v1.proto.TransferProto
import com.google.storagetransfer.v1.proto.TransferProto.CreateTransferJobRequest
import com.google.storagetransfer.v1.proto.TransferTypes
import com.google.storagetransfer.v1.proto.TransferTypes.{TransferOptions, ObjectConditions, TransferSpec, Schedule}
import com.google.storagetransfer.v1.proto.TransferTypes.{AwsS3Data, AwsAccessKey, GcsData, TransferJob => ProtoTransferJob}
import com.google.type.{Date => ProtoDate, TimeOfDay}

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

    // ---------------------------------------------------------------------------
    // Retrieve AWS credentials from Secret Manager.  This implementation assumes
    // two secrets exist in the same project: "aws-access-key-id" and
    // "aws-secret-access-key".  Each secret should have at least one version
    // with the AWS key/secret as plain text.  In production you may want to
    // parameterise the secret names or include them in the YAML config.
    // ---------------------------------------------------------------------------
    val secretClient = SecretManagerServiceClient.create()
    def getSecretPayload(secretName: String): String = {
      // Build the resource name for the latest version of the secret
      val secretVersion = SecretVersionName.of(cfg.projectId, secretName, "latest")
      val response = secretClient.accessSecretVersion(secretVersion)
      response.getPayload.getData.toStringUtf8.trim
    }
    val awsAccessKeyId: String = getSecretPayload("aws-access-key-id")
    val awsSecretAccessKey: String = getSecretPayload("aws-secret-access-key")
    secretClient.close()

    // ---------------------------------------------------------------------------
    // Build the AWS S3 data source.  The bucket name comes from the config and
    // the optional path is used to scope the transfer to a prefix.  If the
    // prefix is empty then the entire bucket will be copied.  Note that the
    // AWS access key and secret are injected here.
    // ---------------------------------------------------------------------------
    val awsAccessKey: AwsAccessKey = AwsAccessKey.newBuilder()
      .setAccessKeyId(awsAccessKeyId)
      .setSecretAccessKey(awsSecretAccessKey)
      .build()
    val awsSource: AwsS3Data = AwsS3Data.newBuilder()
      .setBucketName(tableCfg.source.s3Bucket)
      // Set an empty path when no prefix is provided
      .setPath(if (tableCfg.source.prefix != null && tableCfg.source.prefix.nonEmpty) tableCfg.source.prefix else "")
      .setAwsAccessKey(awsAccessKey)
      .build()

    // ---------------------------------------------------------------------------
    // Build the GCS data sink.  The bucket name and prefix come from the config.
    // When specifying a path (prefix) here, STS will write objects into that
    // prefix, preserving any subdirectories from the source.  Omitting the path
    // writes to the root of the bucket.
    // ---------------------------------------------------------------------------
    val gcsSink: GcsData = GcsData.newBuilder()
      .setBucketName(cfg.gcsBucket)
      .setPath(tableCfg.destination.gcsPrefix)
      .build()

    // ---------------------------------------------------------------------------
    // Configure which objects to include.  STS filters on prefixes rather than
    // suffixes.  Here we include a single prefix (the same as the path on the
    // AWS source) so that reruns copy only the relevant sub-tree.  If you need
    // to include multiple prefixes, call addIncludePrefixes for each.
    // ---------------------------------------------------------------------------
    val objectConditions: ObjectConditions = ObjectConditions.newBuilder()
      .addIncludePrefixes(tableCfg.source.prefix)
      .build()

    // ---------------------------------------------------------------------------
    // Transfer options control behaviour when files already exist at the sink.
    // Setting overwriteWhenDifferent to true makes the job idempotent: on a
    // rerun, STS will replace objects that have changed and skip unchanged ones.
    // Delete options are disabled here because we do not want to remove any
    // objects after the transfer completes.
    // ---------------------------------------------------------------------------
    val transferOptions: TransferOptions = TransferOptions.newBuilder()
      .setOverwriteWhenDifferent(true)
      .setDeleteObjectsFromSourceAfterTransfer(false)
      .setDeleteObjectsUniqueInSink(false)
      .build()

    // ---------------------------------------------------------------------------
    // Assemble the transfer specification with the S3 source, the GCS sink,
    // object conditions and transfer options.
    // ---------------------------------------------------------------------------
    val transferSpec: TransferSpec = TransferSpec.newBuilder()
      .setAwsS3DataSource(awsSource)
      .setGcsDataSink(gcsSink)
      .setObjectConditions(objectConditions)
      .setTransferOptions(transferOptions)
      .build()

    // ---------------------------------------------------------------------------
    // Define a schedule that starts immediately and ends on the same day.  STS
    // requires a start date/time and an end date even for one-off jobs.  The
    // LocalDate and LocalTime classes are used to obtain the current UTC date
    // and time, which are converted to the protobuf Date and TimeOfDay types.
    // ---------------------------------------------------------------------------
    val now = java.time.OffsetDateTime.now(java.time.ZoneOffset.UTC)
    val startDate: ProtoDate = ProtoDate.newBuilder()
      .setYear(now.getYear)
      .setMonth(now.getMonthValue)
      .setDay(now.getDayOfMonth)
      .build()
    val timeOfDay: TimeOfDay = TimeOfDay.newBuilder()
      .setHours(now.getHour)
      .setMinutes(now.getMinute)
      .setSeconds(now.getSecond)
      .build()
    val schedule: Schedule = Schedule.newBuilder()
      .setScheduleStartDate(startDate)
      .setScheduleEndDate(startDate)
      .setStartTimeOfDay(timeOfDay)
      .build()

    // ---------------------------------------------------------------------------
    // Build the TransferJob.  The name is left unset so that the server
    // automatically generates it.  We set the status to ENABLED so that the job
    // runs as soon as it's created.  The description can be any helpful text.
    // ---------------------------------------------------------------------------
    val transferJob: ProtoTransferJob = ProtoTransferJob.newBuilder()
      .setProjectId(cfg.projectId)
      .setDescription(s"Initial data migration for table ${tableCfg.name}")
      .setTransferSpec(transferSpec)
      .setSchedule(schedule)
      .setStatus(ProtoTransferJob.Status.ENABLED)
      .build()

    // Create the request and submit it.  The StorageTransferServiceClient is a
    // lightweight wrapper around the underlying gRPC client.  Once the job is
    // created, its name will be returned.  STS jobs are asynchronous: the
    // create call returns immediately, while the job executes in the background.
    val request: CreateTransferJobRequest =
      CreateTransferJobRequest.newBuilder().setTransferJob(transferJob).build()
    val response: TransferJob = stsClient.createTransferJob(request)
    println(s"Created STS transfer job ${response.getName} for table ${tableCfg.name}")

    // ---------------------------------------------------------------------------
    // Optional: poll until the transfer job completes.  For a one-off job, you
    // can wait for the operation associated with the first run to finish.  In
    // more advanced pipelines you might track job status via Cloud Logging.
    // ---------------------------------------------------------------------------
    // Note: Storage Transfer Service API exposes long-running operations for each
    // transfer run.  The transfer job name ends with "jobs/NNN".  To get the
    // operation, use TransferJobServiceClient.runTransferJob or check the
    // operations API.  Polling is omitted here for brevity.
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