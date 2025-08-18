package the1.initiate

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import scala.util.{Try, Success, Failure}
import com.fasterxml.jackson.databind.{ObjectMapper, DeserializationFeature}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule

// Import all services
import the1.initiate.services._
import the1.initiate.logging.GcsLogger
import java.util.Properties
import scala.io.Source

/**
 * Main entry point for The1 Initiate Pipeline
 * Full implementation with all services integrated
 */
object Main {

  // Configuration classes
  case class Config(
    projectId: String,
    datasetExternal: String,
    datasetFinal: String,
    gcsBucket: String,
    defaultEngine: String,
    tables: Seq[TableConfig],
    validation: ValidationConfig,
    connectionId: String = "demo_gcs_iceberg_connection",
    region: String = "asia-southeast1"
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
    schemaSource: String = "mapping"
  )
  
  case class DestinationConfig(
    gcsPrefix: String,
    biglake: BigLakeConfig,
    finalTable: FinalTableConfig,
    columnMapping: Seq[ColumnMapping] = Seq.empty
  )
  
  case class BigLakeConfig(
    hivePartitioning: Boolean,
    hivePartitionUriPrefix: String
  )
  
  case class FinalTableConfig(dataset: String, table: String)
  case class ColumnMapping(expr: String, as: String)
  case class EngineOverrides(preferred: String, fallbacks: Seq[String])
  case class Thresholds(maxSingleObjectTiB: Int, minCompactTargetMB: Int)
  case class ValidationConfig(
    compareWith: String,
    checksumColumns: Seq[String],
    sampleRatio: Double
  )

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: Main <path_to_job.yaml>")
      System.exit(1)
    }
    
    val configPath = args(0)
    val logger = new GcsLogger()
    val startTime = System.currentTimeMillis()
    
    try {
      logger.info(s"Starting The1 Initiate Pipeline")
      logger.info(s"Config: $configPath")
      
      // Load and enhance configuration
      val config = loadAndEnhanceConfig(configPath, logger)
      
      // Initialize services
      val stsService = new StsService(config.projectId, logger)
      val schemaService = new SchemaService(logger)
      val bigQueryService = new BigQueryService(config.projectId, logger)
      val validationService = new ValidationService(config.projectId, bigQueryService, logger)
      
      // Process each table
      config.tables.foreach { tableConfig =>
        processTable(
          config, 
          tableConfig, 
          stsService, 
          schemaService, 
          bigQueryService, 
          validationService,
          logger
        )
      }
      
      // Clean up
      stsService.close()
      
      val duration = (System.currentTimeMillis() - startTime) / 1000
      logger.info(s"Pipeline completed successfully in ${duration}s")
      
    } catch {
      case e: Exception =>
        logger.error(s"Pipeline failed", e)
        logger.flush("error")
        System.exit(1)
    }
  }
  
  /**
   * Process a single table migration
   */
  private def processTable(
    config: Config,
    tableConfig: TableConfig,
    stsService: StsService,
    schemaService: SchemaService,
    bigQueryService: BigQueryService,
    validationService: ValidationService,
    logger: GcsLogger
  ): Unit = {
    
    val tableName = tableConfig.name
    logger.info(s"Processing table: $tableName")
    
    try {
      // 1. Get schema from mapping file
      val mappingPath = getMappingPath(config, tableConfig)
      val schema = schemaService.generateBigQuerySchema(mappingPath)
      val columnExpression = schemaService.generateColumnSelectExpression(mappingPath)
      
      logger.info(s"Schema discovered with ${schema.split("\n").length} columns")
      
      // 2. Create managed table if not exists
      if (!bigQueryService.tableExists(config.datasetFinal, tableName)) {
        logger.info(s"Creating managed table: ${config.datasetFinal}.$tableName")
        bigQueryService.createManagedTable(
          config.datasetFinal,
          tableName,
          schema,
          config.gcsBucket,
          s"biglake/raw_$tableName",
          config.connectionId,
          config.region
        ) match {
          case Success(_) => logger.info("Managed table created successfully")
          case Failure(e) => throw new RuntimeException(s"Failed to create managed table", e)
        }
      }
      
      // 3. Run STS transfer
      logger.info(s"Starting STS transfer for $tableName")
      stsService.executeTransfer(
        tableConfig.source.s3Bucket,
        tableConfig.source.prefix,
        config.gcsBucket,
        tableConfig.destination.gcsPrefix
      ) match {
        case Success(jobName) => 
          logger.info(s"STS transfer completed: $jobName")
        case Failure(e) => 
          throw new RuntimeException(s"STS transfer failed", e)
      }
      
      // 4. Create/refresh external table
      logger.info(s"Creating external table for $tableName")
      bigQueryService.createOrRefreshExternalTable(
        config.datasetExternal,
        tableName,
        config.gcsBucket,
        tableConfig.destination.gcsPrefix,
        tableConfig.source.format,
        config.connectionId,
        config.region
      ) match {
        case Success(_) => logger.info("External table created/refreshed")
        case Failure(e) => throw new RuntimeException(s"Failed to create external table", e)
      }
      
      // 5. Load data into managed table
      logger.info(s"Loading data into managed table")
      val loadMode = if (tableConfig.source.prefix.contains("delta")) "MERGE" else "TRUNCATE"
      
      bigQueryService.loadIntoManagedTable(
        config.datasetExternal,
        tableName,
        config.datasetFinal,
        tableName,
        columnExpression,
        loadMode
      ) match {
        case Success(rowsAffected) => 
          logger.info(s"Data loaded successfully. Rows affected: $rowsAffected")
        case Failure(e) => 
          throw new RuntimeException(s"Failed to load data", e)
      }
      
      // 6. Validate
      logger.info(s"Validating data transfer")
      val validationResult = validationService.validateTransfer(
        config.datasetExternal,
        tableName,
        config.datasetFinal,
        tableName,
        config.validation.checksumColumns
      )
      
      if (!validationResult.isValid) {
        throw new RuntimeException(s"Validation failed: ${validationResult.issues.mkString(", ")}")
      }
      
      // 7. Write summary
      val duration = "N/A" // Calculate if needed
      logger.writeSummary(
        tableName,
        status = "SUCCESS",
        rowsTransferred = validationResult.targetCount,
        duration = duration,
        errors = Seq.empty
      )
      
    } catch {
      case e: Exception =>
        logger.error(s"Failed to process table $tableName", e)
        logger.writeSummary(
          tableName,
          status = "FAILED",
          errors = Seq(e.getMessage)
        )
        throw e
    }
  }
  
  /**
   * Load configuration and enhance with mapping.json
   */
  private def loadAndEnhanceConfig(configPath: String, logger: GcsLogger): Config = {
    logger.info("Loading configuration")
    
    val mapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    
    val configContent = if (configPath.startsWith("gs://")) {
      readFromGcs(configPath)
    } else {
      new String(Files.readAllBytes(Paths.get(configPath)))
    }
    
    var config = mapper.readValue(configContent, classOf[Config])
    
    // Enhance each table config with mapping.json
    config = config.copy(
      tables = config.tables.map { tableConfig =>
        val mappingPath = getMappingPath(config, tableConfig)
        if (Files.exists(Paths.get(mappingPath)) || mappingPath.startsWith("gs://")) {
          logger.info(s"Loading mapping for ${tableConfig.name} from $mappingPath")
          val mappingContent = if (mappingPath.startsWith("gs://")) {
            readFromGcs(mappingPath)
          } else {
            new String(Files.readAllBytes(Paths.get(mappingPath)))
          }
          
          // Parse mapping.json
          case class MappingFile(
            selectedColumns: Seq[String],
            columnMapping: Seq[ColumnMapping]
          )
          
          val mapping = mapper.readValue(mappingContent, classOf[MappingFile])
          
          // Update destination with column mapping
          tableConfig.copy(
            destination = tableConfig.destination.copy(
              columnMapping = mapping.columnMapping
            )
          )
        } else {
          logger.warn(s"No mapping file found for ${tableConfig.name}")
          tableConfig
        }
      }
    )
    // import scala.collection.JavaConverters._

    // val updatedTables = new java.util.ArrayList[TableConfig]()
    // for (tableConfig <- config.tables) {
    //   // process each table
    //   updatedTables.add(processedTableConfig)
    // }
    // config = config.copy(tables = updatedTables.asScala.toSeq)

      logger.info(s"Configuration loaded with ${config.tables.size} tables")
      config
    }
  
  /**
   * Get mapping file path for a table
   */
  private def getMappingPath(config: Config, tableConfig: TableConfig): String = {
    // Assume mapping.json is in same directory as job.yaml
    if (tableConfig.source.schemaSource == "mapping") {
      s"config/${tableConfig.name}/mapping.json"
    } else {
      s"config/${tableConfig.name}/mapping.json"
    }
  }
  
  /**
   * Read file from GCS
   */
  private def readFromGcs(gcsPath: String): String = {
    import com.google.cloud.storage.{Storage, StorageOptions}
    
    val parts = gcsPath.replace("gs://", "").split("/", 2)
    val bucketName = parts(0)
    val blobName = parts(1)
    
    val storage = StorageOptions.getDefaultInstance.getService
    val blob = storage.get(bucketName, blobName)
    
    new String(blob.getContent(), "UTF-8")
  }
}